import uuid
from dataclasses import replace

import pytest

from fred_pipeline.audit import EtlRun, RunStatus
from fred_pipeline.catalogs.manifest import load_manifests
from fred_pipeline.catalogs.series_geography import build_initial_config_candidates
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.ecb_eligible_assets import (
    ECBEligibleAssetRepository,
    ReleaseMessage,
    diff_assets,
    parse_assets_xml,
)
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.io.postgres_store import PostgresWarehouse
from fred_pipeline.io.series_geography_store import PostgresSeriesGeographyRepository
from fred_pipeline.io.warehouse_factory import WarehouseConfig, WarehouseFactory
from fred_pipeline.fred_client import FredAPIError
from fred_pipeline.manifest import SeriesSpec, ValidationProfile
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.warehouse import Warehouse
from fred_pipeline.gold_config.global_config import load_global_config


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id: str, **kwargs) -> SeriesSpec:
    kwargs.setdefault("title", series_id)
    kwargs.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kwargs)


def _silver_row(
    series_id: str,
    observation_date: str,
    realtime_start: str,
    value: float | None,
    revision_number: int,
    *,
    is_missing: bool = False,
    realtime_end: str = "9999-12-31",
    source: str = "fred",
    ingested_at: str = "2024-01-01T00:00:00+00:00",
):
    raw_value = "." if is_missing else str(value)
    return {
        "source": source,
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "value": value,
        "raw_value": raw_value,
        "is_missing": is_missing,
        "row_hash": f"h-{source}-{series_id}-{observation_date}-{realtime_start}",
        "revision_number": revision_number,
        "ingested_at": ingested_at,
        "run_id": "r",
    }


def _tiingo_rows(records, *, ingested_at, run_id):
    rows = []
    for observation_date, close, dividend, split in records:
        for field, value in (
            ("close", close),
            ("divCash", dividend),
            ("splitFactor", split),
        ):
            rows.append(
                {
                    "source": "tiingo",
                    "series_id": f"AAPL:{field}",
                    "observation_date": observation_date,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": value,
                    "raw_value": str(value),
                    "is_missing": False,
                    "row_hash": f"AAPL:{field}:{observation_date}:{value}",
                    "revision_number": 1,
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                }
            )
    return rows


@pytest.fixture
def postgres_dsn():
    psycopg = pytest.importorskip("psycopg")
    admin_dsn = "postgresql://fred:fred@localhost:55432/postgres"
    db_name = f"fred_pipeline_test_{uuid.uuid4().hex[:10]}"
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{db_name}"')
    except Exception as exc:  # noqa: BLE001 - unavailable local service should skip.
        pytest.skip(f"local Postgres is not available: {exc}")

    dsn = f"postgresql://fred:fred@localhost:55432/{db_name}"
    try:
        yield dsn
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


def test_postgres_warehouse_builds_core_gold_like_local(tmp_path, postgres_dsn):
    rows = [
        _silver_row(
            "PAYEMS",
            "2024-01-01",
            "2024-02-01",
            100.0,
            1,
            realtime_end="2024-02-29",
        ),
        _silver_row("PAYEMS", "2024-01-01", "2024-03-01", 101.5, 2),
        _silver_row(
            "PAYEMS",
            "2024-02-01",
            "2024-02-01",
            None,
            1,
            is_missing=True,
        ),
        _silver_row("DGS10", "2024-01-02", "", 4.25, 1, realtime_end=""),
    ]

    local = LocalWarehouse(_config(), db_path=str(tmp_path / "local.db"))
    pg = PostgresWarehouse(_config(), dsn=postgres_dsn)
    assert isinstance(pg, Warehouse)

    try:
        object_counts = pg.query(
            """
            SELECT table_schema, COUNT(*) AS n
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema IN ('meta', 'audit', 'bronze', 'silver', 'gold')
            GROUP BY table_schema
            """
        )
        assert {row["table_schema"]: row["n"] for row in object_counts} == {
            "audit": 4,
            # FRED response plus the spec011 release/artifact tables.
            "bronze": 3,
            # Includes gold_incremental_checkpoint (spec003 Phase 3 checkpoint
            # infra), mmkt_rate_snapshot (spec009), and Spec015 ECB FX output.
            "gold": 59,
            "meta": 7,
            # Compact observations plus Spec015 immutable observation history,
            # four spec011 PIT tables, and four archive backfill staging tables.
            "silver": 10,
        }
        view_counts = pg.query(
            """
            SELECT table_schema, COUNT(*) AS n
            FROM information_schema.views
            WHERE table_schema = 'gold'
            GROUP BY table_schema
            """
        )
        # Includes gold.fred_point_in_time, the flat-name-translated mirror of
        # LocalWarehouse's view, plus the four spec011 consumer views.
        assert view_counts == [{"table_schema": "gold", "n": 11}]

        local.merge_silver(rows)
        pg.merge_silver(rows)

        local_result = local.build_gold()

        # Regression guard: PostgreSQL Gold must never materialize the complete
        # Silver table in Python. Purpose-specific raw reads go through
        # _read_gold_silver_subset(); revision stats stay set-based in SQL.
        pg_read = pg._read

        def reject_unbounded_fact_read(table):
            if table == "silver_fred_observation":
                raise AssertionError("unbounded PostgreSQL Silver read")
            if table == "gold_fred_latest_observation":
                raise AssertionError("unbounded PostgreSQL Latest read")
            return pg_read(table)

        pg._read = reject_unbounded_fact_read
        pg_result = pg.build_gold()
        assert pg_result == local_result

        latest_sql = """
            SELECT series_id, observation_date, value, realtime_start,
                   realtime_end, is_missing, revision_number, ingested_at
            FROM gold_fred_latest_observation
            ORDER BY series_id, observation_date
        """
        assert pg.query(latest_sql) == local.query(latest_sql)

        pit_sql = """
            SELECT series_id, observation_date, realtime_start, realtime_end,
                   value, revision_number, is_missing, ingested_at
            FROM gold_fred_point_in_time
            ORDER BY series_id, observation_date, realtime_start
        """
        assert pg.query(pit_sql) == local.query(pit_sql)

        revision_sql = """
            SELECT series_id, observation_date, revision_count,
                   first_value, first_realtime_start,
                   latest_value, latest_realtime_start,
                   revision_delta, revision_pct
            FROM gold_fred_revision_stats
            ORDER BY series_id, observation_date
        """
        assert pg.query(revision_sql) == local.query(revision_sql)

        local_mmkt = local.query(
            "SELECT * FROM gold_mmkt_rate_snapshot ORDER BY instrument_id"
        )
        postgres_mmkt = pg.query(
            "SELECT * FROM gold.mmkt_rate_snapshot ORDER BY instrument_id"
        )
        assert postgres_mmkt == local_mmkt
    finally:
        pg.close()
        local.close()


def test_postgres_warehouse_is_idempotent(postgres_dsn):
    row = _silver_row("DGS10", "2024-01-01", "", 4.0, 1, realtime_end="")
    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        wh.merge_silver([row])
        wh.merge_silver([{**row, "value": 4.1, "row_hash": "h-updated"}])
        got = wh.query("SELECT COUNT(*) AS c FROM silver_fred_observation")
        assert got[0]["c"] == 1
        value = wh.query("SELECT value FROM silver_fred_observation")[0]["value"]
        assert value == 4.1
    finally:
        wh.close()


def _spec014_snapshot(warehouse: PostgresWarehouse, run_id: str) -> dict[str, list]:
    """Comparable durable output for a single Spec014 fixture run."""
    return {
        "bronze": warehouse.query(
            """
            SELECT source, series_id, endpoint, request_params, response_payload,
                   observation_count, payload_bytes
            FROM bronze_fred_api_response
            WHERE run_id = ?
            ORDER BY source, series_id
            """,
            (run_id,),
        ),
        "silver": warehouse.query(
            """
            SELECT source, series_id, observation_date, realtime_start,
                   realtime_end, value, raw_value, is_missing, row_hash,
                   revision_number
            FROM silver_fred_observation
            ORDER BY source, series_id, observation_date, realtime_start
            """,
        ),
        "dq": warehouse.query(
            """
            SELECT series_id, check_name, passed, severity, message, metric_value
            FROM audit_data_quality_result
            WHERE run_id = ?
            ORDER BY series_id, check_name
            """,
            (run_id,),
        ),
        "series_audit": warehouse.query(
            """
            SELECT series_id, status, load_type, observations_extracted,
                   rows_written_bronze, rows_merged_silver, dq_passed, error_message
            FROM audit_etl_series_run
            WHERE run_id = ?
            ORDER BY series_id
            """,
            (run_id,),
        ),
    }


def _clear_spec014_tables(warehouse: PostgresWarehouse) -> None:
    """Reset only the fixture's mutable layers between parity-mode runs."""
    with warehouse._pg_conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE bronze.fred_api_response, silver.fred_observation,
                           audit.data_quality_result, audit.etl_series_run,
                           audit.etl_run
            """
        )
    warehouse._pg_conn.commit()


def test_postgres_run_batched_matches_per_series_for_mixed_outcomes(
    postgres_dsn, observations_payload, fake_client_cls
):
    """PostgreSQL stores the same durable facts in either Spec014 write mode."""
    all_missing = {
        "observations": [
            {
                "date": "2024-01-01",
                "value": ".",
                "realtime_start": "2024-01-02",
                "realtime_end": "9999-12-31",
            }
        ]
    }
    specs = [
        _spec("GOOD"),
        _spec("DQ_FAILED", validation_profile=ValidationProfile.STRICT),
        _spec("EXTRACT_FAILED"),
    ]
    payloads = {"GOOD": observations_payload, "DQ_FAILED": all_missing}
    errors = {"EXTRACT_FAILED": FredAPIError("upstream unavailable", 503)}
    warehouse = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        per_series = FredPipeline(
            PipelineConfig(
                environment=Environment.DEV,
                fred_api_key="k",
                persistence_write_mode="per_series",
            ),
            client=fake_client_cls(payloads, errors=errors),
            warehouse=warehouse,
        ).run(specs, build_gold_layer=False, skip_not_due=False)
        per_series_snapshot = _spec014_snapshot(warehouse, per_series.run_id)

        _clear_spec014_tables(warehouse)

        run_batched = FredPipeline(
            PipelineConfig(
                environment=Environment.DEV,
                fred_api_key="k",
                persistence_write_mode="run_batched",
            ),
            client=fake_client_cls(payloads, errors=errors),
            warehouse=warehouse,
        ).run(specs, build_gold_layer=False, skip_not_due=False)
        run_batched_snapshot = _spec014_snapshot(warehouse, run_batched.run_id)

        assert per_series.status == run_batched.status == RunStatus.PARTIAL
        assert per_series_snapshot == run_batched_snapshot
        assert [row["series_id"] for row in run_batched_snapshot["bronze"]] == [
            "DQ_FAILED",
            "GOOD",
        ]
        assert {row["series_id"] for row in run_batched_snapshot["silver"]} == {"GOOD"}
        assert {row["series_id"] for row in run_batched_snapshot["dq"]} == {
            "DQ_FAILED",
            "GOOD",
        }
    finally:
        warehouse.close()


def test_postgres_final_audit_replaces_a_run_series_rows_in_bulk(postgres_dsn):
    warehouse = PostgresWarehouse(_config(), dsn=postgres_dsn)
    run = EtlRun(environment="dev")
    for series_id in ("A", "B", "C"):
        series_run = run.start_series(series_id, load_type="full")
        series_run.complete(RunStatus.SUCCEEDED)
    run.finalize()
    try:
        warehouse.persist_run(run)
        warehouse.persist_run(run)

        rows = warehouse.query(
            """
            SELECT series_id, status, load_type
            FROM audit_etl_series_run
            WHERE run_id = ?
            ORDER BY series_id
            """,
            (run.run_id,),
        )
        assert rows == [
            {"series_id": "A", "status": "succeeded", "load_type": "full"},
            {"series_id": "B", "status": "succeeded", "load_type": "full"},
            {"series_id": "C", "status": "succeeded", "load_type": "full"},
        ]
    finally:
        warehouse.close()


def test_postgres_series_geography_seed_review_and_supersession(postgres_dsn):
    warehouse = PostgresWarehouse(_config(), dsn=postgres_dsn)
    manifests = load_manifests("manifests")
    candidates = build_initial_config_candidates(
        load_global_config("config/global_series.yml"), manifests
    )
    warehouse.sync_meta(manifests)
    repository = PostgresSeriesGeographyRepository(warehouse, manifests)
    try:
        seeded = repository.seed_candidates(candidates)
        assert len(seeded) == 6
        assert repository.seed_candidates(candidates) == seeded
        assert repository.approved_countries() == []

        rejected_candidate = next(
            row for row in seeded if row["series_id"] == "CANCPIALLMINMEI"
        )
        rejected = repository.review_candidate(
            rejected_candidate["mapping_id"], status="rejected", reviewed_by="test"
        )
        assert rejected["status"] == "rejected"
        assert repository.approved_countries() == []

        usa_candidate = next(
            row
            for row in seeded
            if row["series_id"] == "CPIAUCSL" and row["status"] == "candidate"
        )
        first_approval = repository.review_candidate(
            usa_candidate["mapping_id"], status="approved", reviewed_by="test"
        )
        assert first_approval["status"] == "approved"
        assert repository.approved_countries() == [
            {
                "source": "fred",
                "series_id": "CPIAUCSL",
                "country_iso3": "USA",
                "evidence_kind": "global_config",
                "evidence_ref": "config/global_series.yml",
                "mapping_version": 1,
                "reviewed_at_utc": first_approval["reviewed_at_utc"],
            }
        ]

        revised_candidate = replace(candidates[0], evidence_hash="revised-evidence")
        revised = repository.seed_candidates([revised_candidate])[0]
        second_approval = repository.review_candidate(
            revised["mapping_id"], status="approved", reviewed_by="test"
        )
        assert second_approval["mapping_version"] == 2
        old = warehouse.query(
            "SELECT status FROM meta.series_geography WHERE mapping_id = %s",
            (first_approval["mapping_id"],),
        )
        assert old == [{"status": "superseded"}]
        assert repository.approved_countries()[0]["mapping_version"] == 2
    finally:
        warehouse.close()


def test_postgres_series_geography_refuses_source_and_country_conflicts(postgres_dsn):
    warehouse = PostgresWarehouse(_config(), dsn=postgres_dsn)
    manifests = load_manifests("manifests")
    candidates = build_initial_config_candidates(
        load_global_config("config/global_series.yml"), manifests
    )
    repository = PostgresSeriesGeographyRepository(warehouse, manifests)
    try:
        with pytest.raises(ValueError, match="not registered in meta.fred_series"):
            repository.seed_candidates([candidates[0]])

        warehouse.sync_meta(manifests)
        bad_source = replace(candidates[0], source="worldbank")
        with pytest.raises(ValueError, match="expected source"):
            repository.seed_candidates([bad_source])
        assert warehouse.query(
            "SELECT COUNT(*) AS count FROM meta.series_geography"
        ) == [{"count": 0}]

        seeded = repository.seed_candidates([candidates[0]])[0]
        repository.review_candidate(
            seeded["mapping_id"], status="approved", reviewed_by="test"
        )
        conflicting_candidate = replace(
            candidates[0],
            country_iso3="CAN",
            geo_code="CAN",
            evidence_hash="conflicting-country",
        )
        conflicting = repository.seed_candidates([conflicting_candidate])[0]
        with pytest.raises(ValueError, match="conflicts with approved country"):
            repository.review_candidate(
                conflicting["mapping_id"], status="approved", reviewed_by="test"
            )
        assert repository.approved_countries()[0]["country_iso3"] == "USA"
    finally:
        warehouse.close()


def test_postgres_equity_total_return_incremental_matches_local(postgres_dsn, tmp_path):
    local = LocalWarehouse(_config(), db_path=str(tmp_path / "local.db"))
    pg = PostgresWarehouse(_config(), dsn=postgres_dsn)
    query = (
        "SELECT * FROM gold_equity_total_return_index ORDER BY ticker, observation_date"
    )
    pg_query = (
        "SELECT * FROM gold.equity_total_return_index ORDER BY ticker, observation_date"
    )
    try:
        initial = _tiingo_rows(
            [
                ("2024-01-02", 100.0, 0.0, 1.0),
                ("2024-01-03", 101.0, 0.0, 1.0),
                ("2024-01-04", 102.0, 1.0, 1.0),
            ],
            ingested_at="2024-02-01T00:00:00+00:00",
            run_id="initial",
        )
        local.merge_silver(initial)
        pg.merge_silver(initial)
        local.build_gold()
        pg.build_gold()
        assert pg.query(pg_query) == local.query(query)

        forward = _tiingo_rows(
            [("2024-01-05", 103.0, 0.0, 1.0)],
            ingested_at="2024-02-02T00:00:00+00:00",
            run_id="forward",
        )
        local.merge_silver(forward)
        pg.merge_silver(forward)
        local.build_gold()
        pg.build_gold()
        assert pg.query(pg_query) == local.query(query)

        revision = _tiingo_rows(
            [("2024-01-03", 101.0, 2.0, 1.0)],
            ingested_at="2024-02-03T00:00:00+00:00",
            run_id="dividend-restatement",
        )
        local.merge_silver(revision)
        pg.merge_silver(revision)
        local.build_gold()
        pg.build_gold()
        assert pg.query(pg_query) == local.query(query)

        local.build_gold(full=True)
        pg.build_gold(full=True)
        assert pg.query(pg_query) == local.query(query)

        local.build_gold()
        pg.build_gold()
        local.conn.execute(
            "UPDATE gold_incremental_checkpoint SET config_hash = 'stale' "
            "WHERE table_name = 'gold_equity_total_return_index' AND entity_key = 'AAPL'"
        )
        local.conn.commit()
        pg.conn.execute(
            "UPDATE gold_incremental_checkpoint SET config_hash = 'stale' "
            "WHERE table_name = 'gold_equity_total_return_index' AND entity_key = 'AAPL'"
        )
        pg.conn.commit()
        local.build_gold()
        pg.build_gold()
        assert pg.query(pg_query) == local.query(query)

        forward_again = _tiingo_rows(
            [("2024-01-06", 104.0, 0.0, 1.0)],
            ingested_at="2024-02-04T00:00:00+00:00",
            run_id="forward-again",
        )
        local.merge_silver(forward_again)
        pg.merge_silver(forward_again)
        before_rows = pg.query(pg_query)
        before_checkpoints = pg.query(
            "SELECT entity_key, frontier_date, state_json, config_hash "
            "FROM gold_incremental_checkpoint "
            "WHERE table_name = 'gold_equity_total_return_index'"
        )
        write_checkpoints = pg._write_checkpoints_batch

        def fail_after_equity_checkpoint(table_name, entries):
            write_checkpoints(table_name, entries)
            if table_name == "gold_equity_total_return_index":
                raise RuntimeError("simulated checkpoint write interruption")

        pg._write_checkpoints_batch = fail_after_equity_checkpoint
        with pytest.raises(
            RuntimeError, match="simulated checkpoint write interruption"
        ):
            pg.build_gold()
        pg._write_checkpoints_batch = write_checkpoints

        assert pg.query(pg_query) == before_rows
        assert (
            pg.query(
                "SELECT entity_key, frontier_date, state_json, config_hash "
                "FROM gold_incremental_checkpoint "
                "WHERE table_name = 'gold_equity_total_return_index'"
            )
            == before_checkpoints
        )

        local.build_gold()
        pg.build_gold()
        assert pg.query(pg_query) == local.query(query)
    finally:
        pg.close()
        local.close()


def test_postgres_last_ingested_at_by_series_matches_local(postgres_dsn, tmp_path):
    rows = [
        _silver_row(
            "DGS10", "2024-01-01", "", 4.1, 1, ingested_at="2024-01-02T00:00:00+00:00"
        ),
        _silver_row(
            "DGS10", "2024-01-02", "", 4.2, 1, ingested_at="2024-01-03T00:00:00+00:00"
        ),
        _silver_row(
            "DGS2", "2024-01-01", "", 4.5, 1, ingested_at="2024-01-01T00:00:00+00:00"
        ),
    ]
    local = LocalWarehouse(_config(), db_path=str(tmp_path / "local.db"))
    pg = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        local.merge_silver(rows)
        pg.merge_silver(rows)
        assert pg.last_ingested_at_by_series() == local.last_ingested_at_by_series()
        assert pg.last_ingested_at_by_series() == {
            "DGS10": "2024-01-03T00:00:00+00:00",
            "DGS2": "2024-01-01T00:00:00+00:00",
        }
        retry = {**rows[1], "ingested_at": "2024-01-04T00:00:00+00:00"}
        local.merge_silver([retry])
        pg.merge_silver([retry])
        assert pg.last_ingested_at_by_series() == local.last_ingested_at_by_series()
        assert pg.last_ingested_at_by_series()["DGS10"] == rows[1]["ingested_at"]
        assert pg._touched_series_since_watermark(rows[1]["ingested_at"])[0] == {}

        changed = {
            **retry,
            "row_hash": "h-dgs10-revised",
            "value": 4.3,
            "ingested_at": "2024-01-05T00:00:00+00:00",
        }
        local.merge_silver([changed])
        pg.merge_silver([changed])
        assert pg.last_ingested_at_by_series() == local.last_ingested_at_by_series()
        assert pg.last_ingested_at_by_series()["DGS10"] == changed["ingested_at"]
        assert pg._touched_series_since_watermark(rows[1]["ingested_at"])[0] == {
            "DGS10": "2024-01-02"
        }
    finally:
        pg.close()
        local.close()


def test_warehouse_factory_builds_postgres(postgres_dsn):
    factory = WarehouseFactory(
        _config(),
        WarehouseConfig(
            primary_backend="postgres",
            backends={"postgres": {"dsn": postgres_dsn}},
        ),
    )
    wh = factory.build()
    try:
        assert isinstance(wh, PostgresWarehouse)
    finally:
        wh.close()


def _ecb_asset_xml(isin: str, haircut: str, *, change: str | None = None) -> str:
    tag = "assetUpdate" if change else "asset"
    change_xml = f"<change>{change}</change>" if change else ""
    return (
        f"<{tag}><id><isinCode>{isin}</isinCode><otherRegNumber/></id>"
        "<haircutCategory>L1A</haircutCategory><type>AT01</type>"
        "<referenceMarket>RMDE01</referenceMarket><denomination>EUR</denomination>"
        f"<nonOwnUseHaircut>{haircut}</nonOwnUseHaircut>{change_xml}</{tag}>"
    )


def _ecb_release(release_id: str, day: str) -> ReleaseMessage:
    return ReleaseMessage(
        release_id=release_id,
        url=f"https://mid.ecb.europa.eu/rel/{release_id}.xml",
        release_type="EligibleAssetsList",
        release_title="List of eligible marketable assets",
        release_at_local=f"{day}T17:56:07+01:00",
        publication_at_local=f"{day}T17:56:07+01:00",
        publication_at_utc=f"{day}T16:56:07+00:00",
        rss_pub_at=f"{day}T16:56:07+00:00",
        attachments=(),
    )


def _publish_two_ecb_releases(warehouse):
    repo = ECBEligibleAssetRepository(warehouse)
    dictionary = [
        {
            "domain": "assetTypes",
            "code": "AT01",
            "definition": "Bond",
            "short_definition": None,
            "attributes_json": "{}",
        }
    ]
    first = parse_assets_xml(
        (
            "<assets>"
            + _ecb_asset_xml("DE0000000001", "1.0")
            + _ecb_asset_xml("DE0000000002", "2.0")
            + "</assets>"
        ).encode()
    ).rows
    release_1 = _ecb_release("release-one", "2024-01-01")
    repo.record_release(release_1, run_id="run-one")
    repo.publish(
        release_1,
        first,
        {},
        {},
        diff_assets({}, first),
        dictionary,
        dictionary_hash="dictionary-v1",
        schema_hash="schema-v1",
        reconciliation_status="initial_snapshot",
        coverage_status="starts_here",
        change_origin="initial_full_snapshot",
        run_id="run-one",
    )

    prior = repo.current_assets()
    second = parse_assets_xml(
        (
            "<assets>"
            + _ecb_asset_xml("DE0000000001", "1.5")
            + _ecb_asset_xml("DE0000000003", "3.0")
            + "</assets>"
        ).encode()
    ).rows
    update = parse_assets_xml(
        (
            "<assetsUpdates>"
            + _ecb_asset_xml("DE0000000001", "1.5", change="U")
            + _ecb_asset_xml("DE0000000002", "2.0", change="D")
            + _ecb_asset_xml("DE0000000003", "3.0", change="N")
            + "</assetsUpdates>"
        ).encode()
    ).rows
    changes = diff_assets(prior, second)
    release_2 = _ecb_release("release-two", "2024-01-02")
    repo.record_release(release_2, run_id="run-two")
    repo.publish(
        release_2,
        second,
        prior,
        update,
        changes,
        dictionary,
        dictionary_hash="dictionary-v1",
        schema_hash="schema-v1",
        reconciliation_status="matched",
        coverage_status="complete",
        change_origin="source_reconciled",
        run_id="run-two",
    )
    return repo


def _ecb_snapshot(repo, timestamp: str):
    return [
        {
            "asset_key": row["asset_key"],
            "haircut": row["non_own_use_haircut"],
            "label": row["asset_type_label"],
            "release": row["selected_release_id"],
            "coverage": row["coverage_status"],
        }
        for row in repo.as_of(timestamp)
    ]


def test_ecb_point_in_time_history_persists_with_local_postgres_parity(
    postgres_dsn, tmp_path
):
    local = LocalWarehouse(_config(), db_path=str(tmp_path / "ecb-parity.db"))
    postgres = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        local_repo = _publish_two_ecb_releases(local)
        postgres_repo = _publish_two_ecb_releases(postgres)

        before = "2024-01-01T17:00:00+00:00"
        after = "2024-01-02T17:00:00+00:00"
        assert _ecb_snapshot(postgres_repo, before) == _ecb_snapshot(local_repo, before)
        assert _ecb_snapshot(postgres_repo, after) == _ecb_snapshot(local_repo, after)
        assert {row["asset_key"] for row in _ecb_snapshot(postgres_repo, after)} == {
            "ISIN:DE0000000001",
            "ISIN:DE0000000003",
        }
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM bronze.ecb_eligible_asset_release"
        ) == [{"n": 2}]
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM silver.ecb_eligible_asset_version"
        ) == [{"n": 5}]
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM gold.ecb_eligible_security_current"
        ) == [{"n": 2}]
    finally:
        local.close()
        postgres.close()


def test_ecb_source_noop_is_audited_without_new_postgres_version(postgres_dsn):
    postgres = PostgresWarehouse(_config(), dsn=postgres_dsn)
    repo = ECBEligibleAssetRepository(postgres)
    dictionary = [
        {
            "domain": "assetTypes",
            "code": "AT01",
            "definition": "Bond",
            "short_definition": None,
            "attributes_json": "{}",
        }
    ]
    full = parse_assets_xml(
        ("<assets>" + _ecb_asset_xml("DE0000000004", "1.0") + "</assets>").encode()
    ).rows
    update = parse_assets_xml(
        (
            "<assetsUpdates>"
            + _ecb_asset_xml("DE0000000004", "1.0", change="U")
            + "</assetsUpdates>"
        ).encode()
    ).rows
    release_1 = _ecb_release("source-noop-one", "2024-01-01")
    release_2 = _ecb_release("source-noop-two", "2024-01-02")
    key = "ISIN:DE0000000004"
    try:
        repo.record_release(release_1, run_id="source-noop-run-one")
        repo.publish(
            release_1,
            full,
            {},
            {},
            diff_assets({}, full),
            dictionary,
            dictionary_hash="dictionary-v1",
            schema_hash="schema-v1",
            reconciliation_status="initial_snapshot",
            coverage_status="starts_here",
            change_origin="initial_full_snapshot",
            run_id="source-noop-run-one",
        )
        repo.record_release(release_2, run_id="source-noop-run-two")
        result = repo.publish(
            release_2,
            full,
            repo.current_assets(),
            update,
            {},
            dictionary,
            dictionary_hash="dictionary-v1",
            schema_hash="schema-v1",
            reconciliation_status="matched_with_source_noops",
            coverage_status="complete",
            change_origin="source_reconciled",
            run_id="source-noop-run-two",
            source_noop_updates=(key,),
        )

        assert result.source_noop_count == 1
        assert postgres.query(
            "SELECT declared_change_type, computed_change_type, change_origin "
            "FROM silver.ecb_eligible_asset_change "
            "WHERE release_id = %s AND asset_key = %s",
            (release_2.release_id, key),
        ) == [
            {
                "declared_change_type": "U",
                "computed_change_type": None,
                "change_origin": "source_noop",
            }
        ]
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM silver.ecb_eligible_asset_version "
            "WHERE asset_key = %s",
            (key,),
        ) == [{"n": 1}]
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM gold.ecb_eligible_security_current "
            "WHERE asset_key = %s",
            (key,),
        ) == [{"n": 1}]

        duplicate = repo.publish(
            release_2,
            full,
            repo.current_assets(),
            update,
            {},
            dictionary,
            dictionary_hash="dictionary-v1",
            schema_hash="schema-v1",
            reconciliation_status="matched_with_source_noops",
            coverage_status="complete",
            change_origin="source_reconciled",
            run_id="source-noop-run-two",
            source_noop_updates=(key,),
        )
        assert duplicate.status == "duplicate"
        assert postgres.query(
            "SELECT COUNT(*) AS n FROM silver.ecb_eligible_asset_change "
            "WHERE release_id = %s",
            (release_2.release_id,),
        ) == [{"n": 1}]
    finally:
        postgres.close()


# ---------------------------------------------------------------------------
# Upsert keys. A database seeded by scripts/copy_sqlite_to_postgres.py has the
# tables but none of their primary keys, so the first ON CONFLICT upsert failed
# with "there is no unique or exclusion constraint matching the ON CONFLICT
# specification" -- and then every later call failed with a different, useless
# error because the aborted transaction was never rolled back.
# ---------------------------------------------------------------------------


def _drop_all_keys(dsn: str) -> None:
    """Put the database in the state the copy script leaves it in: every upsert
    target present, none of them with a primary key or unique constraint."""
    import psycopg

    from fred_pipeline.io.postgres_store import _UPSERT_KEYS, _split_table_name

    with psycopg.connect(dsn, autocommit=True) as conn:
        for flat in _UPSERT_KEYS:
            schema, table = _split_table_name(flat)
            names = [
                r[0]
                for r in conn.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = %s::regclass AND contype IN ('p', 'u')",
                    (f"{schema}.{table}",),
                )
            ]
            for name in names:
                conn.execute(f'ALTER TABLE {schema}."{table}" DROP CONSTRAINT "{name}"')
            for (idx,) in list(
                conn.execute(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE schemaname = %s AND tablename = %s "
                    "AND indexdef LIKE 'CREATE UNIQUE%%'",
                    (schema, table),
                )
            ):
                conn.execute(f'DROP INDEX {schema}."{idx}"')


def _unique_key_exists(dsn: str, flat_table: str) -> bool:
    import psycopg

    from fred_pipeline.io.postgres_store import (
        _UPSERT_KEYS,
        PostgresWarehouse as PW,
        _split_table_name,
    )

    schema, table = _split_table_name(flat_table)
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        return PW._has_unique_index(cur, schema, table, _UPSERT_KEYS[flat_table])


def test_upsert_keys_match_the_primary_keys_the_ddl_declares():
    """_UPSERT_KEYS is hand-written; the DDL is what actually defines each key.
    If they drift, the startup repair would build a key the table was never
    meant to have."""
    import re

    from fred_pipeline.io.postgres_store import _UPSERT_KEYS, _sqlite_table_defs

    bodies = dict(_sqlite_table_defs())
    for flat, columns in _UPSERT_KEYS.items():
        body = bodies[flat]
        composite = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", body)
        if composite:
            declared = tuple(c.strip() for c in composite.group(1).split(","))
        else:
            inline = re.search(r"(\w+)\s+\w+\s+PRIMARY KEY", body)
            assert inline, f"{flat} declares no primary key"
            declared = (inline.group(1),)
        assert declared == columns, flat


def test_postgres_ddl_preserves_composite_unique_constraints():
    from fred_pipeline.io.postgres_store import _postgres_create_table_sql

    sql = _postgres_create_table_sql(
        "silver",
        "example",
        "run_id TEXT, snapshot_date TEXT, UNIQUE (run_id, snapshot_date)",
    )

    assert 'UNIQUE ("run_id", "snapshot_date")' in sql
    assert '"UNIQUE" TEXT' not in sql


def test_a_database_without_keys_gets_them_back_and_can_upsert(postgres_dsn):
    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)
    assert not _unique_key_exists(postgres_dsn, "meta_fred_series")
    assert not _unique_key_exists(postgres_dsn, "silver_fred_observation")

    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)  # the repair runs here
    try:
        from fred_pipeline.io.postgres_store import _UPSERT_KEYS

        for flat in _UPSERT_KEYS:
            assert _unique_key_exists(postgres_dsn, flat), flat
        # And the thing that was actually failing now works, twice, idempotently.
        keys = _UPSERT_KEYS["meta_fred_series"]
        wh._insert(
            "meta_fred_series", [{"series_id": "X", "title": "a"}], upsert_keys=keys
        )
        wh._insert(
            "meta_fred_series", [{"series_id": "X", "title": "b"}], upsert_keys=keys
        )
        rows = wh.query("SELECT title FROM meta.fred_series WHERE series_id = 'X'")
        assert [r["title"] for r in rows] == ["b"]
    finally:
        wh.close()


def test_the_repair_warns_once_and_a_healthy_database_is_left_alone(
    postgres_dsn, caplog
):
    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)

    with caplog.at_level("WARNING", logger="fred_pipeline"):
        PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    assert "has no unique key on (series_id)" in caplog.text
    assert "copy_sqlite_to_postgres.py" in caplog.text

    caplog.clear()
    with caplog.at_level("WARNING", logger="fred_pipeline"):
        PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    assert "no unique key" not in caplog.text, "second start must not rebuild"


def test_duplicate_rows_stop_the_repair_with_a_clear_error_and_change_nothing(
    postgres_dsn,
):
    import psycopg

    from fred_pipeline.io.postgres_store import PostgresSchemaError

    PostgresWarehouse(_config(), dsn=postgres_dsn).close()
    _drop_all_keys(postgres_dsn)
    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO meta.fred_series (series_id, title) "
            "VALUES ('DUP', 'one'), ('DUP', 'two')"
        )

    with pytest.raises(PostgresSchemaError) as exc:
        PostgresWarehouse(_config(), dsn=postgres_dsn)

    message = str(exc.value)
    assert '"meta"."fred_series"' in message  # quoted: pasteable into psql
    assert "duplicate rows" in message
    assert "HAVING count(*) > 1" in message  # the query to find them is included
    assert "Nothing was changed" in message
    assert not _unique_key_exists(postgres_dsn, "meta_fred_series")
    with psycopg.connect(postgres_dsn) as conn:
        n = conn.execute(
            "SELECT count(*) FROM meta.fred_series WHERE series_id = 'DUP'"
        ).fetchone()[0]
    assert n == 2  # the user's data is untouched


def test_a_failed_statement_does_not_poison_the_connection(postgres_dsn):
    """Regression for the cascade: one error used to leave the transaction
    aborted, so the NEXT, unrelated call failed with 'current transaction is
    aborted' and the real cause was gone."""
    import psycopg

    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        with pytest.raises(psycopg.errors.UndefinedColumn):
            wh._insert(
                "meta_fred_series",
                [{"series_id": "A", "not_a_column": 1}],
                upsert_keys=("series_id",),
            )
        # The very next call must work.
        wh._insert(
            "meta_fred_series",
            [{"series_id": "B", "title": "ok"}],
            upsert_keys=("series_id",),
        )
        assert wh.query("SELECT series_id FROM meta.fred_series") == [
            {"series_id": "B"}
        ]
    finally:
        wh.close()


def test_postgres_retains_changed_observed_states(postgres_dsn):
    wh = PostgresWarehouse(_config(), dsn=postgres_dsn)
    try:
        base = _silver_row(
            "ECB:EXR:D.USD.EUR.SP00.A",
            "2024-01-02",
            "",
            1.25,
            1,
            source="ecb",
            ingested_at="2024-01-02T16:00:00+00:00",
        )
        first = {**base, "row_hash": "a", "run_id": "a"}
        second = {
            **base,
            "value": 1.30,
            "raw_value": "1.30",
            "row_hash": "b",
            "ingested_at": "2024-01-03T16:00:00+00:00",
            "run_id": "b",
        }
        third = {
            **first,
            "ingested_at": "2024-01-04T16:00:00+00:00",
            "run_id": "c",
        }
        for row in (first, second, third):
            wh.merge_silver([row])
        history = wh.query(
            "SELECT value FROM silver.fred_observation_history ORDER BY ingested_at"
        )
        assert history == [{"value": 1.25}, {"value": 1.30}, {"value": 1.25}]
    finally:
        wh.close()

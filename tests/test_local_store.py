import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.transform import daily_feature_matrix


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def test_local_run_persists_all_layers(tmp_path, observations_payload, fake_client_cls):
    db = str(tmp_path / "fred.db")
    client = fake_client_cls({"DGS10": observations_payload})
    wh = LocalWarehouse(_config(), db_path=db)
    pipe = FredPipeline(_config(), client=client, warehouse=wh)

    run = pipe.run([_spec("DGS10")], build_gold_layer=True)
    assert run.status == RunStatus.SUCCEEDED

    # bronze got the verbatim payload
    bronze = wh.query("SELECT * FROM bronze_fred_api_response")
    assert len(bronze) == 1
    assert bronze[0]["observation_count"] == 4

    # silver got 4 normalized rows (3 real values + 1 missing)
    silver = wh.query("SELECT * FROM silver_fred_observation ORDER BY observation_date")
    assert len(silver) == 4
    assert sum(r["is_missing"] for r in silver) == 1

    # gold latest observation built
    latest = wh.query("SELECT * FROM gold_fred_latest_observation")
    assert len(latest) == 4

    # daily feature matrix spans the observation window (4 days, 1 series)
    daily = wh.query("SELECT * FROM gold_fred_macro_feature_daily ORDER BY as_of_date")
    assert len(daily) == 4
    # last day forward-fills the last real value
    assert daily[-1]["value"] == 4.40

    # audit persisted
    assert len(wh.query("SELECT * FROM audit_etl_run")) == 1
    assert len(wh.query("SELECT * FROM audit_etl_series_run")) == 1
    assert len(wh.query("SELECT * FROM audit_data_quality_result")) >= 1
    wh.close()


def test_local_run_builds_dim_date_and_market_calendar(
    tmp_path, observations_payload, fake_client_cls
):
    db = str(tmp_path / "fred.db")
    client = fake_client_cls({"DGS10": observations_payload})
    wh = LocalWarehouse(_config(), db_path=db)
    pipe = FredPipeline(_config(), client=client, warehouse=wh)
    pipe.run([_spec("DGS10")], build_gold_layer=True)

    obs_dates = {
        r["observation_date"]
        for r in wh.query("SELECT observation_date FROM silver_fred_observation")
    }
    n_days = len(obs_dates)

    dim_date = wh.query("SELECT * FROM gold_dim_date")
    assert len(dim_date) == n_days
    assert {"is_imm_date", "is_monthly_option_expiry", "is_triple_witching"} <= set(
        dim_date[0].keys()
    )

    market_calendar = wh.query("SELECT * FROM gold_market_calendar")
    assert len(market_calendar) == n_days * 3
    assert {r["calendar_name"] for r in market_calendar} == {"NYSE", "SIFMA", "FEDWIRE"}
    wh.close()


def test_local_backend_gold_views_exist_and_match_tables(
    tmp_path, observations_payload, fake_client_cls
):
    """SQLite equivalents of the Delta-only gold.v_* views (sql/60_views.sql)
    must exist and agree with the tables they're derived from."""
    db = str(tmp_path / "fred.db")
    client = fake_client_cls({"DGS10": observations_payload})
    wh = LocalWarehouse(_config(), db_path=db)
    pipe = FredPipeline(_config(), client=client, warehouse=wh)
    pipe.run([_spec("DGS10")], build_gold_layer=True)

    views = {
        r["name"] for r in wh.query("SELECT name FROM sqlite_master WHERE type='view'")
    }
    assert views == {
        "gold_v_latest_revised",
        "gold_v_point_in_time",
        "gold_v_series_latest_value",
        "gold_v_series_revision_summary",
        "gold_v_source_coverage",
        "gold_v_company_ratio_ranks",
    }

    latest_table = wh.query("SELECT * FROM gold_fred_latest_observation")
    latest_view = wh.query("SELECT * FROM gold_v_latest_revised")
    assert len(latest_view) == len(latest_table)

    pit_table = wh.query("SELECT * FROM gold_fred_point_in_time")
    pit_view = wh.query("SELECT * FROM gold_v_point_in_time")
    assert len(pit_view) == len(pit_table)

    # one non-missing row per series in the latest-value view
    latest_value = wh.query("SELECT * FROM gold_v_series_latest_value")
    assert {r["series_id"] for r in latest_value} == {"DGS10"}

    revision_summary = wh.query("SELECT * FROM gold_v_series_revision_summary")
    assert {r["series_id"] for r in revision_summary} == {"DGS10"}
    assert revision_summary[0]["observation_count"] > 0
    wh.close()


def test_local_gold_rebuild_rolls_back_on_failure(tmp_path, monkeypatch):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.conn.execute(
        """
        INSERT INTO gold_fred_point_in_time (
            series_id, observation_date, realtime_start, realtime_end, value,
            revision_number, is_missing, ingested_at
        )
        VALUES ('OLD', '2024-01-01', '', '', 1.0, 1, 0, 'old')
        """
    )
    wh.conn.commit()
    wh.merge_silver(
        [
            {
                "source": "fred",
                "series_id": "DGS10",
                "observation_date": "2024-01-01",
                "realtime_start": "",
                "realtime_end": "",
                "value": 4.0,
                "raw_value": "4.0",
                "is_missing": False,
                "row_hash": "h",
                "ingested_at": "now",
                "run_id": "r",
            }
        ]
    )

    original_insert = wh._insert

    def fail_first_gold_insert(table, rows, upsert_keys=None):
        if table == "gold_fred_point_in_time":
            raise RuntimeError("boom")
        return original_insert(table, rows, upsert_keys)

    monkeypatch.setattr(wh, "_insert", fail_first_gold_insert)

    with pytest.raises(RuntimeError, match="boom"):
        wh.build_gold()

    rows = wh.query("SELECT series_id FROM gold_fred_point_in_time")
    assert rows == [{"series_id": "OLD"}]
    wh.close()


def test_local_run_is_idempotent(tmp_path, observations_payload, fake_client_cls):
    db = str(tmp_path / "fred.db")
    cfg = _config()

    def one_run():
        wh = LocalWarehouse(cfg, db_path=db)
        pipe = FredPipeline(
            cfg, client=fake_client_cls({"DGS10": observations_payload}), warehouse=wh
        )
        pipe.run([_spec("DGS10")])
        n = len(wh.query("SELECT * FROM silver_fred_observation"))
        wh.close()
        return n

    assert one_run() == 4
    # second run MERGEs on the natural key -> still 4, no duplicates
    assert one_run() == 4


def test_meta_sync_registers_full_universe(tmp_path):
    from fred_pipeline.manifest import all_series, load_manifests

    manifests = load_manifests("manifests")
    n_series = len(all_series(manifests, active_only=False))
    n_manifests = len(manifests)

    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    counts = wh.sync_meta(manifests)

    assert counts["fred_series"] == n_series
    assert len(wh.query("SELECT * FROM meta_fred_series")) == n_series
    assert len(wh.query("SELECT * FROM meta_fred_manifest")) == n_manifests
    # re-sync is idempotent (upsert on primary key)
    wh.sync_meta(load_manifests("manifests"))
    assert len(wh.query("SELECT * FROM meta_fred_series")) == n_series
    wh.close()


def test_source_is_part_of_natural_key(tmp_path):
    """Two rows identical except for ``source`` must coexist; re-merging one
    source updates in place rather than duplicating."""
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    base = {
        "series_id": "X",
        "observation_date": "2024-01-01",
        "realtime_start": "",
        "realtime_end": "",
        "value": 1.0,
        "raw_value": "1.0",
        "is_missing": False,
        "row_hash": "h",
        "revision_number": 1,
        "ingested_at": "t",
        "run_id": "r",
    }
    wh.merge_silver([{**base, "source": "fred"}])
    wh.merge_silver([{**base, "source": "bls"}])
    # same (series_id, date, realtime) but different source -> two distinct rows
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2

    # re-merging the fred row updates in place (idempotent per source)
    wh.merge_silver([{**base, "source": "fred", "value": 2.0}])
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    got = wh.query("SELECT value FROM silver_fred_observation WHERE source='fred'")
    assert got[0]["value"] == 2.0
    wh.close()


def test_query_logs_to_audit_query_log(tmp_path):
    import hashlib

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "q.db"))
    wh.query("SELECT 1", caller="test-notebook")

    rows = wh.query("SELECT * FROM audit_query_log ORDER BY queried_at")
    # the query() call above logs itself before the SELECT 1 executes, then
    # this query() call logs itself too -- both show up here.
    assert len(rows) == 2
    assert rows[0]["caller"] == "test-notebook"
    assert rows[0]["query_text_hash"] == hashlib.sha256(b"SELECT 1").hexdigest()
    assert rows[1]["caller"] == ""  # this call didn't pass a caller
    wh.close()


def test_query_caller_defaults_to_blank(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "q2.db"))
    wh.query("SELECT 1")
    rows = wh.query("SELECT caller FROM audit_query_log")
    assert all(r["caller"] == "" for r in rows)
    wh.close()


def test_additive_migration_adds_column_to_preexisting_db(tmp_path):
    """A database file written before gold_dim_series gained `geo` must gain
    the column on open -- CREATE TABLE IF NOT EXISTS won't add it, and every
    dim_series insert would otherwise fail against an older local file."""
    import sqlite3

    db = str(tmp_path / "legacy.db")
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE gold_dim_series ("
        " series_id TEXT PRIMARY KEY, title TEXT, source TEXT, frequency TEXT,"
        " units TEXT, econ_category TEXT, polarity INTEGER,"
        " default_transform TEXT, scale TEXT, decimals INTEGER, notes TEXT)"
    )
    con.execute(
        "INSERT INTO gold_dim_series (series_id, econ_category)"
        " VALUES ('UNRATE', 'LABOR')"
    )
    con.commit()
    con.close()

    wh = LocalWarehouse(_config(), db_path=db)
    cols = {r[1] for r in wh.conn.execute("PRAGMA table_info(gold_dim_series)")}
    assert "geo" in cols
    # pre-existing rows survive the migration
    (row,) = wh.query("SELECT series_id, geo FROM gold_dim_series")
    assert row["series_id"] == "UNRATE" and row["geo"] is None
    wh.close()

    # reopening is a no-op, not a duplicate-column error
    wh2 = LocalWarehouse(_config(), db_path=db)
    wh2.close()


def test_daily_feature_matrix_forward_fills():
    latest = [
        {
            "series_id": "X",
            "observation_date": "2024-01-01",
            "value": 1.0,
            "is_missing": False,
        },
        {
            "series_id": "X",
            "observation_date": "2024-01-03",
            "value": 2.0,
            "is_missing": False,
        },
    ]
    rows = daily_feature_matrix(latest)
    assert [r["as_of_date"] for r in rows] == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert [r["value"] for r in rows] == [1.0, 1.0, 2.0]  # jan-02 forward-filled
    assert rows[1]["raw_value"] is None  # no native release on jan-02

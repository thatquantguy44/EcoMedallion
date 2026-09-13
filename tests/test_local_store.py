import pytest

from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.transform import (
    _row_hash,
    daily_feature_matrix,
    latest_by_observation,
)


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


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
        # gold_fred_point_in_time is itself a view now (spec003 section 7) --
        # a pure 1:1 mirror of Silver, identical to gold_v_point_in_time.
        "gold_fred_point_in_time",
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


def test_local_sql_core_gold_rebuild_matches_python_output(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
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
    wh.merge_silver(rows)

    # gold_fred_point_in_time is a live VIEW over Silver (spec003 section 7)
    # -- no rebuild call needed, it already reflects the merge above.
    wh._rebuild_gold_latest_observation_sql()

    pit_cols = [
        "series_id",
        "observation_date",
        "realtime_start",
        "realtime_end",
        "value",
        "revision_number",
        "is_missing",
        "ingested_at",
    ]
    pit_sql = (
        "SELECT "
        + ", ".join(pit_cols)
        + " FROM gold_fred_point_in_time"
        + " ORDER BY series_id, observation_date, realtime_start"
    )
    pit_expected = [
        {col: (int(row[col]) if col == "is_missing" else row[col]) for col in pit_cols}
        for row in sorted(
            rows,
            key=lambda r: (r["series_id"], r["observation_date"], r["realtime_start"]),
        )
    ]
    assert wh.query(pit_sql) == pit_expected

    latest_cols = [
        "series_id",
        "observation_date",
        "value",
        "realtime_start",
        "realtime_end",
        "is_missing",
        "revision_number",
        "ingested_at",
    ]
    latest_sql = (
        "SELECT "
        + ", ".join(latest_cols)
        + " FROM gold_fred_latest_observation"
        + " ORDER BY series_id, observation_date"
    )
    python_latest = latest_by_observation(
        [{**row, "is_missing": bool(row["is_missing"])} for row in rows]
    )
    latest_expected = [
        {
            col: (int(row[col]) if col == "is_missing" else row[col])
            for col in latest_cols
        }
        for row in python_latest
    ]
    assert wh.query(latest_sql) == latest_expected
    wh.close()


def test_local_gold_rebuild_rolls_back_on_failure(tmp_path, monkeypatch):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.conn.execute(
        """
        INSERT INTO gold_fred_latest_observation (
            series_id, observation_date, value, realtime_start, realtime_end,
            is_missing, revision_number, ingested_at
        )
        VALUES ('OLD', '2024-01-01', 1.0, '', '', 0, 1, 'old')
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

    def fail_latest_observation_sql():
        wh.conn.execute("DELETE FROM gold_fred_latest_observation")
        raise RuntimeError("boom")

    monkeypatch.setattr(
        wh, "_rebuild_gold_latest_observation_sql", fail_latest_observation_sql
    )

    with pytest.raises(RuntimeError, match="boom"):
        wh.build_gold()

    rows = wh.query("SELECT series_id FROM gold_fred_latest_observation")
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
        "row_hash": _row_hash("X", "2024-01-01", "", "1.0"),
        "revision_number": 1,
        "ingested_at": "t",
        "run_id": "r",
    }
    wh.merge_silver([{**base, "source": "fred"}])
    wh.merge_silver([{**base, "source": "bls"}])
    # same (series_id, date, realtime) but different source -> two distinct rows
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2

    # re-merging the fred row with a genuinely changed value (and thus a
    # changed row_hash, exactly as real normalize() output would produce)
    # updates in place (idempotent per source)
    wh.merge_silver(
        [
            {
                **base,
                "source": "fred",
                "value": 2.0,
                "raw_value": "2.0",
                "row_hash": _row_hash("X", "2024-01-01", "", "2.0"),
            }
        ]
    )
    assert wh.query("SELECT count(*) c FROM silver_fred_observation")[0]["c"] == 2
    got = wh.query("SELECT value FROM silver_fred_observation WHERE source='fred'")
    assert got[0]["value"] == 2.0
    wh.close()


def test_merge_silver_leaves_ingested_at_unchanged_on_byte_identical_reupsert(
    tmp_path,
):
    """spec003 Phase 3 prerequisite: restate_last_n re-pulls the trailing ~90
    observations of nearly every series on every routine run regardless of
    whether any value actually changed. If a byte-identical re-upsert bumped
    ingested_at anyway, "series touched since the last Gold build" would mean
    "every series, every run" -- there would be no incremental signal to act
    on. row_hash (unchanged here) must gate the update, not just the upsert
    key matching."""
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    row = {
        "source": "fred",
        "series_id": "X",
        "observation_date": "2024-01-01",
        "realtime_start": "",
        "realtime_end": "",
        "value": 1.0,
        "raw_value": "1.0",
        "is_missing": False,
        "row_hash": _row_hash("X", "2024-01-01", "", "1.0"),
        "revision_number": 1,
        "ingested_at": "2024-01-01T00:00:00+00:00",
        "run_id": "r1",
    }
    wh.merge_silver([row])

    # Re-upsert the identical content under a later run_id/ingested_at, the
    # exact shape a routine run's restate window produces for an unchanged
    # observation.
    wh.merge_silver(
        [{**row, "ingested_at": "2024-06-01T00:00:00+00:00", "run_id": "r2"}]
    )

    got = wh.query(
        "SELECT ingested_at, run_id FROM silver_fred_observation WHERE series_id='X'"
    )
    assert len(got) == 1
    assert got[0]["ingested_at"] == "2024-01-01T00:00:00+00:00"
    assert got[0]["run_id"] == "r1"

    # A genuine value change (different row_hash) still updates in place.
    wh.merge_silver(
        [
            {
                **row,
                "value": 2.0,
                "raw_value": "2.0",
                "row_hash": _row_hash("X", "2024-01-01", "", "2.0"),
                "ingested_at": "2024-06-01T00:00:00+00:00",
                "run_id": "r2",
            }
        ]
    )
    got = wh.query(
        "SELECT value, ingested_at, run_id FROM silver_fred_observation WHERE series_id='X'"
    )
    assert len(got) == 1
    assert got[0]["value"] == 2.0
    assert got[0]["ingested_at"] == "2024-06-01T00:00:00+00:00"
    assert got[0]["run_id"] == "r2"
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


# ---- spec003 Phase 3: gold_curve_spread_daily incremental end-to-end -------


def _obs_row(series_id, observation_date, value, ingested_at, run_id="r"):
    raw = str(value)
    return {
        "source": "fred",
        "series_id": series_id,
        "observation_date": observation_date,
        "realtime_start": "",
        "realtime_end": "",
        "value": value,
        "raw_value": raw,
        "is_missing": False,
        "row_hash": _row_hash(series_id, observation_date, "", raw),
        "revision_number": 1,
        "ingested_at": ingested_at,
        "run_id": run_id,
    }


def _checkpoint(wh, spread_name="T10Y2Y"):
    row = wh.query(
        "SELECT frontier_date, state_json, updated_at FROM "
        "gold_incremental_checkpoint WHERE table_name='gold_curve_spread_daily' "
        "AND entity_key=?",
        (spread_name,),
    )
    return row[0] if row else None


def _spread_rows(wh, spread_name="T10Y2Y"):
    return wh.query(
        "SELECT * FROM gold_curve_spread_daily WHERE spread_name=? "
        "ORDER BY observation_date",
        (spread_name,),
    )


def test_incremental_curve_spread_daily_second_build_skips_untouched_spread(
    tmp_path,
):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-02", 4.0, "2024-01-02T00:00:00Z"),
            _obs_row("DGS2", "2024-01-02", 3.8, "2024-01-02T00:00:00Z"),
        ]
    )
    wh.build_gold()

    first_rows = _spread_rows(wh)
    assert len(first_rows) == 1
    first_checkpoint = _checkpoint(wh)
    assert first_checkpoint is not None
    assert first_checkpoint["frontier_date"] == "2024-01-02"

    # No Silver changes -> the second build must skip this spread entirely,
    # not just produce identical output via a redundant recompute.
    wh.build_gold()
    assert _spread_rows(wh) == first_rows
    assert _checkpoint(wh)["updated_at"] == first_checkpoint["updated_at"]
    wh.close()


def test_incremental_curve_spread_daily_appends_new_observation(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-02", 4.0, "2024-01-02T00:00:00Z"),
            _obs_row("DGS2", "2024-01-02", 3.8, "2024-01-02T00:00:00Z"),
        ]
    )
    wh.build_gold()
    before = _spread_rows(wh)
    assert [r["observation_date"] for r in before] == ["2024-01-02"]

    # A new trailing observation, strictly past the checkpoint frontier.
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-03", 4.1, "2024-01-03T00:00:00Z"),
            _obs_row("DGS2", "2024-01-03", 3.9, "2024-01-03T00:00:00Z"),
        ]
    )
    wh.build_gold()
    after = _spread_rows(wh)
    assert [r["observation_date"] for r in after] == ["2024-01-02", "2024-01-03"]
    # The already-written first row is untouched by the append.
    assert after[0] == before[0]
    assert _checkpoint(wh)["frontier_date"] == "2024-01-03"
    wh.close()


def test_incremental_curve_spread_daily_backfill_triggers_full_recompute(tmp_path):
    from fred_pipeline.terminal_views import compute_curve_spread_daily

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-02", 4.0, "2024-01-02T00:00:00Z"),
            _obs_row("DGS2", "2024-01-02", 3.8, "2024-01-02T00:00:00Z"),
            _obs_row("DGS10", "2024-01-03", 4.1, "2024-01-03T00:00:00Z"),
            _obs_row("DGS2", "2024-01-03", 3.9, "2024-01-03T00:00:00Z"),
        ]
    )
    wh.build_gold()
    assert _checkpoint(wh)["frontier_date"] == "2024-01-03"

    # Revise the OLDER date (2024-01-02), a genuine value change -> row_hash
    # differs -> ingested_at advances -> min_touched (01-02) <= frontier
    # (01-03) -> the backfill branch, not the append branch.
    wh.merge_silver([_obs_row("DGS10", "2024-01-02", 4.5, "2024-06-01T00:00:00Z")])
    wh.build_gold()

    got = _spread_rows(wh)
    # Ground truth: a full, from-scratch compute over the final Silver state.
    latest = wh.query("SELECT * FROM gold_fred_latest_observation")
    for r in latest:
        r["is_missing"] = bool(r["is_missing"])
    from fred_pipeline.spread_config import SpreadDef

    expected = [
        r
        for r in compute_curve_spread_daily(
            latest, [SpreadDef("T10Y2Y", "DGS10", "DGS2")]
        )
    ]
    assert [dict(r) for r in got] == expected
    wh.close()


def test_incremental_curve_spread_daily_new_config_entry_builds_even_if_untouched(
    tmp_path, monkeypatch
):
    """A spread with no checkpoint yet must be built on its first
    opportunity even if neither leg happens to be in this run's touched
    set (e.g. a config entry added after the legs were already ingested)."""
    from fred_pipeline.spread_config import SpreadDef

    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-02", 4.0, "2024-01-02T00:00:00Z"),
            _obs_row("DGS2", "2024-01-02", 3.8, "2024-01-02T00:00:00Z"),
        ]
    )
    wh.build_gold()
    assert _checkpoint(wh, "T10Y2Y") is not None

    # Advance the watermark past this data (simulating time passing with no
    # further Silver activity), then "add" a new spread config entry whose
    # legs were already ingested before this run -- neither leg is in the
    # touched set this time.
    import fred_pipeline.spread_config as spread_config_module

    original = spread_config_module.load_spread_defs

    def _with_new_entry(*a, **kw):
        return [*original(*a, **kw), SpreadDef("T10Y2Y_DUP", "DGS10", "DGS2")]

    monkeypatch.setattr("fred_pipeline.spread_config.load_spread_defs", _with_new_entry)
    wh.build_gold()
    assert _checkpoint(wh, "T10Y2Y_DUP") is not None
    assert len(_spread_rows(wh, "T10Y2Y_DUP")) == 1
    wh.close()


def test_gold_full_bypasses_incrementality_and_resets_checkpoints(tmp_path):
    wh = LocalWarehouse(_config(), db_path=str(tmp_path / "f.db"))
    wh.merge_silver(
        [
            _obs_row("DGS10", "2024-01-02", 4.0, "2024-01-02T00:00:00Z"),
            _obs_row("DGS2", "2024-01-02", 3.8, "2024-01-02T00:00:00Z"),
        ]
    )
    wh.build_gold()
    assert _checkpoint(wh) is not None

    wh.build_gold(full=True)
    # --full clears this table's checkpoints; the row content is unchanged
    # since Silver didn't change, but nothing is left resumable from.
    assert _checkpoint(wh) is None
    assert len(_spread_rows(wh)) == 1

    # The very next (non-full) build re-seeds the checkpoint from scratch.
    wh.build_gold()
    assert _checkpoint(wh) is not None
    wh.close()

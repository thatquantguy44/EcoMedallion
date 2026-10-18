import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.writer.equity_views import compute_equity_total_return_index


def _rows(records, *, ingested_at, run_id):
    rows = []
    for day, close, dividend, split in records:
        for field, value in (
            ("close", close),
            ("divCash", dividend),
            ("splitFactor", split),
        ):
            rows.append(
                {
                    "source": "tiingo",
                    "series_id": f"AAPL:{field}",
                    "observation_date": day,
                    "realtime_start": "",
                    "realtime_end": "",
                    "value": value,
                    "raw_value": str(value),
                    "is_missing": False,
                    "row_hash": f"AAPL:{field}:{day}:{value}",
                    "revision_number": 1,
                    "ingested_at": ingested_at,
                    "run_id": run_id,
                }
            )
    return rows


def _gold_rows(warehouse):
    return warehouse.query(
        "SELECT * FROM gold_equity_total_return_index ORDER BY ticker, observation_date"
    )


def _full_equity_rows(warehouse):
    rows = [
        row
        for row in warehouse._read("silver_fred_observation")
        if row["source"] == "tiingo"
    ]
    return compute_equity_total_return_index(rows)


def test_equity_total_return_append_restatement_full_and_reseed(tmp_path):
    config = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "equity.db"))
    try:
        warehouse.merge_silver(
            _rows(
                [
                    ("2024-01-02", 100.0, 0.0, 1.0),
                    ("2024-01-03", 101.0, 0.0, 1.0),
                    ("2024-01-04", 102.0, 1.0, 1.0),
                ],
                ingested_at="2024-02-01T00:00:00+00:00",
                run_id="initial",
            )
        )
        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        checkpoint = warehouse._load_checkpoints("gold_equity_total_return_index")[
            "AAPL"
        ]
        assert checkpoint["frontier_date"] == "2024-01-04"

        before_noop = _gold_rows(warehouse)
        checkpoint_updated_at = warehouse.query(
            "SELECT updated_at FROM gold_incremental_checkpoint "
            "WHERE table_name = 'gold_equity_total_return_index' AND entity_key = 'AAPL'"
        )[0]["updated_at"]
        warehouse.build_gold()
        assert _gold_rows(warehouse) == before_noop
        assert (
            warehouse.query(
                "SELECT updated_at FROM gold_incremental_checkpoint "
                "WHERE table_name = 'gold_equity_total_return_index' AND entity_key = 'AAPL'"
            )[0]["updated_at"]
            == checkpoint_updated_at
        )

        warehouse.merge_silver(
            _rows(
                [("2024-01-05", 103.0, 0.0, 1.0)],
                ingested_at="2024-02-02T00:00:00+00:00",
                run_id="forward",
            )
        )
        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        assert len(_gold_rows(warehouse)) == 4

        warehouse.merge_silver(
            _rows(
                [("2024-01-03", 101.0, 2.0, 1.0)],
                ingested_at="2024-02-03T00:00:00+00:00",
                run_id="dividend-restatement",
            )
        )
        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        assert _gold_rows(warehouse)[1]["total_return_index"] == pytest.approx(103.0)

        warehouse.conn.execute(
            "UPDATE gold_incremental_checkpoint SET config_hash = 'stale' "
            "WHERE table_name = 'gold_equity_total_return_index' AND entity_key = 'AAPL'"
        )
        warehouse.conn.commit()
        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        assert (
            warehouse._load_checkpoints("gold_equity_total_return_index")["AAPL"][
                "config_hash"
            ]
            != "stale"
        )

        warehouse.build_gold(full=True)
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        assert warehouse._load_checkpoints("gold_equity_total_return_index") == {}

        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
        assert "AAPL" in warehouse._load_checkpoints("gold_equity_total_return_index")
    finally:
        warehouse.close()


def test_equity_total_return_checkpoint_and_rows_rollback_together(tmp_path):
    config = PipelineConfig(environment=Environment.DEV, fred_api_key="k")
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "equity.db"))
    try:
        warehouse.merge_silver(
            _rows(
                [
                    ("2024-01-02", 100.0, 0.0, 1.0),
                    ("2024-01-03", 101.0, 0.0, 1.0),
                ],
                ingested_at="2024-02-01T00:00:00+00:00",
                run_id="initial",
            )
        )
        warehouse.build_gold()
        before_rows = _gold_rows(warehouse)
        before_checkpoint = warehouse._load_checkpoints(
            "gold_equity_total_return_index"
        )

        warehouse.merge_silver(
            _rows(
                [("2024-01-04", 102.0, 0.0, 1.0)],
                ingested_at="2024-02-02T00:00:00+00:00",
                run_id="forward",
            )
        )
        write_checkpoints = warehouse._write_checkpoints_batch

        def fail_after_equity_checkpoint(table_name, entries):
            write_checkpoints(table_name, entries)
            if table_name == "gold_equity_total_return_index":
                raise RuntimeError("simulated checkpoint write interruption")

        warehouse._write_checkpoints_batch = fail_after_equity_checkpoint
        with pytest.raises(
            RuntimeError, match="simulated checkpoint write interruption"
        ):
            warehouse.build_gold()
        warehouse._write_checkpoints_batch = write_checkpoints

        assert _gold_rows(warehouse) == before_rows
        assert (
            warehouse._load_checkpoints("gold_equity_total_return_index")
            == before_checkpoint
        )

        warehouse.build_gold()
        assert _gold_rows(warehouse) == _full_equity_rows(warehouse)
    finally:
        warehouse.close()

"""Use the existing isolated database fixture for native Postgres audit proof."""

from fred_pipeline.config import PipelineConfig
from fred_pipeline.io.postgres_store import PostgresWarehouse
from fred_pipeline.performance import PerformanceCollector, span
from fred_pipeline.performance_store import PerformanceStore
from tests.test_postgres_warehouse import postgres_dsn as postgres_dsn


def test_postgres_performance_flush_and_final_snapshot_are_idempotent(
    tmp_path, postgres_dsn
):
    warehouse = PostgresWarehouse(PipelineConfig(fred_api_key=""), dsn=postgres_dsn)
    collector = PerformanceCollector(output_dir=tmp_path)
    store = PerformanceStore(warehouse)
    collector.attach_sink(store.write_spans)
    try:
        with collector.activate():
            with span("extract", kind="stage"):
                with span("extract.fetch", source="bea", series_id="test"):
                    pass
        collector.finish("success")
        store.save(collector)
        store.save(collector)
        assert (
            warehouse.query("SELECT count(*) AS n FROM audit.run_performance")[0]["n"]
            == 1
        )
        assert warehouse.query("SELECT count(*) AS n FROM audit.run_span")[0]["n"] == 2
        assert warehouse.query("SELECT DISTINCT status FROM audit.run_span") == [
            {"status": "succeeded"}
        ]
        assert store.baseline(collector)["count"] == 0
    finally:
        warehouse.close()

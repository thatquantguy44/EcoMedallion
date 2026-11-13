"""Every live ECB entry point takes ownership before source or mutation work."""

from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("action", ["poll", "ingest", "promotion", "initialize"])
def test_busy_writer_stops_every_entry_point_before_work(monkeypatch, action):
    from fred_pipeline import ecb_writer_lock

    class BusyLock:
        def __init__(self, connection):
            self.acquired = False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(ecb_writer_lock, "ECBWriterLock", BusyLock)
    warehouse = SimpleNamespace(_pg_conn=object(), conn=object(), _insert=lambda: None)
    with pytest.raises(ecb_writer_lock.ECBWriterBusy):
        if action == "promotion":
            from fred_pipeline.ecb_archive_promotion import ECBHistoricalPromoter

            ECBHistoricalPromoter(warehouse).promote(
                "unused", acknowledge_quarantines=0
            )
        elif action == "initialize":
            from fred_pipeline.io.postgres_store import PostgresWarehouse

            class Connection:
                def close(self):
                    pass

            monkeypatch.setattr(PostgresWarehouse, "_connect", lambda *_: Connection())
            from fred_pipeline.config import PipelineConfig

            PostgresWarehouse(PipelineConfig())
        else:
            from fred_pipeline.ecb_eligible_assets import ECBEligibleAssetsPipeline

            pipeline = ECBEligibleAssetsPipeline(warehouse, client=object())
            if action == "poll":
                pipeline.poll()
            else:
                pipeline.ingest_release("unused")

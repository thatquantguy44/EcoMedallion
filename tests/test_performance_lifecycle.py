"""Direct library lifecycle compatibility for complete performance summaries."""

from fred_pipeline.performance import PerformanceCollector


class Clock:
    value = 0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += int(seconds * 1_000_000_000)


def test_direct_pipeline_report_includes_gold_and_calendar_before_delivery(
    tmp_path,
    monkeypatch,
    observations_payload,
    fake_client_cls,
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.io.local_store import LocalWarehouse
    from fred_pipeline.pipeline import FredPipeline
    from fred_pipeline.manifest import SeriesSpec
    from fred_pipeline.governance import alerting

    clock = Clock()
    collector = PerformanceCollector(clock=clock, output_dir=tmp_path)
    warehouse = LocalWarehouse(
        PipelineConfig(fred_api_key=""), db_path=str(tmp_path / "run.db")
    )
    pipeline = FredPipeline(
        PipelineConfig(fred_api_key=""),
        warehouse=warehouse,
        client=fake_client_cls({"GDP": observations_payload}),
    )

    def calendar(tracker, **_kwargs):
        with tracker.stage("fomc_calendar"):
            clock.advance(2)

    monkeypatch.setattr(pipeline, "_check_fomc_calendar", calendar)
    monkeypatch.setattr(
        warehouse, "build_gold", lambda: (clock.advance(5), {"table": "ok"})[1]
    )
    monkeypatch.setattr(
        pipeline, "_refresh_release_calendar", lambda _: clock.advance(3)
    )
    sent = []

    def send(run, tracker, **kwargs):
        sent.append(kwargs["performance"])
        clock.advance(7)  # Sending must not inflate the frozen processing time.

    monkeypatch.setattr(alerting, "send_run_alert_result", send)
    try:
        with collector.activate():
            run = pipeline.run(
                [SeriesSpec(series_id="GDP", title="GDP", frequency="d")]
            )
        assert run.run_id == collector.run_id
        assert len(sent) == 1
        assert sent[0]["processing_seconds"] == 10
        assert collector.snapshot()["processing_seconds"] == 10
        assert warehouse.query("SELECT status FROM audit_run_performance") == [
            {"status": "success"}
        ]
    finally:
        warehouse.close()


def test_reused_pipeline_does_not_attach_a_previous_runs_report_when_disabled(
    tmp_path,
    monkeypatch,
    observations_payload,
    fake_client_cls,
):
    from fred_pipeline.config import PipelineConfig
    from fred_pipeline.pipeline import FredPipeline
    from fred_pipeline.manifest import SeriesSpec
    from fred_pipeline.governance import alerting

    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path))
    pipeline = FredPipeline(
        PipelineConfig(fred_api_key="", notify_on="never"),
        client=fake_client_cls({"A": observations_payload}),
        persist_audit=False,
    )
    sent = []
    monkeypatch.setattr(
        alerting,
        "send_run_alert_result",
        lambda *_a, **kw: sent.append(kw["performance"]),
    )
    spec = SeriesSpec(series_id="A", title="Example", frequency="d")
    pipeline.run([spec], build_gold_layer=False)
    monkeypatch.setenv("FRED_PERFORMANCE_ENABLED", "0")
    pipeline.run([spec], build_gold_layer=False)
    assert sent[0] is not None
    assert sent[1] is None

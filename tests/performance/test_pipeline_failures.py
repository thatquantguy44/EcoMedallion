"""Persist/report the result of real pipeline stages when a boundary fails."""

import json
from types import SimpleNamespace

import pytest

from fred_pipeline.config import PipelineConfig
from fred_pipeline.governance import alerting
from fred_pipeline.governance.stages import overall_verdict
from fred_pipeline.io.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline


@pytest.mark.parametrize(
    "failed_stage,expected_verdict",
    [
        ("plan", "failure"),
        ("gold", "failure"),
        ("release_calendar", "partial"),
        ("persist", "failure"),
    ],
)
def test_failure_verdict_agrees_in_alert_artifact_and_native_audit(
    tmp_path,
    monkeypatch,
    fake_client_cls,
    performance_fixture,
    failed_stage,
    expected_verdict,
):
    payload = performance_fixture("http_retry")["responses"][1]["body"]
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path / "reports"))
    config = PipelineConfig(fred_api_key="", notify_on="never")
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "pipeline.db"))
    pipeline = FredPipeline(
        config, warehouse=warehouse, client=fake_client_cls({"EXAMPLE": payload})
    )
    monkeypatch.setattr(pipeline, "_notify", lambda _: None)

    def check_calendar(tracker, **_kwargs):
        with tracker.stage("fomc_calendar"):
            pass

    monkeypatch.setattr(pipeline, "_check_fomc_calendar", check_calendar)
    monkeypatch.setattr(pipeline, "_refresh_release_calendar", lambda _: None)
    monkeypatch.setattr(warehouse, "build_gold", lambda: {"fixture": "ok"})
    failure = RuntimeError(f"fixture {failed_stage} failure")

    def fail(*_args, **_kwargs):
        raise failure

    target, method = {
        "plan": (pipeline, "_partition_by_due_date"),
        "gold": (warehouse, "build_gold"),
        "release_calendar": (pipeline, "_refresh_release_calendar"),
        "persist": (warehouse, "persist_run"),
    }[failed_stage]
    monkeypatch.setattr(target, method, fail)
    sent = []

    def capture(run, tracker, **kwargs):
        verdict, _ = overall_verdict(
            tracker.records(),
            series_total=run.series_total,
            series_failed=run.series_failed,
            required_stages=tracker.required_stages,
        )
        sent.append((verdict, kwargs["performance"]))
        return SimpleNamespace(
            status="previewed", category="", correlation_id="fixture"
        )

    monkeypatch.setattr(alerting, "send_run_alert_result", capture)
    try:
        spec = SeriesSpec(series_id="EXAMPLE", title="Example", frequency="d")
        if failed_stage == "plan":
            with pytest.raises(RuntimeError) as caught:
                pipeline.run([spec])
            assert caught.value is failure
            assert pipeline._stage_tracker.get("extract") is None
        else:
            run = pipeline.run([spec])
            assert run.series_succeeded == 1
            assert (
                warehouse.query("SELECT count(*) AS n FROM silver_fred_observation")[0][
                    "n"
                ]
                == 2
            )
        assert len(sent) == 1
        assert sent[0][0] == sent[0][1]["status"] == expected_verdict
        report_id = sent[0][1]["run_id"]
        saved = json.loads((tmp_path / "reports" / f"{report_id}.json").read_text())
        assert saved["status"] == expected_verdict
        assert (tmp_path / "reports" / f"{report_id}.html").is_file()
        audit = warehouse.query(
            "SELECT status, summary_json FROM audit_run_performance"
        )[0]
        assert audit["status"] == expected_verdict
        assert json.loads(audit["summary_json"])["status"] == expected_verdict
    finally:
        warehouse.close()


def test_performance_storage_outage_preserves_successful_ingestion_and_file_reports(
    tmp_path,
    monkeypatch,
    fake_client_cls,
    performance_fixture,
):
    import fred_pipeline.performance_store as storage

    def unavailable(_warehouse):
        raise RuntimeError("fixture telemetry database unavailable")

    monkeypatch.setattr(storage, "PerformanceStore", unavailable)
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path / "reports"))
    config = PipelineConfig(fred_api_key="", notify_on="never")
    warehouse = LocalWarehouse(config, db_path=str(tmp_path / "pipeline.db"))
    payload = performance_fixture("http_retry")["responses"][1]["body"]
    pipeline = FredPipeline(
        config, warehouse=warehouse, client=fake_client_cls({"EXAMPLE": payload})
    )
    monkeypatch.setattr(pipeline, "_notify", lambda _: None)
    sent = []
    monkeypatch.setattr(
        alerting,
        "send_run_alert_result",
        lambda *_a, **kw: sent.append(kw["performance"]),
    )
    try:
        run = pipeline.run(
            [SeriesSpec(series_id="EXAMPLE", title="Example", frequency="d")],
            build_gold_layer=False,
        )
        assert run.series_succeeded == 1
        assert sent[0]["status"] == "success"
        assert "storage" in sent[0]["telemetry_errors"]
        saved = json.loads((tmp_path / "reports" / f"{run.run_id}.json").read_text())
        assert saved["status"] == "success" and "storage" in saved["telemetry_errors"]
        assert (
            warehouse.query("SELECT count(*) AS n FROM silver_fred_observation")[0]["n"]
            == 2
        )
    finally:
        warehouse.close()

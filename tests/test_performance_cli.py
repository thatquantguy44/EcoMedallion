"""Exercise command → extraction → native audit → ECB → final alert/artifacts."""

import json
import sqlite3
from types import SimpleNamespace

import pytest

from fred_pipeline import cli
from fred_pipeline.config import PipelineConfig
from fred_pipeline.governance import alerting
from fred_pipeline.governance.email_report import build_summary
from fred_pipeline.governance.stages import overall_verdict


@pytest.mark.parametrize(
    ("ecb_fails", "audit_fails"), [(False, False), (True, False), (False, True)]
)
def test_real_command_persists_complete_report_after_ecb_and_never_sends_mail(
    tmp_path,
    monkeypatch,
    capsys,
    observations_payload,
    fake_client_cls,
    ecb_fails,
    audit_fails,
):
    import fred_pipeline.pipeline as pipeline_module

    config = PipelineConfig(fred_api_key="test-key", notify_on="never")
    if audit_fails:
        from fred_pipeline.io.local_store import LocalWarehouse

        original = LocalWarehouse.persist_run
        calls = 0

        def persist(warehouse, run):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("fixture final audit failure")
            return original(warehouse, run)

        monkeypatch.setattr(LocalWarehouse, "persist_run", persist)
    monkeypatch.setattr(cli.PipelineConfig, "resolve", staticmethod(lambda **_: config))
    monkeypatch.setattr(
        pipeline_module,
        "SOURCE_FACTORIES",
        {
            "fred": lambda _: fake_client_cls({"A": observations_payload}),
        },
    )
    manifest = tmp_path / "manifest.yml"
    manifest.write_text(
        "name: example\nversion: 1\nseries:\n  - series_id: A\n    title: Example\n    source: fred\n    frequency: d\n    active: true\n"
    )
    events = []

    def poll(*_args, **_kwargs):
        events.append("ecb")
        if ecb_fails:
            raise RuntimeError("fixture MID failure")
        return []

    monkeypatch.setattr(cli, "_poll_ecb_assets_for_run", poll)
    monkeypatch.setattr(
        cli,
        "_send_ecb_assets_health_alert",
        lambda *_a, **_k: events.append("ecb delivery"),
    )
    summaries = []

    def capture(run, tracker, **kwargs):
        events.append("summary")
        verdict, reason = overall_verdict(
            tracker.records(),
            series_failed=run.series_failed,
            series_total=run.series_total,
            required_stages=tracker.required_stages,
        )
        summary = build_summary(
            run,
            tracker.records(),
            verdict=verdict,
            reason=reason,
            performance=kwargs["performance"],
        )
        summaries.append(summary)
        return SimpleNamespace(
            status="previewed", category="", correlation_id="fixture"
        )

    monkeypatch.setattr(alerting, "send_run_alert_result", capture)
    database = tmp_path / "run.db"
    artifacts = tmp_path / "reports"
    args = cli.build_parser().parse_args(
        [
            "run",
            "--local",
            "--db-path",
            str(database),
            "--manifests",
            str(manifest),
            "--no-gold",
            "--performance-dir",
            str(artifacts),
        ]
    )
    assert cli._cmd_run(args) == (1 if ecb_fails else 0)
    assert events == ["ecb", "ecb delivery", "summary"]
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.verdict == ("failure" if ecb_fails or audit_fails else "success")
    assert summary.performance["processing_seconds"] > 0
    assert "ecb_assets" in {s["name"] for s in summary.performance["stages"]}
    assert (artifacts / f"{summary.run_id}.html").is_file()
    final = json.loads((artifacts / f"{summary.run_id}.json").read_text())
    assert final["status"] == summary.verdict
    assert final["metadata"]["ecb_assets_workload"] == {
        "mode": "poll",
        "releases": 0,
        "accepted": 0,
    }
    assert any(s["name"] == "report.delivery" for s in final["spans"])
    with sqlite3.connect(database) as connection:
        status, raw, delivery = connection.execute(
            "SELECT status, summary_json, delivery_json FROM audit_run_performance"
        ).fetchone()
        assert status == summary.verdict
        assert any(s["name"] == "report.delivery" for s in json.loads(raw)["spans"])
        assert json.loads(delivery)["status"] == "previewed"
        assert (
            connection.execute(
                "SELECT count(*) FROM audit_run_span WHERE name = 'extract.fetch'"
            ).fetchone()[0]
            == 1
        )
    capsys.readouterr()


def test_explicit_required_ecb_failure_overrides_successful_series():
    from fred_pipeline.governance.stages import RunStageTracker

    tracker = RunStageTracker()
    for name in ("plan", "extract", "gold", "persist"):
        with tracker.stage(name):
            pass
    with tracker.stage("ecb_assets", swallow=True):
        raise RuntimeError("fixture failure")
    tracker.required_stages.add("ecb_assets")
    assert (
        overall_verdict(
            tracker.records(), series_total=10, required_stages=tracker.required_stages
        )[0]
        == "failure"
    )

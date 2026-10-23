"""Default ``run`` orchestration for spec011 ECB eligible assets."""

from types import SimpleNamespace

from fred_pipeline import cli
from fred_pipeline.config import Environment, PipelineConfig


class _Warehouse:
    def __init__(self):
        self.closed = False
        self.conn = object()

    def _insert(self, *_args, **_kwargs):
        raise AssertionError("not used by the orchestration test")

    def close(self):
        self.closed = True


class _Run:
    status = SimpleNamespace(value="succeeded")

    def to_row(self):
        return {"run_id": "observation-run", "status": "succeeded"}


def _install_run_fakes(monkeypatch):
    config = PipelineConfig(environment=Environment.DEV, fred_api_key="")
    warehouse = _Warehouse()

    monkeypatch.setattr(
        cli.PipelineConfig, "resolve", staticmethod(lambda **_kwargs: config)
    )
    monkeypatch.setattr(cli, "load_manifests", lambda _path: [])
    monkeypatch.setattr(cli, "all_series", lambda _manifests, active_only: [])

    import fred_pipeline.io.warehouse_factory as factory_module
    import fred_pipeline.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "missing_source_keys", lambda *_args: {})

    class _Factory:
        def __init__(self, *_args, **_kwargs):
            pass

        def build(self, *, force_dry_run):
            assert not force_dry_run
            return warehouse

    class _ObservationPipeline:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_from_manifest(self, *_args, **_kwargs):
            return _Run()

    monkeypatch.setattr(factory_module, "WarehouseFactory", _Factory)
    monkeypatch.setattr(cli, "FredPipeline", _ObservationPipeline)
    return warehouse


def test_run_parser_enables_ecb_assets_by_default_and_allows_opt_out():
    parser = cli.build_parser()
    assert parser.parse_args(["run"]).ecb_assets is True
    assert parser.parse_args(["run", "--no-ecb-assets"]).ecb_assets is False


def test_run_polls_ecb_assets_by_default(monkeypatch, capsys):
    warehouse = _install_run_fakes(monkeypatch)
    calls = []
    alerts = []

    def poll(config, selected_warehouse):
        calls.append((config, selected_warehouse))
        return [{"release_id": "release-one", "status": "accepted"}]

    monkeypatch.setattr(cli, "_poll_ecb_assets_for_run", poll)
    monkeypatch.setattr(
        cli,
        "_send_ecb_assets_health_alert",
        lambda *args, **kwargs: alerts.append((args, kwargs)),
    )
    args = cli.build_parser().parse_args(["run"])

    assert cli._cmd_run(args) == 0
    assert len(calls) == 1
    assert len(alerts) == 1
    assert alerts[0][1]["action"] == "poll"
    assert alerts[0][1]["results"][0]["release_id"] == "release-one"
    assert calls[0][1] is warehouse
    assert warehouse.closed
    output = capsys.readouterr().out
    assert '"ecb_assets"' in output
    assert '"release_id": "release-one"' in output


def test_run_reports_ecb_poll_failure_and_closes_warehouse(monkeypatch, capsys):
    warehouse = _install_run_fakes(monkeypatch)
    alerts = []

    def fail(_config, _warehouse):
        raise RuntimeError("MID unavailable")

    monkeypatch.setattr(cli, "_poll_ecb_assets_for_run", fail)
    monkeypatch.setattr(
        cli,
        "_send_ecb_assets_health_alert",
        lambda *args, **kwargs: alerts.append((args, kwargs)),
    )
    args = cli.build_parser().parse_args(["run"])

    assert cli._cmd_run(args) == 1
    assert warehouse.closed
    assert len(alerts) == 1
    assert alerts[0][1]["error"] == "MID unavailable"
    output = capsys.readouterr().out
    assert '"status": "failed"' in output
    assert "MID unavailable" in output


def test_run_opt_out_does_not_poll(monkeypatch, capsys):
    warehouse = _install_run_fakes(monkeypatch)

    def unexpected(*_args):
        raise AssertionError("ECB poll should be disabled")

    monkeypatch.setattr(cli, "_poll_ecb_assets_for_run", unexpected)
    args = cli.build_parser().parse_args(["run", "--no-ecb-assets"])

    assert cli._cmd_run(args) == 0
    assert warehouse.closed
    output = capsys.readouterr().out
    assert "disabled by --no-ecb-assets" in output

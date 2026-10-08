"""WarehouseFactory: a failed backend must not silently become a no-op run.

Regression for a run that asked for Postgres, failed to initialize it, and
carried on in-memory: it extracted 2,895 series against live APIs and persisted
nothing, with only two WARNING lines to show for it.
"""

from __future__ import annotations

import pytest

from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.io.warehouse_factory import (
    WarehouseConfig,
    WarehouseFactory,
    WarehouseInitError,
)


def _factory(monkeypatch, primary, fallbacks=(), *, broken=(), working=None):
    """A factory whose backends are faked at the _build_backend boundary.

    `broken` backends raise on init; `working` (name -> object) succeed.
    """
    working = working or {}

    def fake_build_backend(self, name):
        if name in broken:
            raise RuntimeError(f"{name} is down")
        if name == "none":
            return None
        return working[name]

    monkeypatch.setattr(WarehouseFactory, "_build_backend", fake_build_backend)
    return WarehouseFactory(
        PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
        WarehouseConfig(primary_backend=primary, fallback_backends=list(fallbacks)),
    )


def test_all_backends_failing_raises_instead_of_going_in_memory(monkeypatch):
    factory = _factory(monkeypatch, "postgres", broken=("postgres",))

    with pytest.raises(WarehouseInitError) as exc:
        factory.build()

    msg = str(exc.value)
    assert "Refusing to start" in msg
    assert "postgres: postgres is down" in msg  # the real cause is surfaced
    assert "--dry-run" in msg  # and the way out is named


def test_error_lists_every_backend_that_was_tried(monkeypatch):
    factory = _factory(monkeypatch, "postgres", ["local"], broken=("postgres", "local"))

    with pytest.raises(WarehouseInitError) as exc:
        factory.build()

    assert "postgres -> local" in str(exc.value)
    assert "local: local is down" in str(exc.value)


def test_a_working_fallback_still_rescues_the_run(monkeypatch):
    sentinel = object()
    factory = _factory(
        monkeypatch,
        "postgres",
        ["local"],
        broken=("postgres",),
        working={"local": sentinel},
    )

    assert factory.build() is sentinel


def test_explicit_none_fallback_permits_in_memory(monkeypatch):
    """config/warehouse.yml documents `none` as the opt-in to in-memory."""
    factory = _factory(monkeypatch, "postgres", ["none"], broken=("postgres",))

    assert factory.build() is None


def test_primary_none_permits_in_memory(monkeypatch):
    assert _factory(monkeypatch, "none").build() is None


def test_force_dry_run_never_raises_and_never_touches_a_backend(monkeypatch):
    factory = _factory(monkeypatch, "postgres", broken=("postgres",))
    monkeypatch.setattr(
        WarehouseFactory,
        "_build_backend",
        lambda self, name: pytest.fail("dry-run must not initialize a backend"),
    )

    assert factory.build(force_dry_run=True) is None


def test_a_working_primary_is_returned_untouched(monkeypatch):
    sentinel = object()
    factory = _factory(monkeypatch, "local", working={"local": sentinel})

    assert factory.build() is sentinel


# ---- CLI: the failure must stop the run before any extraction --------------


@pytest.fixture
def failing_factory(monkeypatch):
    """Every backend fails; constructing the pipeline is a test failure."""
    monkeypatch.setenv("FRED_API_KEY", "not-a-real-key")

    def boom(self, force_dry_run=False):
        raise WarehouseInitError("Every configured warehouse backend failed")

    monkeypatch.setattr(WarehouseFactory, "build", boom)
    monkeypatch.setattr(
        "fred_pipeline.cli.FredPipeline",
        lambda *a, **k: pytest.fail("pipeline started without a warehouse"),
    )


def test_run_exits_2_before_touching_any_source(failing_factory, capsys):
    from fred_pipeline.cli import main

    code = main(["run", "--series", "DGS10"])

    assert code == 2
    err = capsys.readouterr().err
    assert err.startswith("ERROR: Every configured warehouse backend failed")


def test_replay_exits_2_instead_of_a_traceback(failing_factory, capsys):
    from fred_pipeline.cli import main

    assert main(["replay", "--series", "DGS10"]) == 2
    assert "ERROR:" in capsys.readouterr().err


def test_dry_run_still_works_with_every_backend_down(failing_factory, monkeypatch):
    """--dry-run is the documented way to run without persistence; the factory
    must not even be consulted for it."""
    from fred_pipeline import cli

    started = []

    class FakePipeline:
        def __init__(self, *a, **k):
            started.append(k.get("warehouse"))

        def run_from_manifest(self, *a, **k):
            raise SystemExit(0)  # proof we got past warehouse setup

    monkeypatch.setattr(cli, "FredPipeline", FakePipeline)

    with pytest.raises(SystemExit):
        cli.main(["run", "--series", "DGS10", "--dry-run"])

    assert started == [None]

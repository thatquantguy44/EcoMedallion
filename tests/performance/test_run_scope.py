"""Deployment metadata and outer run-scope behavior without provider calls."""

import json

import pytest

import fred_pipeline.performance as performance


@pytest.mark.parametrize("exit_code,expected", [(0, "success"), (2, "failure")])
def test_command_exit_status_is_saved_without_changing_its_return_value(
    tmp_path, monkeypatch, exit_code, expected
):
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path))

    @performance.performance_run
    def command():
        return exit_code

    assert command() == exit_code
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["status"] == expected


def test_zero_exit_does_not_overwrite_an_explicit_partial_stage_verdict(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path))

    @performance.performance_run
    def command():
        performance.current_performance().finish("partial")
        return 0

    assert command() == 0
    assert json.loads(next(tmp_path.glob("*.json")).read_text())["status"] == "partial"


def test_disabled_scope_writes_nothing_and_preserves_context(tmp_path, monkeypatch):
    monkeypatch.setenv("FRED_PERFORMANCE_ENABLED", "0")
    monkeypatch.setenv("FRED_PERFORMANCE_DIR", str(tmp_path))

    @performance.performance_run
    def command():
        assert performance.current_performance() is None
        return "fixture-result"

    assert command() == "fixture-result"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "override,expected", [("a" * 100, "a" * 80), (None, "unknown")]
)
def test_revision_override_or_missing_git_is_safe(monkeypatch, override, expected):
    performance.code_revision.cache_clear()
    if override is None:
        monkeypatch.delenv("FRED_CODE_REVISION", raising=False)
    else:
        monkeypatch.setenv("FRED_CODE_REVISION", override)

    def unavailable(*_args, **_kwargs):
        raise OSError("fixture git unavailable")

    monkeypatch.setattr(performance.subprocess, "check_output", unavailable)
    try:
        assert performance.code_revision() == expected
    finally:
        performance.code_revision.cache_clear()

"""Preserve source truth and stop bad stages before any repair writes."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

spec = importlib.util.spec_from_file_location(
    "source_repair", Path(__file__).parents[1] / "scripts/repair_marketatlas_fred.py"
)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)


def row():
    return {
        "source": "fred",
        "series_id": "AKUR",
        "observation_date": "2026-09-01",
        "realtime_start": "2026-10-20",
        "realtime_end": "9999-12-31",
        "value": 4.3,
        "is_missing": False,
    }


def test_validate_requires_true_availability_open_current_interval_and_identity():
    assert repair.validate([row()], "AKUR", "2024-01-01", "2026-10-31") == 1
    for changed in (
        {"realtime_start": ""},
        {"realtime_end": "2026-10-20"},
        {"source": "bls"},
        {"value": float("nan")},
        {"is_missing": True},
        {"observation_date": "2023-01-01"},
        {"realtime_start": "2026-11-01"},
    ):
        with pytest.raises(ValueError):
            repair.validate([{**row(), **changed}], "AKUR", "2024-01-01", "2026-10-31")
    with pytest.raises(ValueError):
        repair.validate([row(), row()], "AKUR", "2024-01-01", "2026-10-31")


def test_read_only_stage_quarantines_incomplete_series_and_validates_resume(tmp_path):
    manifest = tmp_path / "input.json"
    manifest.write_text(
        json.dumps({"series": [{"series_id": "AKUR"}, {"series_id": "BAD"}]})
    )
    payload = {
        "observations": [
            {
                "date": "2026-09-01",
                "realtime_start": "2026-10-20",
                "realtime_end": "9999-12-31",
                "value": "4.3",
            }
        ]
    }
    fake = SimpleNamespace(
        get_observations_all_vintages=lambda sid, **kw: (
            payload if sid == "AKUR" else {"observations": []}
        )
    )
    args = SimpleNamespace(
        output=str(tmp_path / "stage"),
        manifests=[str(manifest)],
        series=None,
        config="unused",
        start="2024-01-01",
        end="2026-10-31",
    )
    with (
        patch.object(
            repair.PipelineConfig,
            "resolve",
            return_value=SimpleNamespace(fred_api_key="test"),
        ),
        patch.object(repair, "FredClient", return_value=fake),
        patch.object(
            repair, "connect", side_effect=AssertionError("no writes during stage")
        ),
    ):
        repair.stage(args)
        result = json.loads((tmp_path / "stage/manifest.json").read_text())
        assert list(result["series"]) == ["AKUR"]
        assert result["failures"] == {"BAD": "ValueError"}
        args.series = "AKUR"
        repair.stage(args)
        assert (
            json.loads((tmp_path / "stage/manifest.json").read_text())["series"][
                "AKUR"
            ]["sha256"]
            == result["series"]["AKUR"]["sha256"]
        )


def test_apply_rejects_modified_capture_before_connecting_for_source_replacement(
    tmp_path,
):
    # A bad staged series identity must fail before any data mutation.
    (tmp_path / "manifest.json").write_text(
        json.dumps({"series": {}, "start": "2024-01-01", "end": "2026-10-31"})
    )
    with (
        patch.object(
            repair, "connect", side_effect=AssertionError("unexpected connection")
        ),
        pytest.raises(ValueError),
    ):
        repair.apply(SimpleNamespace(output=str(tmp_path), series="AKUR"))


def test_current_only_refresh_keeps_dates_unknown_and_cannot_enter_vintage_mode():
    source = {**row(), "realtime_start": "", "realtime_end": ""}
    assert (
        repair.validate([source], "AKUR", "2024-01-01", "2026-10-31", current_only=True)
        == 1
    )
    with pytest.raises(ValueError):
        repair.validate([source], "AKUR", "2024-01-01", "2026-10-31")
    with pytest.raises(ValueError):
        repair.validate([row()], "AKUR", "2024-01-01", "2026-10-31", current_only=True)

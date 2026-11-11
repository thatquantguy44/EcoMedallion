"""Saved performance cases and deterministic clocks for the extended tests."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from fred_pipeline.governance.email_report import RunSummary
from fred_pipeline.governance.stages import StageRecord, StageStatus

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "performance"


class ManualClock:
    def __init__(self):
        self.nanoseconds = 0

    def __call__(self):
        return self.nanoseconds

    def advance(self, seconds):
        self.nanoseconds += round(seconds * 1_000_000_000)


@pytest.fixture
def manual_clock():
    return ManualClock()


@pytest.fixture
def performance_fixture():
    def load(name):
        return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))

    return load


@pytest.fixture
def performance_summary(performance_fixture):
    def load(name):
        fixture = performance_fixture(name)
        stages = []
        for record in fixture["stages"]:
            converted = dict(record, status=StageStatus(record["status"]))
            for key in ("started_at", "ended_at"):
                if converted.get(key):
                    converted[key] = datetime.fromisoformat(converted[key])
            stages.append(StageRecord(**converted))
        performance = fixture["performance"]
        return RunSummary(
            run_id=fixture["run_id"],
            environment=fixture["environment"],
            verdict=fixture["verdict"],
            reason=fixture["reason"],
            stages=stages,
            series_total=fixture["series"]["total"],
            series_succeeded=fixture["series"]["succeeded"],
            series_failed=fixture["series"]["failed"],
            series_skipped_not_due=fixture["series"]["skipped_not_due"],
            duration_seconds=performance["processing_seconds"],
            started_at=datetime.fromisoformat(performance["started_at"]),
            ended_at=datetime.fromisoformat(performance["ended_at"]),
            performance=performance,
        )

    return load

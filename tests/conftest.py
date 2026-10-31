"""Shared fixtures and test doubles for the FRED pipeline test suite."""

from __future__ import annotations

import sys
import socket
from pathlib import Path
from typing import Any

import pytest


class NetworkAccessDenied(RuntimeError):
    """Raised when a required offline test tries to open a network socket."""


_SYSTEM_SOCKET = socket.socket


class _OfflineSocket(_SYSTEM_SOCKET):
    def connect(self, address: Any) -> None:
        raise NetworkAccessDenied(
            f"network access is disabled for required offline tests: {address!r}"
        )

    def connect_ex(self, address: Any) -> int:
        raise NetworkAccessDenied(
            f"network access is disabled for required offline tests: {address!r}"
        )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Classify legacy tests until they are migrated to explicit markers."""
    for item in items:
        filename = Path(str(item.path)).name
        if filename == "test_spark_integration.py":
            item.add_marker(pytest.mark.spark)
        elif filename == "test_postgres_warehouse.py":
            item.add_marker(pytest.mark.postgres)
        if not any(
            item.get_closest_marker(marker)
            for marker in ("unit", "postgres", "spark", "external")
        ):
            item.add_marker(pytest.mark.unit)


@pytest.fixture(autouse=True)
def block_network_for_offline_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
):
    """Keep required tests deterministic and free of provider/network access."""
    if any(
        request.node.get_closest_marker(marker)
        for marker in ("external", "postgres", "spark")
    ):
        return
    monkeypatch.setattr(socket, "socket", _OfflineSocket)


# Make the src/ package importable without an editable install.
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def observations_payload() -> dict[str, Any]:
    """A realistic FRED series/observations payload with a missing value."""
    return {
        "realtime_start": "2024-01-01",
        "realtime_end": "2024-01-01",
        "observation_start": "2024-01-01",
        "observation_end": "2024-01-05",
        "count": 4,
        "observations": [
            {
                "realtime_start": "2024-01-02",
                "realtime_end": "9999-12-31",
                "date": "2024-01-01",
                "value": "4.25",
            },
            {
                "realtime_start": "2024-01-03",
                "realtime_end": "9999-12-31",
                "date": "2024-01-02",
                "value": "4.30",
            },
            {
                "realtime_start": "2024-01-04",
                "realtime_end": "9999-12-31",
                "date": "2024-01-03",
                "value": ".",
            },  # missing
            {
                "realtime_start": "2024-01-05",
                "realtime_end": "9999-12-31",
                "date": "2024-01-04",
                "value": "4.40",
            },
        ],
    }


@pytest.fixture
def vintage_payload() -> dict[str, Any]:
    """A payload where one observation_date has two vintages (a revision)."""
    return {
        "count": 3,
        "observations": [
            {
                "realtime_start": "2024-02-01",
                "realtime_end": "2024-02-28",
                "date": "2024-01-01",
                "value": "100.0",
            },
            {
                "realtime_start": "2024-03-01",
                "realtime_end": "9999-12-31",
                "date": "2024-01-01",
                "value": "101.5",
            },  # revised up
            {
                "realtime_start": "2024-02-01",
                "realtime_end": "9999-12-31",
                "date": "2024-02-01",
                "value": "102.0",
            },
        ],
    }


class FakeResponse:
    def __init__(self, json_data: Any, status_code: int = 200, text: str = ""):
        self._json = json_data
        self.status_code = status_code
        self.text = text

    def json(self) -> Any:
        return self._json


class FakeSession:
    """A requests.Session stand-in that replays a queued list of responses."""

    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
        headers: dict[str, Any] | None = None,
    ):
        self.calls.append(
            {"url": url, "params": params, "timeout": timeout, "headers": headers}
        )
        if not self._responses:
            raise AssertionError("FakeSession ran out of queued responses")
        return self._responses.pop(0)


class FakeClient:
    """A FredClient stand-in returning a fixed payload (or raising) per series."""

    def __init__(
        self, payloads: dict[str, Any], errors: dict[str, Exception] | None = None
    ):
        self._payloads = payloads
        self._errors = errors or {}
        self.requested: list[str] = []

    def get_observations(self, series_id: str, **kwargs: Any) -> dict[str, Any]:
        self.requested.append(series_id)
        if series_id in self._errors:
            raise self._errors[series_id]
        return self._payloads[series_id]


@pytest.fixture
def fake_response_cls():
    return FakeResponse


@pytest.fixture
def fake_session_cls():
    return FakeSession


@pytest.fixture
def fake_client_cls():
    return FakeClient

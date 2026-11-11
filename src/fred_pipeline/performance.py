"""Bounded, dependency-free run measurements shared by decorators and stages.

Workers only append measurements under a lock. The orchestration thread calls
``flush`` to journal/persist them; no database connection crosses threads.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import json
import logging
import math
import os
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

log = logging.getLogger(__name__)
_collector = contextvars.ContextVar("performance_collector", default=None)
_parent = contextvars.ContextVar("performance_parent", default=None)
_ATTRIBUTES = frozenset(
    {
        "source",
        "series_id",
        "table",
        "backend",
        "attempt",
        "http_status",
        "rows",
        "rows_written",
        "bytes",
        "retries",
        "series_count",
        "reason",
        "artifact_kind",
        "release_id",
        "cache_hit",
    }
)


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_performance() -> PerformanceCollector | None:
    return _collector.get()


@dataclass
class Measurement:
    name: str
    span_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "succeeded"
    attributes: dict[str, Any] = field(default_factory=dict)

    def count(self, name: str, value: int | float) -> None:
        if name in _ATTRIBUTES and isinstance(value, (int, float)):
            self.attributes[name] = value

    def outcome(self, status: str) -> None:
        if status in {
            "succeeded",
            "failed",
            "cancelled",
            "skipped",
            "unchanged",
            "not_run",
            "warned",
        }:
            self.status = status


class PerformanceCollector:
    def __init__(
        self,
        *,
        run_id: str | None = None,
        clock: Callable[[], int] = time.perf_counter_ns,
        output_dir: str | Path | None = None,
        max_spans: int = 4096,
        detailed: bool | None = None,
    ) -> None:
        self.run_id = run_id or uuid.uuid4().hex
        self.clock = clock
        self.started_ns = clock()
        self.started_at = _utc()
        self.ended_at: str | None = None
        self.finished_ns: int | None = None
        self.status = "running"
        self.metadata: dict[str, Any] = {}
        self.max_spans = max(1, max_spans)
        self.detailed = (
            detailed
            if detailed is not None
            else os.getenv("FRED_PERFORMANCE_DETAIL") == "1"
        )
        self.output_dir = Path(
            output_dir or os.getenv("FRED_PERFORMANCE_DIR", "data/performance")
        )
        self._lock = threading.Lock()
        self._records: deque[dict[str, Any]] = deque(maxlen=self.max_spans)
        self._pending: deque[dict[str, Any]] = deque(maxlen=self.max_spans)
        self._stages: list[dict[str, Any]] = []
        self._slowest: list[dict[str, Any]] = []
        self._aggregates: dict[tuple[str, str], dict[str, Any]] = {}
        self._sink: Callable[[list[dict[str, Any]]], None] | None = None
        self.dropped_events = 0
        self.telemetry_errors: set[str] = set()
        self.baseline: dict[str, Any] = {"count": 0}

    @contextmanager
    def activate(self) -> Iterator[PerformanceCollector]:
        token = _collector.set(self)
        parent_token = _parent.set(None)
        try:
            yield self
        finally:
            _parent.reset(parent_token)
            _collector.reset(token)

    @contextmanager
    def span(
        self,
        name: str,
        *,
        detailed: bool = False,
        _started_ns: int | None = None,
        **attributes: Any,
    ) -> Iterator[Measurement]:
        measurement = Measurement(
            name=name,
            attributes={
                key: value
                for key, value in attributes.items()
                if key in _ATTRIBUTES and isinstance(value, (str, bool, int, float))
            },
        )
        started = self.clock() if _started_ns is None else _started_ns
        started_at = _utc()
        parent_id = _parent.get()
        token = _parent.set(measurement.span_id)
        error_type = ""
        try:
            yield measurement
        except BaseException as exc:
            measurement.status = (
                "cancelled"
                if isinstance(
                    exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)
                )
                else "failed"
            )
            error_type = type(exc).__name__
            raise
        finally:
            ended = self.clock()
            _parent.reset(token)
            record = {
                **asdict(measurement),
                "run_id": self.run_id,
                "parent_span_id": parent_id,
                "started_at": started_at,
                "ended_at": _utc(),
                "offset_seconds": max(0, started - self.started_ns) / 1e9,
                "duration_seconds": max(0, ended - started) / 1e9,
                "error_type": error_type,
            }
            try:
                self._record(record, retain=not detailed or self.detailed)
            except Exception:
                self.telemetry_errors.add("collection")
                log.warning("Performance collection failed (%s)", self.run_id)

    def _record(self, record: dict[str, Any], *, retain: bool) -> None:
        with self._lock:
            key = (record["name"], record["attributes"].get("source", ""))
            aggregate = self._aggregates.setdefault(
                key,
                {
                    "name": key[0],
                    "source": key[1],
                    "count": 0,
                    "worker_seconds": 0.0,
                    "failed": 0,
                    "counters": {},
                    "samples": deque(maxlen=2048),
                },
            )
            duration = record["duration_seconds"]
            aggregate["count"] += 1
            aggregate["worker_seconds"] += duration
            aggregate["failed"] += record["status"] in {"failed", "cancelled"}
            aggregate["samples"].append(duration)
            for name, value in record["attributes"].items():
                if name in {"rows", "rows_written", "bytes", "retries"} and isinstance(
                    value, (int, float)
                ):
                    aggregate["counters"][name] = (
                        aggregate["counters"].get(name, 0) + value
                    )
            self._slowest.append(record)
            self._slowest.sort(key=lambda r: r["duration_seconds"], reverse=True)
            del self._slowest[10:]
            if not retain:
                return
            if record["parent_span_id"] is None and len(self._stages) < 128:
                self._stages.append(record)
            self._records.append(record)
            if len(self._pending) == self.max_spans:
                self.dropped_events += 1
            self._pending.append(record)

    def attach_sink(self, sink: Callable[[list[dict[str, Any]]], None]) -> None:
        self._sink = sink

    def flush(self) -> None:
        """Best effort; caller owns the warehouse connection and flush cadence."""
        with self._lock:
            records = list(self._pending)
            self._pending.clear()
        if not records:
            return
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            with (self.output_dir / f"{self.run_id}.jsonl").open(
                "a", encoding="utf-8"
            ) as journal:
                for record in records:
                    journal.write(json.dumps(record, sort_keys=True) + "\n")
        except Exception:
            self.telemetry_errors.add("journal")
        if self._sink is not None:
            try:
                self._sink(records)
            except Exception:
                self.telemetry_errors.add("storage")
                log.warning("Performance database flush failed (%s)", self.run_id)

    def snapshot(self) -> dict[str, Any]:
        end = self.finished_ns if self.finished_ns is not None else self.clock()
        with self._lock:
            aggregates = []
            for value in self._aggregates.values():
                samples = sorted(value["samples"])

                def percent(p: float) -> float | None:
                    return (
                        samples[max(0, math.ceil(len(samples) * p) - 1)]
                        if samples
                        else None
                    )

                aggregates.append(
                    {key: item for key, item in value.items() if key != "samples"}
                    | {
                        "p50_seconds": percent(0.50),
                        "p95_seconds": percent(0.95),
                        "sample_count": len(samples),
                        "sampled": value["count"] > len(samples),
                    }
                )
            return {
                "schema_version": 1,
                "run_id": self.run_id,
                "status": self.status,
                "started_at": self.started_at,
                "ended_at": self.ended_at,
                "processing_seconds": max(0, end - self.started_ns) / 1e9,
                "metadata": dict(self.metadata),
                "stages": list(self._stages),
                "spans": list(self._records),
                "aggregates": aggregates,
                "slowest": list(self._slowest),
                "dropped_events": self.dropped_events,
                "telemetry_errors": sorted(self.telemetry_errors),
                "baseline": self.baseline,
            }

    def finish(self, status: str | None = None) -> dict[str, Any]:
        if self.finished_ns is None:
            self.finished_ns = self.clock()
            self.ended_at = _utc()
        if status is not None:
            self.status = status
        self.flush()
        report = self.snapshot()
        self.write_artifact("json", json.dumps(report, indent=2))
        return report

    def write_artifact(self, suffix: str, content: str) -> None:
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            target = self.output_dir / f"{self.run_id}.{suffix}"
            temporary = target.with_suffix(target.suffix + ".tmp")
            temporary.write_text(content, encoding="utf-8")
            temporary.replace(target)
        except Exception:
            self.telemetry_errors.add("artifact")


@contextmanager
def span(
    name: str, *, detailed: bool = False, **attributes: Any
) -> Iterator[Measurement]:
    collector = current_performance()
    if collector is None:
        yield Measurement(name=name)
    else:
        with collector.span(name, detailed=detailed, **attributes) as measurement:
            yield measurement


def submit_with_context(
    executor: Any,
    function: Callable[..., Any],
    *args: Any,
    queue_name: str | None = None,
    queue_attributes: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Any:
    context = contextvars.copy_context()  # Each submission needs its own context.
    collector = current_performance()
    submitted = collector.clock() if collector is not None else 0

    def invoke() -> Any:
        if collector is not None and queue_name:
            with collector.span(
                queue_name,
                _started_ns=submitted,
                detailed=True,
                **(queue_attributes or {}),
            ):
                pass
        return function(*args, **kwargs)

    return executor.submit(context.run, invoke)


def performance_run(function: Callable[..., Any]) -> Callable[..., Any]:
    """Own a collector only at the outermost CLI/library entry point."""

    @functools.wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if (
            current_performance() is not None
            or os.getenv("FRED_PERFORMANCE_ENABLED", "1") == "0"
        ):
            return function(*args, **kwargs)
        collector = PerformanceCollector()
        with collector.activate():
            try:
                result = function(*args, **kwargs)
            except BaseException:
                collector.finish("failed")
                raise
            else:
                collector.finish(
                    "failed" if isinstance(result, int) and result else None
                )
                return result

    return wrapped

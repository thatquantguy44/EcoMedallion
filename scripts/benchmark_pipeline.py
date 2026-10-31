#!/usr/bin/env python
"""Reproducible pipeline benchmark with stage, row, and workload metrics.

Fixture mode makes no network calls and seeds a fresh SQLite database for each
iteration. Live mode uses the configured real warehouse and source clients.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover - resource is unavailable on Windows
    resource = None  # type: ignore[assignment]

os.environ.setdefault("FRED_GOLD_STAGE_TIMING", "1")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("benchmark_pipeline")

DEFAULT_SERIES = ["GDPC1", "UNRATE", "CPIAUCSL", "DGS10", "SOFR", "BAMLH0A0HYM2"]
_GOLD_TIMING_RE = re.compile(r"^gold\.(.+) finished in ([0-9.]+)s$")
_GOLD_ROW_COUNTS_RE = re.compile(r"^gold\.row_counts silver=(\d+) latest=(\d+)$")


class GoldTimingCapture(logging.Handler):
    """Capture the local Gold builder's opt-in per-stage timing records."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.stage_seconds: dict[str, float] = {}
        self.row_counts: dict[str, int] = {}
        self._lock = Lock()

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        timing = _GOLD_TIMING_RE.match(message)
        counts = _GOLD_ROW_COUNTS_RE.match(message)
        with self._lock:
            if timing:
                self.stage_seconds[timing.group(1)] = float(timing.group(2))
            elif counts:
                self.row_counts = {
                    "silver_rows_read": int(counts.group(1)),
                    "latest_rows_read": int(counts.group(2)),
                }


class CountingClient:
    """Count logical source-client calls while delegating the client API."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.calls: dict[str, int] = {}
        self._lock = Lock()

    def __getattr__(self, name: str) -> Any:
        method = getattr(self.client, name)
        if name not in {
            "get_observations",
            "get_observations_all_vintages",
            "get_release_dates",
        } or not callable(method):
            return method

        def counted(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                self.calls[name] = self.calls.get(name, 0) + 1
            return method(*args, **kwargs)

        return counted

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            self.calls[method] = self.calls.get(method, 0) + 1
        return getattr(self.client, method)(*args, **kwargs)


class FixtureFredClient:
    """FRED-shaped fixture client with optional per-series restatements."""

    def __init__(self, fixture: dict[str, Any], workload: str) -> None:
        self.fixture = fixture
        self.workload = workload

    def get_observations(self, series_id: str, **kwargs: Any) -> dict[str, Any]:
        observations = [dict(row) for row in self.fixture["series"][series_id]]
        if self.workload == "restatement":
            updates = self.fixture.get("restatements", {}).get(series_id, [])
            by_date = {update["date"]: update for update in updates}
            observations = [
                {**row, **by_date[row["date"]]} if row["date"] in by_date else row
                for row in observations
            ]

        start = kwargs.get("observation_start")
        end = kwargs.get("observation_end")
        if start:
            observations = [row for row in observations if row["date"] >= start]
        if end:
            observations = [row for row in observations if row["date"] <= end]
        return {"observations": observations, "count": len(observations)}

    def get_release_dates(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return []


class TimedWarehouse:
    """Record Bronze/Silver writes while preserving the warehouse interface."""

    def __init__(self, warehouse: Any) -> None:
        self.warehouse = warehouse
        self.write_seconds = {"bronze_write": 0.0, "silver_merge": 0.0}
        self.silver_rows_changed: int | None = (
            0 if hasattr(warehouse.conn, "total_changes") else None
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self.warehouse, name)

    def write_bronze(self, rows: list[dict[str, Any]]) -> int:
        started = time.perf_counter()
        try:
            return self.warehouse.write_bronze(rows)
        finally:
            self.write_seconds["bronze_write"] += time.perf_counter() - started

    def merge_silver(self, rows: list[dict[str, Any]]) -> int:
        started = time.perf_counter()
        before = self._compact_silver_hashes()
        try:
            return self.warehouse.merge_silver(rows)
        finally:
            after = self._compact_silver_hashes()
            if (
                before is not None
                and after is not None
                and self.silver_rows_changed is not None
            ):
                self.silver_rows_changed += sum(
                    1 for key, row_hash in after.items() if before.get(key) != row_hash
                )
            self.write_seconds["silver_merge"] += time.perf_counter() - started

    def _compact_silver_hashes(self) -> dict[tuple[str, str, str, str], str] | None:
        """Return compact-Silver content for the benchmark's changed-row metric.

        Spec015 writes immutable history events alongside compact Silver. SQLite
        ``total_changes`` now counts both relations, while this benchmark has
        always reported changes to the compact current-state surface only.
        """
        if not hasattr(self.warehouse.conn, "total_changes"):
            return None
        rows = self.warehouse.query(
            "SELECT source, series_id, observation_date, realtime_start, row_hash "
            "FROM silver_fred_observation"
        )
        return {
            (
                row["source"],
                row["series_id"],
                row["observation_date"],
                row["realtime_start"],
            ): row["row_hash"]
            for row in rows
        }


def _enable_gold_stage_timing() -> None:
    os.environ["FRED_GOLD_STAGE_TIMING"] = "1"
    from fred_pipeline.io import local_store

    local_store._STAGE_TIMING_ENABLED = True


def _peak_rss_bytes() -> int | None:
    if resource is None:
        return None
    try:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (AttributeError, OSError):
        return None
    return int(peak if sys.platform == "darwin" else peak * 1024)


def _load_fixture(path: str | Path) -> dict[str, Any]:
    fixture = json.loads(Path(path).read_text(encoding="utf-8"))
    series = fixture.get("series")
    if not isinstance(series, dict) or not series:
        raise ValueError("Fixture must contain a non-empty 'series' mapping")
    for series_id, observations in series.items():
        if not isinstance(observations, list) or not observations:
            raise ValueError(f"Fixture series {series_id!r} needs observations")
        if any("date" not in row or "value" not in row for row in observations):
            raise ValueError(f"Fixture series {series_id!r} has an invalid observation")
    return fixture


def _fixture_specs(fixture: dict[str, Any], requested: list[str] | None) -> list[Any]:
    from fred_pipeline.manifest import all_series, load_manifests

    wanted = set(requested or fixture["series"])
    missing_fixture = wanted - set(fixture["series"])
    if missing_fixture:
        raise ValueError(f"Series absent from fixture: {sorted(missing_fixture)}")
    specs = {
        spec.series_id: spec
        for spec in all_series(load_manifests("manifests"), active_only=True)
    }
    missing_manifest = wanted - set(specs)
    if missing_manifest:
        raise ValueError(
            f"Series are not active in the manifests: {sorted(missing_manifest)}"
        )
    return [specs[series_id] for series_id in sorted(wanted)]


def _stage_metrics(pipeline: Any) -> dict[str, dict[str, Any]]:
    tracker = pipeline._stage_tracker
    if tracker is None:
        return {}
    return {
        record.name: {
            "status": record.status.value,
            "seconds": record.duration_seconds,
            "detail": dict(record.detail),
        }
        for record in tracker.records()
    }


def _row_counts(warehouse: Any, backend: str) -> dict[str, int]:
    if warehouse is None:
        return {}
    silver_table = (
        "silver.fred_observation"
        if backend == "postgres"
        else "silver_fred_observation"
    )
    latest_table = (
        "gold.fred_latest_observation"
        if backend == "postgres"
        else "gold_fred_latest_observation"
    )
    result = {}
    for label, table in (
        ("silver_rows_after", silver_table),
        ("latest_rows_after", latest_table),
    ):
        rows = warehouse.query(f"SELECT COUNT(*) AS n FROM {table}")
        result[label] = int(rows[0]["n"])
    return result


def _run_record(
    *,
    run: Any,
    pipeline: Any,
    warehouse: TimedWarehouse | None,
    source_clients: dict[str, CountingClient],
    selected_series: int,
    backend: str,
    started: float,
    gold_capture: GoldTimingCapture,
    database_path: Path | None = None,
) -> dict[str, Any]:
    elapsed = time.perf_counter() - started
    stages = _stage_metrics(pipeline)
    series_runs = run.series_runs
    row_metrics = {
        "observations_extracted": sum(s.observations_extracted for s in series_runs),
        "bronze_rows_written": sum(s.rows_written_bronze for s in series_runs),
        "silver_rows_merge_reported": sum(s.rows_merged_silver for s in series_runs),
        "silver_rows_changed": (
            warehouse.silver_rows_changed if warehouse is not None else None
        ),
        **gold_capture.row_counts,
    }
    if warehouse is not None:
        row_metrics.update(_row_counts(warehouse, backend))
    db_size = (
        database_path.stat().st_size
        if database_path and database_path.exists()
        else None
    )
    return {
        "run_id": run.run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "workload": "full"
        if any(s.load_type == "full" for s in series_runs)
        else "routine",
        "backend": backend,
        "status": run.status.value,
        "total_wall_seconds": round(elapsed, 6),
        "process_peak_rss_bytes": _peak_rss_bytes(),
        "selected_series": selected_series,
        "due_series": max(0, selected_series - run.series_skipped_not_due),
        "succeeded_series": run.series_succeeded,
        "failed_series": run.series_failed,
        "skipped_not_due": run.series_skipped_not_due,
        "rows": row_metrics,
        "source_calls": {
            source: dict(client.calls) for source, client in source_clients.items()
        },
        "warehouse_write_seconds": dict(warehouse.write_seconds) if warehouse else {},
        "stages": stages,
        "gold_table_seconds": dict(gold_capture.stage_seconds),
        "database_size_bytes": db_size,
    }


def _seed_fixture_database(
    config: Any,
    specs: list[Any],
    fixture: dict[str, Any],
    database_path: Path,
) -> None:
    from fred_pipeline.local_store import LocalWarehouse
    from fred_pipeline.pipeline import FredPipeline

    client = FixtureFredClient(fixture, "unchanged")
    normalizer = FredPipeline(config, client=client)
    warehouse = LocalWarehouse(config, db_path=str(database_path))
    try:
        for spec in specs:
            rows = normalizer._normalize(
                spec,
                {"observations": fixture["series"][spec.series_id]},
                run_id="benchmark-seed",
            )
            warehouse.merge_silver(rows)
        warehouse.build_gold()
    finally:
        warehouse.close()


def run_fixture_benchmark(
    fixture_path: str | Path,
    *,
    workload: str = "unchanged",
    iterations: int = 3,
    series: list[str] | None = None,
) -> dict[str, Any]:
    """Run repeatable unchanged/restatement/full workloads from a FRED fixture."""
    if workload not in {"unchanged", "restatement", "full"}:
        raise ValueError(f"Unsupported fixture workload: {workload}")
    if iterations < 1:
        raise ValueError("iterations must be >= 1")

    _enable_gold_stage_timing()
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.io.local_store import LocalWarehouse
    from fred_pipeline.pipeline import FredPipeline

    fixture = _load_fixture(fixture_path)
    specs = _fixture_specs(fixture, series)
    config = PipelineConfig.resolve(environment=Environment.DEV, fred_api_key="fixture")
    runs = []
    with tempfile.TemporaryDirectory(prefix="fred-benchmark-") as temp_dir:
        for iteration in range(1, iterations + 1):
            database_path = Path(temp_dir) / f"{workload}-{iteration}.db"
            _seed_fixture_database(config, specs, fixture, database_path)
            delegate = LocalWarehouse(config, db_path=str(database_path))
            warehouse = TimedWarehouse(delegate)
            fixture_client = CountingClient(FixtureFredClient(fixture, workload))
            pipeline = FredPipeline(
                config,
                clients={"fred": fixture_client},
                warehouse=warehouse,
                persist_audit=True,
            )
            capture = GoldTimingCapture()
            local_store_logger = logging.getLogger("fred_pipeline.io.local_store")
            previous_log_level = local_store_logger.level
            local_store_logger.setLevel(logging.INFO)
            local_store_logger.addHandler(capture)
            started = time.perf_counter()
            try:
                run = pipeline.run(
                    specs,
                    manifest_path=str(fixture_path),
                    triggered_by="benchmark-fixture",
                    force_full=workload == "full",
                    skip_not_due=False,
                )
                record = _run_record(
                    run=run,
                    pipeline=pipeline,
                    warehouse=warehouse,
                    source_clients={"fred": fixture_client},
                    selected_series=len(specs),
                    backend="local",
                    started=started,
                    gold_capture=capture,
                    database_path=database_path,
                )
                record["iteration"] = iteration
                record["workload"] = workload
                runs.append(record)
            finally:
                local_store_logger.removeHandler(capture)
                local_store_logger.setLevel(previous_log_level)
                delegate.close()
    return _summarize(runs, workload, "local", iterations)


def _build_live_warehouse(config: Any, backend: str, db_path: str) -> Any:
    from fred_pipeline.io.warehouse_factory import (
        WarehouseConfig,
        WarehouseFactory,
        load_warehouse_config,
    )

    if backend == "local":
        warehouse_config = WarehouseConfig(
            primary_backend="local",
            fallback_backends=[],
            backends={"local": {"db_path": db_path}},
        )
    else:
        loaded = load_warehouse_config(environment=config.environment.value)
        warehouse_config = WarehouseConfig(
            primary_backend="postgres",
            fallback_backends=[],
            backends=loaded.backends,
        )
    return WarehouseFactory(config, warehouse_config).build(force_dry_run=False)


def run_live_benchmark(
    *,
    series: list[str] | None,
    full_catalog: bool,
    db_path: str,
    backend: str,
    dry_run: bool,
    workload: str,
    skip_not_due: bool,
) -> dict[str, Any]:
    """Run against configured live sources and a real warehouse backend."""
    from fred_pipeline.config import Environment, PipelineConfig
    from fred_pipeline.manifest import all_series, load_manifests
    from fred_pipeline.pipeline import (
        FredPipeline,
        SOURCE_FACTORIES,
        missing_source_keys,
    )

    _enable_gold_stage_timing()
    config = PipelineConfig.resolve(environment=Environment.DEV)
    active = all_series(load_manifests("manifests"), active_only=True)
    selected = None if full_catalog else (series or DEFAULT_SERIES)
    if selected is not None:
        active_ids = {spec.series_id for spec in active}
        missing = set(selected) - active_ids
        if missing:
            raise ValueError(f"Requested series are not active: {sorted(missing)}")
    sources = sorted(
        {
            (spec.source or "fred").lower()
            for spec in active
            if selected is None or spec.series_id in set(selected)
        }
    )
    missing = missing_source_keys(config, sources)
    if missing:
        requirements = ", ".join(
            f"{source}: {setting}" for source, setting in sorted(missing.items())
        )
        raise ValueError(f"Missing source credentials/configuration: {requirements}")
    source_clients = {
        source: CountingClient(SOURCE_FACTORIES[source](config))
        for source in sources
        if source in SOURCE_FACTORIES
    }
    raw_warehouse = None if dry_run else _build_live_warehouse(config, backend, db_path)
    warehouse = TimedWarehouse(raw_warehouse) if raw_warehouse is not None else None
    pipeline = FredPipeline(
        config,
        clients=source_clients,
        warehouse=warehouse,
        persist_audit=not dry_run,
    )
    capture = GoldTimingCapture()
    local_store_logger = logging.getLogger("fred_pipeline.io.local_store")
    previous_log_level = local_store_logger.level
    local_store_logger.setLevel(logging.INFO)
    local_store_logger.addHandler(capture)
    started = time.perf_counter()
    try:
        run = pipeline.run_from_manifest(
            "manifests",
            triggered_by="benchmark-live",
            series=selected,
            build_gold_layer=not dry_run,
            force_full=workload == "full",
            skip_not_due=skip_not_due,
        )
        record = _run_record(
            run=run,
            pipeline=pipeline,
            warehouse=warehouse,
            source_clients=source_clients,
            selected_series=len(active) if selected is None else len(selected),
            backend="none" if dry_run else backend,
            started=started,
            gold_capture=capture,
            database_path=Path(db_path) if backend == "local" and not dry_run else None,
        )
        record["workload"] = "full" if workload == "full" else "routine"
        return record
    finally:
        local_store_logger.removeHandler(capture)
        local_store_logger.setLevel(previous_log_level)
        if raw_warehouse is not None:
            raw_warehouse.close()


def _summarize(
    runs: list[dict[str, Any]], workload: str, backend: str, iterations: int
) -> dict[str, Any]:
    elapsed = [run["total_wall_seconds"] for run in runs]
    return {
        "schema_version": 1,
        "summary": {
            "workload": workload,
            "backend": backend,
            "iterations_requested": iterations,
            "iterations_completed": len(runs),
            "median_total_wall_seconds": statistics.median(elapsed)
            if elapsed
            else None,
            "min_total_wall_seconds": min(elapsed) if elapsed else None,
            "max_total_wall_seconds": max(elapsed) if elapsed else None,
        },
        "runs": runs,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture", help="FRED-shaped JSON fixture; disables network calls"
    )
    parser.add_argument(
        "--workload",
        choices=("unchanged", "restatement", "full"),
        default="unchanged",
        help="Fixture workload; full forces full-history extraction",
    )
    parser.add_argument("--series", help="Comma-separated active series IDs")
    parser.add_argument(
        "--full-catalog",
        action="store_true",
        help="Select all active series (live only)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        help="Repeat count (fixture default: 3; live default: 1)",
    )
    parser.add_argument("--backend", choices=("local", "postgres"), default="local")
    parser.add_argument(
        "--db-path", default="/tmp/benchmark.db", help="SQLite benchmark database path"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Live extraction without warehouse writes",
    )
    parser.add_argument(
        "--no-skip-not-due", action="store_true", help="Attempt all selected series"
    )
    parser.add_argument("--output", help="Write detailed JSON report to this path")
    args = parser.parse_args(argv)

    selected = (
        list(
            dict.fromkeys(
                item.strip() for item in args.series.split(",") if item.strip()
            )
        )
        if args.series
        else None
    )
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be at least 1")
    iterations = (
        args.iterations if args.iterations is not None else (3 if args.fixture else 1)
    )
    if args.fixture:
        if args.full_catalog or args.dry_run or args.backend != "local":
            parser.error(
                "--fixture requires the local backend and cannot be combined with --full-catalog or --dry-run"
            )
        report = run_fixture_benchmark(
            args.fixture,
            workload=args.workload,
            iterations=iterations,
            series=selected,
        )
    else:
        if args.workload == "restatement":
            parser.error("the restatement workload requires --fixture")
        live_runs = [
            run_live_benchmark(
                series=selected,
                full_catalog=args.full_catalog,
                db_path=args.db_path,
                backend=args.backend,
                dry_run=args.dry_run,
                workload=args.workload,
                skip_not_due=not args.no_skip_not_due,
            )
            for _ in range(iterations)
        ]
        live_workload = "full" if args.workload == "full" else "routine"
        report = _summarize(
            live_runs,
            live_workload,
            "none" if args.dry_run else args.backend,
            iterations,
        )

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
        )
        log.info("Benchmark report written to %s", output_path)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))

    required_stages = {"plan", "extract", "gold", "persist"}
    failed = [
        run
        for run in report["runs"]
        if run["status"] != "succeeded"
        or any(
            run.get("stages", {}).get(name, {}).get("status") in {"failed", "not_run"}
            for name in required_stages
        )
    ]
    return 1 if not report["runs"] or failed else 0


if __name__ == "__main__":
    sys.exit(main())

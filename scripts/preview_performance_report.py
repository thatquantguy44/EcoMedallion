#!/usr/bin/env python3
"""Render a representative performance email using explicitly simulated data.

    python scripts/preview_performance_report.py --out data/performance/example.html

Does not connect to providers, a database or an email transport.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from fred_pipeline.governance.email_report import RunSummary, render_html
from fred_pipeline.governance.stages import StageRecord, StageStatus


def demonstration() -> RunSummary:
    stages = []
    offset = 0
    for name, seconds in (
        ("startup.config", 1),
        ("startup.warehouse", 3),
        ("startup.network", 2),
        ("plan", 4),
        ("extract", 200),
        ("fomc_calendar", 2),
        ("gold", 90),
        ("release_calendar", 10),
        ("persist", 3),
        ("ecb_assets", 25),
    ):
        stages.append(
            {
                "name": name,
                "status": "succeeded",
                "offset_seconds": offset,
                "duration_seconds": seconds,
            }
        )
        offset += seconds
    aggregates = []
    for source, count, rows, seconds, p95, retries in (
        ("fred", 500, 120000, 200, 1.8, 2),
        ("bea", 7, 17073, 85, 4.2, 1),
        ("bls", 60, 6000, 110, 2.1, 0),
        ("tiingo", 80, 90000, 160, 1.2, 4),
        ("ecb", 45, 30000, 150, 2.8, 1),
    ):
        aggregates.extend(
            [
                {
                    "name": "http.request",
                    "source": source,
                    "count": count,
                    "failed": retries,
                    "worker_seconds": seconds,
                    "p50_seconds": p95 / 3,
                    "p95_seconds": p95,
                    "sampled": False,
                    "counters": {},
                },
                {
                    "name": "extract.fetch",
                    "source": source,
                    "count": count,
                    "worker_seconds": seconds,
                    "first_offset_seconds": 10,
                    "last_end_seconds": 10 + min(200, seconds),
                    "counters": {},
                },
                {
                    "name": "extract.normalize",
                    "source": source,
                    "count": count,
                    "worker_seconds": seconds / 10,
                    "counters": {"rows": rows},
                },
                {
                    "name": "http.rate_limit",
                    "source": source,
                    "count": count,
                    "worker_seconds": seconds / 8,
                    "counters": {},
                },
                {
                    "name": "http.retry_wait",
                    "source": source,
                    "count": retries,
                    "worker_seconds": retries * 2,
                    "counters": {"retries": retries},
                },
            ]
        )
    performance = {
        "processing_seconds": offset,
        "stages": stages,
        "aggregates": aggregates,
        "dropped_events": 0,
        "telemetry_errors": [],
        "slowest": [
            {
                "name": name,
                "status": "succeeded",
                "duration_seconds": seconds,
                "attributes": {"table": table},
            }
            for name, table, seconds in (
                (
                    "gold.cross_series_feature_pit",
                    "gold_fred_cross_series_feature_pit",
                    40,
                ),
                ("warehouse.write", "gold_fred_macro_feature_daily", 30),
                (
                    "gold.equity_total_return_index_incremental",
                    "gold_equity_total_return_index",
                    20,
                ),
                ("ecb_assets.verify_signature", "full_xml", 12),
                ("extract.fetch", "NIUnderlyingDetail:U20404:2:M", 8),
            )
        ],
        "baseline": {
            "count": 12,
            "median_seconds": 350,
            "previous_seconds": [
                380,
                355,
                365,
                340,
                360,
                350,
                345,
                355,
                335,
                350,
                340,
                345,
            ],
        },
    }
    return RunSummary(
        run_id="demonstration",
        environment="preview",
        verdict="success",
        reason="DEMONSTRATION DATA — layout preview; all durations and counts are simulated.",
        stages=[
            StageRecord(
                name=s["name"],
                status=StageStatus.SUCCEEDED,
                duration_seconds=s["duration_seconds"],
            )
            for s in stages
        ],
        series_total=200,
        series_succeeded=180,
        series_skipped_not_due=20,
        duration_seconds=offset,
        started_at=datetime(2026, 11, 11, 12, tzinfo=timezone.utc),
        performance=performance,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path, default=Path("data/performance/example.html")
    )
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render_html(demonstration()), encoding="utf-8")
    print(args.out)


if __name__ == "__main__":
    main()

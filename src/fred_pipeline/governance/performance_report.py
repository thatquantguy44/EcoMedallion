"""Pure, email-compatible visual performance summaries; no clock or I/O."""

from __future__ import annotations

import html
from typing import Any


def duration(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, remainder = divmod(int(seconds), 60)
    return f"{minutes}m {remainder:02d}s"


def _baseline(report: dict[str, Any]) -> str:
    baseline = report.get("baseline", {})
    median = baseline.get("median_seconds")
    count = baseline.get("count", 0)
    if median is None or count < 3:
        return f"No comparable baseline ({count}/3 successful runs available)"
    change = (report["processing_seconds"] / median - 1) * 100 if median > 0 else None
    delta = f"{change:+.1f}%" if change is not None else "n/a"
    return f"{delta} vs median {duration(median)} of {count} comparable successful runs"


def _sources(report: dict[str, Any]) -> list[dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    for value in report.get("aggregates", []):
        name, source = value["name"], value.get("source")
        if not source:
            continue
        row = sources.setdefault(
            source,
            {
                "source": source,
                "requests": 0,
                "failed": 0,
                "rows": 0,
                "retry": 0,
                "wait": 0.0,
                "backoff": 0.0,
                "fetch": 0.0,
                "window": 0.0,
                "p50": None,
                "p95": None,
                "sampled": False,
            },
        )
        if name == "http.request":
            row.update(
                requests=value["count"],
                failed=value["failed"],
                p50=value["p50_seconds"],
                p95=value["p95_seconds"],
                sampled=value.get("sampled", False),
            )
        elif name == "extract.normalize":
            row["rows"] += value.get("counters", {}).get("rows", 0)
        elif name == "extract.fetch":
            row["fetch"] += value["worker_seconds"]
            row["window"] = value.get("last_end_seconds", 0) - value.get(
                "first_offset_seconds", 0
            )
        elif name == "http.rate_limit":
            row["wait"] += value["worker_seconds"]
        elif name == "http.retry_wait":
            row["backoff"] += value["worker_seconds"]
            row["retry"] += value.get("counters", {}).get("retries", 0)
    return sorted(
        sources.values(), key=lambda r: (r["window"], r["fetch"]), reverse=True
    )


def _warnings(report: dict[str, Any]) -> str:
    errors = report.get("telemetry_errors", [])
    dropped = report.get("dropped_events", 0)
    unfinished = len(report.get("unfinished_spans", []))
    if errors or dropped or unfinished:
        return f"Incomplete telemetry: {dropped} journal events dropped; {unfinished} unfinished spans; affected sinks: {', '.join(errors) or 'none'}. Aggregate counts include completed operations even when detailed samples are bounded."
    return ""


def _processing_stages(report: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        (s for s in report.get("stages", []) if not s["name"].startswith("report.")),
        key=lambda s: s["offset_seconds"],
    )


def _after_processing(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        value
        for value in report.get("aggregates", [])
        if value["name"].startswith("report.")
    ]


def render_performance_text(report: dict[str, Any] | None) -> str:
    if not report:
        return ""
    sources = _sources(report)
    lines = [
        "",
        "PERFORMANCE",
        "-----------",
        f"Elapsed processing: {duration(report['processing_seconds'])}",
        _baseline(report),
    ]
    warning = _warnings(report)
    if warning:
        lines.append(warning)
    lines += ["", "Timeline (offset / elapsed; concurrent work overlaps)"]
    for stage in _processing_stages(report):
        lines.append(
            f" {stage['name']}: {stage['status']} | +{duration(stage['offset_seconds'])} / {duration(stage['duration_seconds'])}"
        )
    lines += [
        "",
        "Source performance (request latency excludes throttle/backoff; waits are accumulated worker time)",
    ]
    for source in sources:
        lines.append(
            f" {source['source']}: {source['rows']} rows | window {duration(source['window'])} | {source['requests']} requests ({source['failed']} failed attempts) | p50 {duration(source['p50'])} / p95 {duration(source['p95'])}{' (bounded sample)' if source['sampled'] else ''} | retry {source['retry']} | throttle {duration(source['wait'])} | backoff {duration(source['backoff'])}"
        )
    lines += ["", "Slowest operations (nested durations are inclusive; do not sum)"]
    for record in report.get("slowest", [])[:10]:
        attributes = record.get("attributes", {})
        label = (
            attributes.get("series_id")
            or attributes.get("table")
            or attributes.get("source", "")
        )
        lines.append(
            f" {record['name']} {label}: {duration(record['duration_seconds'])} ({record['status']})"
        )
    if _after_processing(report):
        lines += [
            "",
            "After processing (excluded from total; nested durations are inclusive)",
        ]
        for value in _after_processing(report):
            lines.append(f" {value['name']}: {duration(value['worker_seconds'])}")
    return "\n".join(lines)


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(
        f"<th style='padding:7px;text-align:left;background:#F1F5F9'>{html.escape(h)}</th>"
        for h in headers
    )
    body = "".join(
        "<tr>"
        + "".join(
            f"<td style='padding:7px;border-bottom:1px solid #E2E8F0;vertical-align:top'>{cell}</td>"
            for cell in row
        )
        + "</tr>"
        for row in rows
    )
    return f"<table role='table' width='100%' style='width:100%;border-collapse:collapse;font-size:12px'><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _bar(offset: float, length: float, total: float, colour: str = "#1B7F79") -> str:
    if total <= 0 or length <= 0:
        return "<span style='color:#64748B'>—</span>"
    left = max(0, min(99, int(offset / total * 100)))
    width = max(1, min(100 - left, int(length / total * 100)))
    cells = []
    for percent, background in (
        (left, "#F1F5F9"),
        (width, colour),
        (100 - left - width, "#F1F5F9"),
    ):
        if percent:
            cells.append(
                f"<td width='{percent}%' height='12' style='width:{percent}%;height:12px;background:{background};font-size:1px;line-height:12px'>&nbsp;</td>"
            )
    return (
        "<table role='presentation' width='100%' cellspacing='0' cellpadding='0' style='width:100%;border-collapse:collapse'><tr>"
        + "".join(cells)
        + "</tr></table>"
    )


def render_performance_html(report: dict[str, Any] | None) -> str:
    if not report:
        return ""

    def esc(value: Any) -> str:
        return html.escape(str(value))

    total = report["processing_seconds"]
    sources = _sources(report)
    cards = _table(
        ["Elapsed processing", "Rows normalized", "Retries"],
        [
            [
                duration(total),
                str(sum(r["rows"] for r in sources)),
                str(sum(r["retry"] for r in sources)),
            ]
        ],
    )
    warning = _warnings(report)
    warning_html = (
        f"<p style='padding:10px;background:#FFF7ED;color:#9A3412'>{esc(warning)}</p>"
        if warning
        else ""
    )
    timeline_rows = []
    for stage in _processing_stages(report):
        colour = (
            "#B4441F"
            if stage["status"] in {"failed", "cancelled"}
            else "#D08C34"
            if stage["status"] == "warned"
            else "#1B7F79"
        )
        timeline_rows.append(
            [
                esc(stage["name"])
                + "<br><span style='color:#64748B'>"
                + esc(stage["status"])
                + "</span>",
                _bar(stage["offset_seconds"], stage["duration_seconds"], total, colour),
                duration(stage["duration_seconds"]),
            ]
        )
    source_rows = [
        [
            esc(r["source"]),
            str(r["rows"]),
            duration(r["window"]),
            f"{r['requests']} / {r['failed']} failed",
            f"{duration(r['p50'])} / {duration(r['p95'])}{'*' if r['sampled'] else ''}",
            f"{r['retry']} / {duration(r['wait'])} / {duration(r['backoff'])}",
        ]
        for r in sources
    ]
    slowest = report.get("slowest", [])[:10]
    maximum = max((r["duration_seconds"] for r in slowest), default=0)
    slow_rows = []
    for record in slowest:
        tags = record.get("attributes", {})
        label = tags.get("series_id") or tags.get("table") or tags.get("source", "")
        slow_rows.append(
            [
                esc(record["name"])
                + "<br><span style='color:#64748B'>"
                + esc(label)
                + " · "
                + esc(record["status"])
                + "</span>",
                _bar(0, record["duration_seconds"], maximum),
                duration(record["duration_seconds"]),
            ]
        )
    history = report.get("baseline", {}).get("previous_seconds", [])[-12:]
    history_html = ""
    if history:
        peak = max(history) or 1
        cells = "".join(
            f"<td style='vertical-align:bottom;padding:0 2px;text-align:center;font-size:10px;color:#64748B'><table role='presentation' width='100%' style='border-collapse:collapse'><tr><td height='{max(2, int(value / peak * 40))}' style='background:#78B3B0;height:{max(2, int(value / peak * 40))}px'>&nbsp;</td></tr></table>{duration(value)}</td>"
            for value in history
        )
        history_html = f"<table role='presentation' width='100%' style='width:100%;border-collapse:collapse'><tr>{cells}</tr></table><p style='font-size:11px;color:#64748B'>Oldest → newest; up to 12 preceding successful runs with the same configuration and workload band.</p>"
    after = _after_processing(report)
    after_html = (
        "<h3 style='font-size:14px'>After processing</h3>"
        + _table(
            ["Operation", "Elapsed (excluded from processing total)"],
            [
                [esc(value["name"]), duration(value["worker_seconds"])]
                for value in after
            ],
        )
        if after
        else ""
    )
    return f"""<div style='margin-top:22px;border-top:2px solid #CBD5E1;padding-top:14px'>
<h2 style='font-size:18px;margin:0 0 10px'>Performance</h2>{cards}
<p style='font-size:12px;color:#475569'>{esc(_baseline(report))}</p>{warning_html}
<h3 style='font-size:14px'>Timeline</h3>{_table(["Section / status", "Position in processing run", "Elapsed"], timeline_rows)}
<p style='font-size:11px;color:#64748B'>Durations include nested work. Concurrent operations overlap; child durations must not be added to calculate total elapsed time.</p>
<h3 style='font-size:14px'>Source performance</h3>{_table(["Source", "Rows", "Fetch window", "Requests", "p50 / p95", "Retries / throttle / backoff"], source_rows)}
<p style='font-size:11px;color:#64748B'>Request latency excludes throttle and backoff. Waits are accumulated worker time. Fetch window spans the first through last fetch. * Percentiles use a bounded sample.</p>
<h3 style='font-size:14px'>Slowest operations</h3>{_table(["Operation", "Relative duration", "Elapsed"], slow_rows)}
<h3 style='font-size:14px'>Recent comparable runs</h3>{history_html or '<p style="font-size:12px;color:#64748B">No comparable baseline history yet.</p>'}
{after_html}
<p style='font-size:11px;color:#64748B'>Processing time is frozen before report rendering and delivery. Their timings are recorded afterward in the durable performance report.</p>
</div>"""

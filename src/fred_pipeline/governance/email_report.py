"""Render a run's stage tracking into a summary email.

Pure: records in, strings out. No network, no I/O, no clock — so the whole
thing is unit-testable and a rendering change cannot break a pipeline run.

The design goal is that **the subject line alone is actionable**. Someone
glancing at a phone should learn the environment, the verdict and the reason
without opening anything:

    [FRED][prod] FAILURE — required stage(s) failed: gold (312/312 series ok)

The body then answers "which stage, and what did it say", because the case this
exists for is a run whose series all succeeded and whose Gold build did not.
Both an HTML and a plain-text part are produced; every mail client takes one.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

from fred_pipeline.governance.stages import StageRecord, StageStatus
from fred_pipeline.governance.performance_report import (
    render_performance_html,
    render_performance_text,
)

_VERDICT_LABEL = {
    "success": "SUCCESS",
    "partial": "PARTIAL",
    "failure": "FAILURE",
}
_VERDICT_EMOJI = {"success": "✅", "partial": "⚠️", "failure": "❌"}
# Colour-blind-safe: teal / amber / rust, never pure red-green, and always
# paired with the word and the glyph so colour is never the only channel.
_VERDICT_COLOUR = {
    "success": "#1B7F79",
    "partial": "#D08C34",
    "failure": "#B4441F",
}
_STATUS_GLYPH = {
    StageStatus.SUCCEEDED: "✓",
    StageStatus.FAILED: "✗",
    StageStatus.SKIPPED: "–",
    StageStatus.WARNED: "!",
    StageStatus.NOT_RUN: "?",
}


@dataclass(frozen=True)
class RunSummary:
    """Everything the renderer needs. Built by the caller from the run object,
    so the renderer never reaches into pipeline internals."""

    run_id: str
    environment: str
    verdict: str
    reason: str
    stages: Sequence[StageRecord]
    series_total: int = 0
    series_succeeded: int = 0
    series_failed: int = 0
    series_skipped_not_due: int = 0
    duration_seconds: Optional[float] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    triggered_by: str = ""
    manifest_path: str = ""
    failures: Sequence[tuple[str, str]] = ()  # (series_id, error)
    dq_failures: Sequence[tuple[str, str]] = ()  # (series_id, check)
    performance: Optional[dict[str, Any]] = None


def _fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def _fmt_ts(value: Optional[datetime]) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S UTC") if value else "n/a"


def render_subject(summary: RunSummary, prefix: str = "[FRED]") -> str:
    """One line that is worth acting on without opening the mail."""
    label = _VERDICT_LABEL.get(summary.verdict, summary.verdict.upper())
    env = f"[{summary.environment}]" if summary.environment else ""
    counts = (
        f" ({summary.series_succeeded}/{summary.series_total} series ok)"
        if summary.series_total
        else ""
    )
    return f"{prefix}{env} {label} — {summary.reason}{counts}".strip()


def render_text(summary: RunSummary, *, max_failures: int = 25) -> str:
    """Plain-text part. Also what gets logged when no transport is configured."""
    emoji = _VERDICT_EMOJI.get(summary.verdict, "")
    lines = [
        f"{emoji} {_VERDICT_LABEL.get(summary.verdict, summary.verdict.upper())}"
        f" — {summary.reason}",
        "",
        f"run       : {summary.run_id}",
        f"environment: {summary.environment or 'n/a'}",
        f"started   : {_fmt_ts(summary.started_at)}",
        f"finished  : {_fmt_ts(summary.ended_at)}",
        f"duration  : {_fmt_duration(summary.duration_seconds)}",
    ]
    if summary.triggered_by:
        lines.append(f"triggered : {summary.triggered_by}")
    if summary.manifest_path:
        lines.append(f"manifests : {summary.manifest_path}")
    lines += [
        "",
        "STAGES",
        "------",
    ]
    for record in summary.stages:
        glyph = _STATUS_GLYPH.get(record.status, "?")
        lines.append(
            f" {glyph} {record.name:<18} {record.status.value:<10}"
            f" {_fmt_duration(record.duration_seconds)}"
        )
        for key, value in sorted(record.detail.items()):
            lines.append(f"      {key}: {value}")
        if record.error_message:
            lines.append(f"      ERROR {record.error_type}: {record.error_message}")

    if summary.series_total:
        lines += [
            "",
            "SERIES",
            "------",
            f" total {summary.series_total} | ok {summary.series_succeeded} "
            f"| failed {summary.series_failed} | skipped {summary.series_skipped_not_due}",
        ]
    if summary.failures:
        lines.append("")
        lines.append(
            f"FAILED SERIES (showing {min(len(summary.failures), max_failures)}"
            f" of {len(summary.failures)})"
        )
        lines.append("-" * 20)
        for series_id, error in list(summary.failures)[:max_failures]:
            lines.append(f" • {series_id}: {error or 'unknown error'}")
        if len(summary.failures) > max_failures:
            lines.append(f" … and {len(summary.failures) - max_failures} more")
    if summary.dq_failures:
        lines.append("")
        lines.append("DATA-QUALITY FAILURES")
        lines.append("-" * 20)
        for series_id, check in list(summary.dq_failures)[:max_failures]:
            lines.append(f" • {series_id}: {check}")
    return "\n".join(lines) + render_performance_text(summary.performance)


def _row_html(record: StageRecord) -> str:
    colour = {
        StageStatus.SUCCEEDED: "#1B7F79",
        StageStatus.FAILED: "#B4441F",
        StageStatus.WARNED: "#D08C34",
        StageStatus.SKIPPED: "#6B7280",
        StageStatus.NOT_RUN: "#B4441F",
    }.get(record.status, "#6B7280")
    detail_bits = [
        f"{html.escape(str(k))}: {html.escape(str(v))}"
        for k, v in sorted(record.detail.items())
    ]
    detail = "<br>".join(detail_bits)
    if record.error_message:
        detail += (
            f"<br><span style='color:{colour}'><strong>"
            f"{html.escape(record.error_type)}</strong>: "
            f"{html.escape(record.error_message)}</span>"
        )
    glyph = _STATUS_GLYPH.get(record.status, "?")
    return (
        "<tr>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #E5E7EB'>"
        f"<span style='color:{colour};font-weight:bold'>{glyph}</span> "
        f"{html.escape(record.name)}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #E5E7EB;"
        f"color:{colour};font-weight:600'>{html.escape(record.status.value)}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #E5E7EB;"
        f"text-align:right'>{_fmt_duration(record.duration_seconds)}</td>"
        f"<td style='padding:6px 10px;border-bottom:1px solid #E5E7EB;"
        f"font-size:12px;color:#374151'>{detail}</td>"
        "</tr>"
    )


def render_html(summary: RunSummary, *, max_failures: int = 25) -> str:
    """HTML part. Table-based and inline-styled on purpose: Outlook's rendering
    engine ignores most modern CSS, and a summary that arrives unreadable is a
    summary nobody reads."""
    colour = _VERDICT_COLOUR.get(summary.verdict, "#6B7280")
    label = _VERDICT_LABEL.get(summary.verdict, summary.verdict.upper())
    emoji = _VERDICT_EMOJI.get(summary.verdict, "")

    rows = "".join(_row_html(r) for r in summary.stages)

    failures_html = ""
    if summary.failures:
        shown = list(summary.failures)[:max_failures]
        items = "".join(
            f"<li><code>{html.escape(sid)}</code>: "
            f"{html.escape(err or 'unknown error')}</li>"
            for sid, err in shown
        )
        more = (
            f"<p style='color:#6B7280;font-size:12px'>… and "
            f"{len(summary.failures) - max_failures} more</p>"
            if len(summary.failures) > max_failures
            else ""
        )
        failures_html = (
            f"<h3 style='font-family:Segoe UI,Arial,sans-serif;font-size:14px'>"
            f"Failed series ({len(summary.failures)})</h3>"
            f"<ul style='font-family:Segoe UI,Arial,sans-serif;font-size:13px'>"
            f"{items}</ul>{more}"
        )

    meta_rows = "".join(
        f"<tr><td style='padding:2px 10px 2px 0;color:#6B7280'>{html.escape(k)}</td>"
        f"<td style='padding:2px 0'><code>{html.escape(v)}</code></td></tr>"
        for k, v in (
            ("run", summary.run_id),
            ("environment", summary.environment or "n/a"),
            ("started", _fmt_ts(summary.started_at)),
            ("duration", _fmt_duration(summary.duration_seconds)),
            ("triggered by", summary.triggered_by or "n/a"),
        )
    )

    series_line = (
        f"<p style='font-family:Segoe UI,Arial,sans-serif;font-size:13px'>"
        f"<strong>{summary.series_succeeded}</strong> of "
        f"<strong>{summary.series_total}</strong> series succeeded, "
        f"<strong>{summary.series_failed}</strong> failed, "
        f"<strong>{summary.series_skipped_not_due}</strong> skipped as not due.</p>"
        if summary.series_total
        else ""
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Pipeline performance report</title></head><body style="margin:0;padding:16px;background:#F9FAFB">
<div style="max-width:760px;margin:0 auto;background:#FFFFFF;padding:20px;
            border:1px solid #E5E7EB;font-family:Segoe UI,Arial,sans-serif">
  <div style="border-left:5px solid {colour};padding:8px 14px;margin-bottom:16px">
    <h1 style="font-size:20px;font-weight:700;color:{colour};margin:0">
      {emoji} {label}
    </h1>
    <div style="font-size:14px;color:#374151">{html.escape(summary.reason)}</div>
  </div>

  <table style="font-size:12px;border-collapse:collapse;margin-bottom:16px">
    {meta_rows}
  </table>

  <h2 style="font-size:14px;margin:0 0 6px">Stages</h2>
  <table style="width:100%;border-collapse:collapse;font-size:13px">
    <thead>
      <tr style="background:#F3F4F6;text-align:left">
        <th style="padding:6px 10px">Stage</th>
        <th style="padding:6px 10px">Status</th>
        <th style="padding:6px 10px;text-align:right">Duration</th>
        <th style="padding:6px 10px">Detail</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>

  {series_line}
  {failures_html}
  {render_performance_html(summary.performance)}

  <p style="color:#6B7280;font-size:11px;margin-top:20px;
            border-top:1px solid #E5E7EB;padding-top:10px">
    Sent by the fred-bronze-to-gold pipeline. Recipients are configured in
    <code>config/alerting.yml</code>.
  </p>
</div>
</body></html>"""


def build_summary(
    run: Any,
    stages: Iterable[StageRecord],
    *,
    verdict: str,
    reason: str,
    environment: str = "",
    max_failures: int = 25,
    performance: Optional[dict[str, Any]] = None,
) -> RunSummary:
    """Adapt an :class:`~fred_pipeline.audit.EtlRun` into a :class:`RunSummary`.

    Kept separate from the renderers so they stay free of pipeline types and can
    be tested with hand-built records.
    """
    failures: list[tuple[str, str]] = []
    dq_failures: list[tuple[str, str]] = []
    for series_run in getattr(run, "series_runs", []) or []:
        status = getattr(series_run, "status", None)
        if getattr(status, "value", status) == "failed":
            failures.append(
                (series_run.series_id, getattr(series_run, "error_message", ""))
            )
        if getattr(series_run, "dq_passed", None) is False:
            dq_failures.append((series_run.series_id, "data-quality check failed"))

    return RunSummary(
        run_id=getattr(run, "run_id", ""),
        environment=environment or getattr(run, "environment", ""),
        verdict=verdict,
        reason=reason,
        stages=list(stages),
        series_total=getattr(run, "series_total", 0) or 0,
        series_succeeded=getattr(run, "series_succeeded", 0) or 0,
        series_failed=getattr(run, "series_failed", 0) or 0,
        series_skipped_not_due=getattr(run, "series_skipped_not_due", 0) or 0,
        duration_seconds=performance["processing_seconds"]
        if performance
        else getattr(run, "duration_seconds", None),
        started_at=datetime.fromisoformat(performance["started_at"])
        if performance and performance.get("started_at")
        else getattr(run, "started_at", None),
        ended_at=datetime.fromisoformat(performance["ended_at"])
        if performance and performance.get("ended_at")
        else getattr(run, "ended_at", None),
        triggered_by=getattr(run, "triggered_by", ""),
        manifest_path=getattr(run, "manifest_path", ""),
        failures=failures[: max_failures * 2],
        dq_failures=dq_failures[: max_failures * 2],
        performance=performance,
    )

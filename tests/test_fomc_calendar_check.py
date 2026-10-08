"""The pre-Gold FOMC calendar check and its opt-in refresh.

Why this exists: an expired, missing or unreadable config/fomc.yml makes the
FOMC Gold tables come out EMPTY without any error, and the run still reports
success. These tests pin that the state is recorded instead, that the opt-in
network refresh can never stop a run, and that it can never lose a date.

Every test injects "today" and the fetch function. Nothing here reads the wall
clock or touches the network, so none of it changes meaning as the real
calendar approaches its end (2028-01-26).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from fred_pipeline.catalogs.fomc_calendar import FOMCScrapeError
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.gold_config.fomc_config import load_fomc_config
from fred_pipeline.governance.fomc_calendar_check import (
    FAIL,
    OK,
    WARN,
    check_fomc_calendar,
)
from fred_pipeline.governance.stages import StageStatus, overall_verdict
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.manifest import SeriesSpec, ValidationProfile

REPO = Path(__file__).resolve().parents[1]
REAL_CONFIG = REPO / "config" / "fomc.yml"
FED_PAGE = (REPO / "tests" / "fixtures" / "fomccalendars.html").read_text(
    encoding="utf-8"
)

# The captured Fed page and config/fomc.yml agree through 2028-01-26. Tests
# pretend it is this day so the dates they remove are still in the future.
TODAY = date(2026, 10, 7)
LAST_MEETING = date(2028, 1, 26)


def _days_before_last(n: int) -> date:
    return date.fromordinal(LAST_MEETING.toordinal() - n)


@pytest.fixture
def cfg_path(tmp_path) -> Path:
    """A private copy of the real config, so a test can edit or break it."""
    p = tmp_path / "fomc.yml"
    p.write_text(REAL_CONFIG.read_text(encoding="utf-8"), encoding="utf-8")
    return p


def _drop_dates(path: Path, *dates: str) -> None:
    """Remove meeting_dates lines, leaving every comment and other key alone."""
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    keep = [ln for ln in lines if not any(f'"{d}"' in ln for d in dates)]
    assert len(keep) == len(lines) - len(dates)
    path.write_text("".join(keep), encoding="utf-8")


class Fetch:
    """A fake for fetch_calendar_html that records how it was called."""

    def __init__(self, html: str | None = None, error: Exception | None = None):
        self.html, self.error, self.calls = html, error, []

    def __call__(self, **kwargs) -> str:
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.html


# ---- the always-on offline check --------------------------------------------


def test_healthy_calendar_is_ok_and_never_touches_the_network(cfg_path):
    fetch = Fetch(error=AssertionError("the offline check must not fetch"))

    result = check_fomc_calendar(str(cfg_path), today=TODAY, fetch=fetch)

    assert result.status == OK
    assert result.messages == []
    assert result.runway_days == (LAST_MEETING - TODAY).days
    assert fetch.calls == []


@pytest.mark.parametrize(
    "runway,status,level",
    [
        (400, OK, "ok"),
        (271, OK, "ok"),
        # "due" is the weekly job's nudge, not the run's: warning here would
        # make every run for months a "partial" one.
        (270, OK, "due"),
        (121, OK, "due"),
        (120, WARN, "priority"),
        (46, WARN, "priority"),
        (45, WARN, "urgent"),
        (1, WARN, "urgent"),
        (0, FAIL, "expired"),
        (-30, FAIL, "expired"),
    ],
)
def test_severity_at_every_runway_boundary(cfg_path, runway, status, level):
    result = check_fomc_calendar(str(cfg_path), today=_days_before_last(runway))
    assert (result.status, result.level) == (status, level)
    assert result.runway_days == runway


def test_expired_calendar_says_the_gold_tables_will_be_empty(cfg_path):
    result = check_fomc_calendar(str(cfg_path), today=date(2028, 6, 1))

    assert result.status == FAIL
    assert "EMPTY" in result.messages[0]


def test_missing_file_names_the_path_and_the_working_directory_trap(
    tmp_path, monkeypatch
):
    """The default path is relative to the cwd, so running from anywhere but
    the repo root silently yields empty tables. The message has to say so."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FRED_FOMC_CONFIG_FILE", raising=False)

    result = check_fomc_calendar(today=TODAY)

    assert result.status == FAIL
    msg = result.messages[0]
    assert "config/fomc.yml" in msg
    assert str(tmp_path.resolve()) in msg or str(tmp_path) in msg  # the cwd
    assert "relative to the working directory" in msg


def test_the_env_var_override_is_honoured_and_reported(cfg_path, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FRED_FOMC_CONFIG_FILE", str(cfg_path))

    result = check_fomc_calendar(today=TODAY)

    assert result.status == OK
    assert result.path == str(cfg_path)


def test_malformed_config_fails_instead_of_raising(cfg_path):
    cfg_path.write_text("meeting_dates: not-a-list\nbogus_key: 1\n")

    result = check_fomc_calendar(str(cfg_path), today=TODAY)

    assert result.status == FAIL
    assert "malformed" in result.messages[0]


# ---- the opt-in refresh: it adds dates --------------------------------------


def test_refresh_adds_newly_published_meetings_and_keeps_everything_else(cfg_path):
    _drop_dates(cfg_path, "2027-12-08", "2028-01-26")
    before = load_fomc_config(str(cfg_path))
    assert max(before.meeting_dates) == date(2027, 10, 27)
    fetch = Fetch(html=FED_PAGE)

    result = check_fomc_calendar(str(cfg_path), refresh=True, today=TODAY, fetch=fetch)

    assert result.refreshed
    assert result.added_dates == ("2027-12-08", "2028-01-26")
    after = load_fomc_config(str(cfg_path))
    assert max(after.meeting_dates) == LAST_MEETING
    assert set(before.meeting_dates) <= set(after.meeting_dates)
    assert after.calendar_provenance.last_verified == TODAY
    assert after.calendar_provenance.verified_by == "scraper"
    # Hand-written commentary survives: the editor inserts, it does not rewrite.
    text = cfg_path.read_text(encoding="utf-8")
    assert "# FOMC rate probabilities" in text
    assert "bucket_step_bps: 25" in text


def test_refresh_fetches_with_the_plain_http_backend_and_a_short_timeout(cfg_path):
    """Selenium is break-glass. Launching a browser from inside a pipeline run,
    or waiting 30s+ on a hung connection, would be the wrong trade."""
    fetch = Fetch(html=FED_PAGE)

    check_fomc_calendar(str(cfg_path), refresh=True, today=TODAY, fetch=fetch)

    assert fetch.calls == [{"backend": "requests", "timeout_seconds": 15}]


def test_refresh_with_nothing_new_does_not_rewrite_the_file(cfg_path):
    before = cfg_path.read_bytes()

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
    )

    assert result.status == OK
    assert not result.refreshed
    assert cfg_path.read_bytes() == before


# ---- the opt-in refresh: and the fallback when anything goes wrong ----------


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("federalreserve.gov unreachable"),
        TimeoutError("read timed out"),
        FOMCScrapeError("HTTP GET returned 503"),
        RuntimeError("anything at all"),
    ],
    ids=["unreachable", "timeout", "bad-status", "unexpected"],
)
def test_a_failed_fetch_falls_back_to_the_file_on_disk(cfg_path, error):
    before = cfg_path.read_bytes()

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(error=error)
    )

    assert cfg_path.read_bytes() == before  # byte-identical
    assert result.status == WARN  # surfaced, but the calendar is still usable
    assert not result.refreshed
    assert "continuing with the calendar already on disk" in result.messages[0]
    assert type(error).__name__ in result.messages[0]
    assert result.runway_days == (LAST_MEETING - TODAY).days  # still assessed


def test_a_redesigned_page_that_parses_to_nothing_falls_back(cfg_path):
    before = cfg_path.read_bytes()

    result = check_fomc_calendar(
        str(cfg_path),
        refresh=True,
        today=TODAY,
        fetch=Fetch(html="<html><body>We moved everything.</body></html>"),
    )

    assert cfg_path.read_bytes() == before
    assert result.status == WARN


def test_a_failed_refresh_cannot_mask_an_expired_calendar(cfg_path):
    """Both problems reported; the worse one decides the status."""
    result = check_fomc_calendar(
        str(cfg_path),
        refresh=True,
        today=date(2028, 6, 1),
        fetch=Fetch(error=ConnectionError("down")),
    )

    assert result.status == FAIL
    assert any("refresh failed" in m for m in result.messages)
    assert any("run out" in m for m in result.messages)


def test_refresh_never_removes_a_date_the_fed_page_lacks(cfg_path):
    """A configured date missing upstream usually means a meeting MOVED. That is
    a human's call, so the date stays and the run is told."""
    cfg_path.write_text(
        cfg_path.read_text(encoding="utf-8").replace(
            '  - "2027-10-27"\n', '  - "2027-10-27"\n  - "2027-11-17"\n'
        ),
        encoding="utf-8",
    )
    assert date(2027, 11, 17) in load_fomc_config(str(cfg_path)).meeting_dates

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
    )

    assert date(2027, 11, 17) in load_fomc_config(str(cfg_path)).meeting_dates
    assert result.status == WARN
    assert "2027-11-17" in " ".join(result.messages)
    assert "moved" in " ".join(result.messages)


def test_a_lost_advance_notice_is_called_a_parser_problem_not_a_move(cfg_path):
    notice = "A two-day meeting is scheduled for January 25-26, 2028."
    assert FED_PAGE.count(notice) == 1
    before = cfg_path.read_bytes()

    result = check_fomc_calendar(
        str(cfg_path),
        refresh=True,
        today=TODAY,
        fetch=Fetch(html=FED_PAGE.replace(notice, "")),
    )

    assert cfg_path.read_bytes() == before
    assert result.status == WARN
    assert "parser problem" in " ".join(result.messages)


def test_a_refresh_that_would_corrupt_the_file_is_rejected(cfg_path, monkeypatch):
    """The write is validated first: if the edited text does not load as a
    config, nothing replaces the original."""
    _drop_dates(cfg_path, "2028-01-26")
    before = cfg_path.read_bytes()
    monkeypatch.setattr(
        "fred_pipeline.governance.fomc_calendar_check.apply_calendar_refresh",
        lambda text, new, **kw: "meeting_dates: [definitely not valid\n",
    )

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
    )

    assert cfg_path.read_bytes() == before
    assert result.status == WARN
    assert "refresh failed" in result.messages[0]


def test_a_refresh_that_would_drop_an_existing_date_is_rejected(cfg_path, monkeypatch):
    _drop_dates(cfg_path, "2028-01-26")
    before = cfg_path.read_bytes()
    # Looks valid, but silently loses 2026-10-28.
    monkeypatch.setattr(
        "fred_pipeline.governance.fomc_calendar_check.apply_calendar_refresh",
        lambda text, new, **kw: re.sub(r'.*"2026-10-28".*\n', "", text),
    )

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
    )

    assert cfg_path.read_bytes() == before
    assert "would drop existing date" in result.messages[0]
    assert not list(cfg_path.parent.glob(".fomc.yml.refresh-*")), "temp file leaked"


def test_no_edit_means_no_claim_that_dates_were_added(cfg_path, monkeypatch):
    """If the editor hands back the text unchanged, the stage must not report
    dates as added (or rewrite the file): a log line saying 'added 2028-01-26'
    over a file that did not change would send a reviewer looking for a diff
    that does not exist."""
    _drop_dates(cfg_path, "2028-01-26")
    before = cfg_path.read_bytes()
    monkeypatch.setattr(
        "fred_pipeline.governance.fomc_calendar_check.apply_calendar_refresh",
        lambda text, new, **kw: text,
    )

    result = check_fomc_calendar(
        str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
    )

    assert not result.refreshed
    assert result.added_dates == ()
    assert cfg_path.read_bytes() == before


def test_a_read_only_location_falls_back_instead_of_crashing(cfg_path):
    """E.g. a config baked into a deployed bundle: the run must still work."""
    _drop_dates(cfg_path, "2028-01-26")
    cfg_path.parent.chmod(0o555)  # cannot create the temp file
    try:
        result = check_fomc_calendar(
            str(cfg_path), refresh=True, today=TODAY, fetch=Fetch(html=FED_PAGE)
        )
    finally:
        cfg_path.parent.chmod(0o755)

    assert result.status == WARN
    assert not result.refreshed
    assert "refresh failed" in result.messages[0]


# ---- wired into the pipeline ------------------------------------------------


def _spec(series_id="DGS10"):
    return SeriesSpec(
        series_id=series_id,
        title=series_id,
        category="rates",
        frequency="d",
        units="pct",
        validation_profile=ValidationProfile.STANDARD,
    )


class _Warehouse:
    """Just enough warehouse for a run that builds Gold."""

    def __init__(self):
        self.gold_built = 0
        self.calendar_end_seen_by_gold = None

    def restate_start(self, series_id, n):
        return None

    def write_bronze(self, rows):
        return len(rows)

    def merge_silver(self, rows):
        return len(rows)

    def persist_dq(self, run_id, report):
        pass

    def persist_run_state(self, run):
        pass

    def persist_series_run(self, series_run):
        pass

    def build_gold(self):
        self.gold_built += 1
        # What Gold actually sees: read the calendar the way the real build does.
        cfg = load_fomc_config()
        self.calendar_end_seen_by_gold = max(cfg.meeting_dates) if cfg else None
        return {}

    def write_release_calendar(self, rows):
        return len(rows)

    def persist_run(self, run):
        pass


@pytest.fixture
def run_pipeline(observations_payload, fake_client_cls, monkeypatch, tmp_path):
    """Run one pipeline pass with FRED_FOMC_CONFIG_FILE pointing at ``path``."""

    def _run(path: Path | str, **run_kwargs):
        monkeypatch.setenv("FRED_FOMC_CONFIG_FILE", str(path))
        monkeypatch.chdir(tmp_path)
        wh = _Warehouse()
        pipe = FredPipeline(
            PipelineConfig(environment=Environment.DEV, fred_api_key="k"),
            client=fake_client_cls({"DGS10": observations_payload}),
            warehouse=wh,
            persist_audit=False,
        )
        run = pipe.run([_spec()], **run_kwargs)
        return run, pipe._stage_tracker, wh

    return _run


def _future_config(tmp_path: Path, days_ahead: int) -> Path:
    """A minimal valid config whose last meeting is ``days_ahead`` from now."""
    last = date.fromordinal(date.today().toordinal() + days_ahead)
    p = tmp_path / "fomc_pipeline.yml"
    p.write_text(
        f'meeting_dates: ["{last.isoformat()}"]\n'
        "bucket_step_bps: 25\n"
        "target_low_series: DFEDTARL\n"
        "target_high_series: DFEDTARU\n"
        "effective_rate_series: EFFR\n"
        "tenors:\n"
        "  - {series_id: DGS1MO, tenor_months: 1}\n"
        "  - {series_id: DGS3MO, tenor_months: 3}\n"
    )
    return p


def test_a_healthy_calendar_is_recorded_and_leaves_the_run_a_success(
    run_pipeline, tmp_path
):
    run, tracker, wh = run_pipeline(_future_config(tmp_path, 400))

    stage = tracker.get("fomc_calendar")
    assert stage.status is StageStatus.SUCCEEDED
    assert stage.detail["runway_level"] == "ok"
    assert overall_verdict(tracker.records())[0] == "success"
    assert wh.gold_built == 1


def test_a_short_runway_degrades_the_verdict_but_gold_still_builds(
    run_pipeline, tmp_path
):
    _, tracker, wh = run_pipeline(_future_config(tmp_path, 30))

    stage = tracker.get("fomc_calendar")
    assert stage.status is StageStatus.WARNED
    assert "runs out in" in stage.detail["warnings"][0]
    assert overall_verdict(tracker.records())[0] == "partial"
    assert wh.gold_built == 1


def test_a_missing_calendar_fails_the_stage_but_not_the_run(run_pipeline, tmp_path):
    """The case that used to be invisible: no calendar, empty FOMC tables, and a
    run that said it succeeded. Now it is on the record -- and Gold still builds,
    because a calendar problem must never cost you the other 50+ tables."""
    run, tracker, wh = run_pipeline(tmp_path / "does_not_exist.yml")

    stage = tracker.get("fomc_calendar")
    assert stage.status is StageStatus.FAILED
    assert "EMPTY" in stage.error_message
    verdict, reason = overall_verdict(tracker.records())
    assert verdict == "partial"  # optional stage -> degraded, not failed
    assert "fomc_calendar" in reason
    assert run.series_succeeded == 1
    assert wh.gold_built == 1


def test_the_refresh_flag_is_off_by_default(run_pipeline, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "fred_pipeline.governance.fomc_calendar_check.fetch_calendar_html",
        lambda **kw: calls.append(kw) or FED_PAGE,
    )

    run_pipeline(_future_config(tmp_path, 400))

    assert calls == [], "a run without the flag must not touch federalreserve.gov"


def test_the_refresh_flag_reaches_the_check(run_pipeline, tmp_path, monkeypatch):
    seen = []
    from fred_pipeline.governance import fomc_calendar_check as mod

    real = mod.check_fomc_calendar
    monkeypatch.setattr(
        mod,
        "check_fomc_calendar",
        lambda *a, **kw: seen.append(kw.get("refresh")) or real(*a, **kw),
    )

    run_pipeline(_future_config(tmp_path, 400), refresh_fomc_calendar=True)

    assert seen == [True]


def test_a_check_that_blows_up_cannot_stop_the_run(run_pipeline, tmp_path, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("something nobody anticipated")

    monkeypatch.setattr(
        "fred_pipeline.governance.fomc_calendar_check.check_fomc_calendar", boom
    )

    run, tracker, wh = run_pipeline(_future_config(tmp_path, 400))

    assert tracker.get("fomc_calendar").status is StageStatus.FAILED
    assert wh.gold_built == 1
    assert run.series_succeeded == 1


def test_the_stage_is_skipped_not_not_run_when_gold_is_skipped(run_pipeline, tmp_path):
    """A declared stage that never reports reads as NOT_RUN -- a problem state.
    Every path that skips Gold has to record the skip."""
    _, tracker, _ = run_pipeline(_future_config(tmp_path, 400), build_gold_layer=False)

    stage = tracker.get("fomc_calendar")
    assert stage.status is StageStatus.SKIPPED
    assert stage.detail["reason"] == "gold layer not built"


def test_a_refresh_lands_before_gold_reads_the_calendar(
    run_pipeline, tmp_path, monkeypatch
):
    """The reason the stage precedes Gold. If it ran after, the run that fetched
    the new dates would still build the FOMC tables from the old file."""
    import functools

    from fred_pipeline.governance import fomc_calendar_check as mod

    cfg = tmp_path / "fomc_refresh.yml"
    cfg.write_text(REAL_CONFIG.read_text(encoding="utf-8"), encoding="utf-8")
    _drop_dates(cfg, "2027-12-08", "2028-01-26")
    monkeypatch.setattr(
        mod,
        "check_fomc_calendar",
        functools.partial(
            mod.check_fomc_calendar, today=TODAY, fetch=Fetch(html=FED_PAGE)
        ),
    )

    _, tracker, wh = run_pipeline(cfg, refresh_fomc_calendar=True)

    assert wh.calendar_end_seen_by_gold == LAST_MEETING
    assert tracker.get("fomc_calendar").detail["added_dates"] == (
        "2027-12-08, 2028-01-26"
    )

"""spec008 Phase 3: the automation driver's decision logic and command
construction.

Split into two layers, tested separately, matching the module's own split:

* ``plan_actions`` is pure -- dates and meetings in, a plan out. No network,
  no subprocess, no filesystem. Tested directly against synthetic inputs.
* The execution layer (``apply_and_open_pr`` / ``execute_plan`` / the issue
  helpers) is tested with an injected recording runner, so the exact git/gh
  commands this script *would* issue are verified without a real repo, git,
  or `gh` -- the actual execution against a real GitHub repository has never
  been run from this environment (see the module's own docstring).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

from fred_pipeline.catalogs.fomc_calendar import FOMCMeeting

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "fomc_calendar_automation.py"
_spec = importlib.util.spec_from_file_location("fomc_calendar_automation", SCRIPT)
fca = importlib.util.module_from_spec(_spec)
# Must be registered before exec_module: the dataclass decorator on
# AutomationPlan looks itself up via sys.modules[cls.__module__] at class
# creation time, which crashes if the module isn't there yet.
sys.modules["fomc_calendar_automation"] = fca
_spec.loader.exec_module(fca)

AutomationError = fca.AutomationError
AutomationPlan = fca.AutomationPlan
MOVED_MEETING_ISSUE_TITLE = fca.MOVED_MEETING_ISSUE_TITLE
PARSER_ISSUE_TITLE = fca.PARSER_ISSUE_TITLE
REFRESH_BRANCH = fca.REFRESH_BRANCH
RUNWAY_ISSUE_TITLE = fca.RUNWAY_ISSUE_TITLE
_existing_issue_number = fca._existing_issue_number
_existing_pr_number = fca._existing_pr_number
_moved_meeting_issue_body = fca._moved_meeting_issue_body
_parser_issue_body = fca._parser_issue_body
_pr_title = fca._pr_title
_run = fca._run
apply_and_open_pr = fca.apply_and_open_pr
execute_plan = fca.execute_plan
plan_actions = fca.plan_actions

TODAY = date(2026, 9, 23)


def _m(iso: str, *, projection: bool = False) -> FOMCMeeting:
    d = date.fromisoformat(iso)
    return FOMCMeeting(d, d, d.year, is_projection_meeting=projection)


def _advance(iso: str) -> FOMCMeeting:
    """The furthest-out meeting a real scraped feed always carries: the one
    parsed from the advance-notice sentence, not a panel row (see
    catalogs.fomc_calendar's own module docstring on _ADVANCE_NOTICE_RE).
    Needed so synthetic feeds don't accidentally trip
    CalendarDiff.advance_notice_missing the way an all-default FOMCMeeting
    list would (no meeting ever has from_advance_notice=True)."""
    d = date.fromisoformat(iso)
    return FOMCMeeting(d, d, d.year, from_advance_notice=True)


# ---- plan_actions (pure) -------------------------------------------------


def test_quiet_plan_when_in_sync_with_plenty_of_runway():
    # >270 days out from TODAY, clearing spec008 Decision 4's "due" threshold.
    far_out = date(2027, 8, 1)
    meetings = [_advance(far_out.isoformat())]
    plan = plan_actions(meetings, [far_out], today=TODAY)
    assert plan.is_quiet
    assert not plan.propose_pr
    assert not plan.open_moved_meeting_issue
    assert not plan.open_parser_issue
    assert plan.runway_issue_level is None


def test_proposes_pr_for_newly_published_meetings():
    meetings = [_m("2027-01-27"), _m("2027-03-17", projection=True)]
    plan = plan_actions(meetings, [date(2027, 1, 27)], today=TODAY)
    assert plan.propose_pr
    assert [m.decision_date for m in plan.new_meetings] == [date(2027, 3, 17)]
    assert plan.new_meetings[0].is_projection_meeting


def test_new_meetings_are_sorted_regardless_of_input_order():
    meetings = [_m("2027-06-09"), _m("2027-01-27"), _m("2027-03-17")]
    plan = plan_actions(meetings, [], today=TODAY)
    assert [m.decision_date for m in plan.new_meetings] == [
        date(2027, 1, 27), date(2027, 3, 17), date(2027, 6, 9),
    ]  # fmt: skip


def test_opens_moved_meeting_issue_for_a_vanished_date():
    # The vanished date sits BEFORE the advance-notice horizon -- a real
    # moved meeting, not "beyond everything parsed" (which would instead
    # read as a parser problem; see the next test).
    meetings = [_m("2027-01-27"), _advance("2028-01-26")]
    configured = [date(2027, 1, 27), date(2027, 2, 15), date(2028, 1, 26)]
    plan = plan_actions(meetings, configured, today=TODAY)
    assert plan.open_moved_meeting_issue
    assert not plan.open_parser_issue
    assert plan.moved_dates == (date(2027, 2, 15),)


def test_beyond_everything_parsed_with_no_advance_notice_is_a_parser_problem():
    """The mirror case: nothing in the scraped feed carries
    from_advance_notice=True, and the vanished date lies beyond everything
    parsed -- that's the signature of the advance-notice sentence itself
    failing to parse, not a moved meeting."""
    meetings = [_m("2027-01-27"), _m("2027-06-09")]
    configured = [date(2027, 1, 27), date(2027, 6, 9), date(2028, 1, 26)]
    plan = plan_actions(meetings, configured, today=TODAY)
    assert plan.open_parser_issue
    assert not plan.open_moved_meeting_issue


def test_pr_and_issue_can_both_apply_in_one_run():
    """spec008 §6's branches are parallel, not mutually exclusive."""
    meetings = [_m("2027-01-27"), _advance("2028-01-26")]
    configured = [date(2027, 2, 15), date(2028, 1, 26)]
    plan = plan_actions(meetings, configured, today=TODAY)
    assert plan.propose_pr
    assert [m.decision_date for m in plan.new_meetings] == [date(2027, 1, 27)]
    assert plan.open_moved_meeting_issue
    assert plan.moved_dates == (date(2027, 2, 15),)
    assert not plan.is_quiet


def test_runway_issue_level_set_when_runway_shrinks():
    meetings = [_m("2027-01-27")]
    close_date = date(2027, 1, 27)
    plan = plan_actions(meetings, [close_date], today=date(2026, 12, 1))
    assert plan.runway_issue_level is not None
    assert plan.runway_days_remaining == (close_date - date(2026, 12, 1)).days


def test_configured_and_parsed_through_reflect_the_max_date():
    meetings = [_m("2027-01-27"), _m("2027-06-09")]
    plan = plan_actions(meetings, [date(2027, 1, 27), date(2027, 3, 17)], today=TODAY)
    assert plan.parsed_through == date(2027, 6, 9)
    assert plan.configured_through == date(2027, 3, 17)


def test_pr_title_uses_parsed_through_when_available():
    plan = AutomationPlan(
        propose_pr=True, new_meetings=(), open_moved_meeting_issue=False,
        open_parser_issue=False, moved_dates=(), runway_issue_level=None,
        runway_days_remaining=100, parsed_through=date(2027, 6, 9),
        configured_through=None,
    )
    assert "2027-06-09" in _pr_title(plan)


def test_moved_meeting_issue_body_lists_each_date_and_never_implies_an_edit():
    plan = AutomationPlan(
        propose_pr=False, new_meetings=(), open_moved_meeting_issue=True,
        open_parser_issue=False, moved_dates=(date(2027, 1, 1), date(2027, 6, 1)),
        runway_issue_level=None, runway_days_remaining=100,
        parsed_through=None, configured_through=None,
    )
    body = _moved_meeting_issue_body(plan)
    assert "2027-01-01" in body
    assert "2027-06-01" in body
    assert "Nothing has been edited" in body


def test_parser_issue_body_includes_the_error_text():
    body = _parser_issue_body(error="boom", saved_html_path=None)
    assert "boom" in body


def test_parser_issue_body_names_the_advance_notice_case_without_an_error():
    body = _parser_issue_body(error=None, saved_html_path="/tmp/x.html")
    assert "advance-notice" in body.lower()
    assert "/tmp/x.html" in body


# ---- execution layer (injected runner) -----------------------------------


class _Recorder:
    """Records every command issued and returns canned results, keyed by a
    prefix match -- e.g. registering ("gh", "pr", "list") answers every `gh
    pr list ...` call regardless of its other flags."""

    def __init__(self):
        self.calls: list[list[str]] = []
        self._canned: dict[tuple[str, ...], subprocess.CompletedProcess] = {}

    def set_result(self, prefix: tuple[str, ...], *, returncode=0, stdout="", stderr=""):
        self._canned[prefix] = subprocess.CompletedProcess(
            list(prefix), returncode, stdout, stderr
        )

    def __call__(self, cmd: list[str]) -> subprocess.CompletedProcess:
        self.calls.append(cmd)
        for prefix, result in self._canned.items():
            if tuple(cmd[: len(prefix)]) == prefix:
                return result
        return subprocess.CompletedProcess(cmd, 0, "", "")


def _empty_json_list(recorder: _Recorder, *prefixes: tuple[str, ...]) -> None:
    for prefix in prefixes:
        recorder.set_result(prefix, stdout="[]")


def test_run_raises_automation_error_on_nonzero_exit():
    recorder = _Recorder()
    recorder.set_result(("git", "push"), returncode=1, stderr="denied")
    with pytest.raises(AutomationError):
        _run(recorder, ["git", "push"])


def test_run_tolerates_an_allowed_failure_text():
    recorder = _Recorder()
    recorder.set_result(("git", "commit"), returncode=1, stdout="nothing to commit")
    result = _run(recorder, ["git", "commit"], allow_failure_text="nothing to commit")
    assert result.returncode == 1


def test_existing_pr_number_parses_gh_output():
    recorder = _Recorder()
    recorder.set_result(("gh", "pr", "list"), stdout=json.dumps([{"number": 42}]))
    assert _existing_pr_number(recorder, REFRESH_BRANCH) == 42


def test_existing_pr_number_none_when_list_is_empty():
    recorder = _Recorder()
    recorder.set_result(("gh", "pr", "list"), stdout="[]")
    assert _existing_pr_number(recorder, REFRESH_BRANCH) is None


def test_existing_issue_number_parses_gh_output():
    recorder = _Recorder()
    recorder.set_result(("gh", "issue", "list"), stdout=json.dumps([{"number": 7}]))
    assert _existing_issue_number(recorder, PARSER_ISSUE_TITLE) == 7


def test_apply_and_open_pr_creates_when_no_existing_pr(tmp_path):
    config_path = tmp_path / "fomc.yml"
    config_path.write_text('meeting_dates:\n  - "2026-01-15"\n', encoding="utf-8")

    plan = AutomationPlan(
        propose_pr=True,
        new_meetings=(_m("2026-03-01"),),
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level=None, runway_days_remaining=100,
        parsed_through=date(2026, 3, 1), configured_through=None,
    )
    recorder = _Recorder()
    _empty_json_list(recorder, ("gh", "pr", "list"))

    apply_and_open_pr(plan, config_path=config_path, runner=recorder)

    assert '  - "2026-03-01"' in config_path.read_text(encoding="utf-8")
    commands = [c[0:3] for c in recorder.calls]
    assert ["git", "checkout", "-B"] in commands
    assert ["git", "push", "-u"] in commands
    create_calls = [c for c in recorder.calls if c[:3] == ["gh", "pr", "create"]]
    assert len(create_calls) == 1
    assert "--head" in create_calls[0] and REFRESH_BRANCH in create_calls[0]


def test_apply_and_open_pr_edits_when_a_pr_already_exists(tmp_path):
    config_path = tmp_path / "fomc.yml"
    config_path.write_text('meeting_dates:\n  - "2026-01-15"\n', encoding="utf-8")

    plan = AutomationPlan(
        propose_pr=True, new_meetings=(_m("2026-03-01"),),
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level=None, runway_days_remaining=100,
        parsed_through=date(2026, 3, 1), configured_through=None,
    )
    recorder = _Recorder()
    recorder.set_result(("gh", "pr", "list"), stdout=json.dumps([{"number": 5}]))

    apply_and_open_pr(plan, config_path=config_path, runner=recorder)

    edit_calls = [c for c in recorder.calls if c[:3] == ["gh", "pr", "edit"]]
    create_calls = [c for c in recorder.calls if c[:3] == ["gh", "pr", "create"]]
    assert len(edit_calls) == 1
    assert "5" in edit_calls[0]
    assert not create_calls


def test_apply_and_open_pr_dry_run_never_writes_the_file(tmp_path):
    config_path = tmp_path / "fomc.yml"
    original = 'meeting_dates:\n  - "2026-01-15"\n'
    config_path.write_text(original, encoding="utf-8")

    plan = AutomationPlan(
        propose_pr=True, new_meetings=(_m("2026-03-01"),),
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level=None, runway_days_remaining=100,
        parsed_through=date(2026, 3, 1), configured_through=None,
    )
    recorder = _Recorder()
    _empty_json_list(recorder, ("gh", "pr", "list"))

    apply_and_open_pr(plan, config_path=config_path, runner=recorder, dry_run=True)

    assert config_path.read_text(encoding="utf-8") == original


def test_apply_and_open_pr_is_a_noop_when_nothing_new(tmp_path):
    """Defensive: apply_calendar_refresh's own no-op guard should mean this
    function never even reaches a git command when there's nothing to add."""
    config_path = tmp_path / "fomc.yml"
    config_path.write_text('meeting_dates:\n  - "2026-03-01"\n', encoding="utf-8")

    plan = AutomationPlan(
        propose_pr=True, new_meetings=(_m("2026-03-01"),),  # already present
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level=None, runway_days_remaining=100,
        parsed_through=date(2026, 3, 1), configured_through=None,
    )
    recorder = _Recorder()
    apply_and_open_pr(plan, config_path=config_path, runner=recorder)
    assert recorder.calls == []


def test_execute_plan_only_runs_the_branches_the_plan_calls_for(tmp_path):
    config_path = tmp_path / "fomc.yml"
    config_path.write_text('meeting_dates:\n  - "2026-01-15"\n', encoding="utf-8")

    plan = AutomationPlan(
        propose_pr=False, new_meetings=(),
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level="urgent", runway_days_remaining=10,
        parsed_through=None, configured_through=date(2026, 1, 15),
    )
    recorder = _Recorder()
    _empty_json_list(recorder, ("gh", "issue", "list"))

    execute_plan(plan, config_path=config_path, runner=recorder)

    assert not any(c[:2] == ["git", "checkout"] for c in recorder.calls)
    create_calls = [c for c in recorder.calls if c[:3] == ["gh", "issue", "create"]]
    assert len(create_calls) == 1
    assert RUNWAY_ISSUE_TITLE in create_calls[0]
    assert "urgent" in create_calls[0]


def test_execute_plan_quiet_plan_issues_no_commands(tmp_path):
    config_path = tmp_path / "fomc.yml"
    config_path.write_text('meeting_dates:\n  - "2026-01-15"\n', encoding="utf-8")
    plan = AutomationPlan(
        propose_pr=False, new_meetings=(),
        open_moved_meeting_issue=False, open_parser_issue=False, moved_dates=(),
        runway_issue_level=None, runway_days_remaining=1000,
        parsed_through=None, configured_through=date(2026, 1, 15),
    )
    recorder = _Recorder()
    execute_plan(plan, config_path=config_path, runner=recorder)
    assert recorder.calls == []


def test_moved_meeting_and_parser_issue_use_different_stable_titles():
    """They need to stay distinguishable so dedup-by-title never conflates
    a genuinely moved meeting with a parser bug."""
    assert MOVED_MEETING_ISSUE_TITLE != PARSER_ISSUE_TITLE


# ---- main() exit-code contract (subprocess -- exercises real CLI parsing) --


def _run_main(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True, text=True,
        cwd=SCRIPT.parents[1],
        env={**os.environ, "PYTHONPATH": "src"},
    )


def test_main_exits_zero_on_a_clean_in_sync_run():
    result = _run_main([
        "--html-file", str(Path(__file__).parent / "fixtures" / "fomccalendars.html"),
        "--config", "config/fomc.yml", "--dry-run",
    ])
    assert result.returncode == 0, result.stderr


def test_main_exits_nonzero_when_the_page_fails_to_parse(tmp_path):
    """AC-006: a broken parser must make the job itself go red, not just
    quietly open an issue."""
    bad_html = tmp_path / "bad.html"
    bad_html.write_text("<html>nothing here</html>", encoding="utf-8")
    result = _run_main([
        "--html-file", str(bad_html), "--config", "config/fomc.yml", "--dry-run",
    ])
    assert result.returncode == 1, result.stderr

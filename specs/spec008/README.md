# Spec 008: Productionalizing the FOMC Meeting Calendar

Status: **proposed — ready for review**

Last verified: 2026-09-15

Primary owner: TBD

Target: `config/fomc.yml`'s `meeting_dates` block and everything that
expires with it — `gold.fomc_probability`, `gold.fomc_meeting_path`, the
terminal FOMC module, and the Power BI Fed Policy Watch report.

Builds on: [`docs/handoffs/fomc_calendar_scraper.md`](../../docs/handoffs/fomc_calendar_scraper.md)
(the scraper this spec automates — read it first; its §4 parsing contract
and §5 failure table are assumed here, not restated).

---

## 1. Goal

The FOMC scraper solved the *typing* problem and left the *remembering*
problem untouched.

`config/fomc.yml` is a hand-maintained list of forward meeting dates that
expires. The scraper (`scripts/scrape_fomc_calendar.py`, shipped 2026-08-14)
turns "read a web page and retype twelve dates" into one command — but that
command only helps someone who runs it, and nothing in this repo runs it.
The entire mechanism that decides *when* to run it is a unit test that fails
once runway drops below 120 days, in the middle of the normal `pytest -q`
CI job, on every unrelated pull request.

This spec closes the loop: a scheduled job that watches the Fed's calendar,
opens a reviewable pull request when new dates appear, opens an issue with
saved evidence when the page structure changes, and escalates as runway
shrinks — so the config cannot expire, and so no one has to remember a
once-a-year chore.

It also fixes a second problem the scraper shipped with: **its parser has
never been run against the real page.** Automating an unvalidated parser
would industrialize a guess, so §8 Phase 0 gates everything else on a first
live run.

## 2. Current State (Evidence)

### 2.1 The expiry is silent

`compute_fomc_probability` (`writer/terminal_views.py:1900`) filters to
future meetings and returns empty on an exhausted list, with no error and
no warning:

```python
# src/fred_pipeline/writer/terminal_views.py:1956-1958
meetings = [d for d in cfg.meeting_dates if d >= today]
if not meetings:
    return {"probability": [], "meeting_path": []}
```

Both Gold tables go empty, the pipeline reports success, and the Power BI
report renders blank. `RunStatus` is computed from series outcomes, so
nothing in the run's own audit trail registers that a Gold family emptied.

### 2.2 The only alarm is a wall-clock unit test

```python
# tests/test_fomc_probability.py:244
MIN_RUNWAY_DAYS = 120  # ~2-3 meetings' notice to refresh the calendar

# tests/test_fomc_probability.py:262-275
def test_fomc_meeting_dates_have_runway():
    """Early warning: fail while there is still time to act, not after."""
    ...
    remaining = (last - date.today()).days
    assert remaining >= MIN_RUNWAY_DAYS, (...)
```

This test is collected by the ordinary `pytest -q` step in
`.github/workflows/ci.yml`, which runs on `push: branches: ["**"]` and on
every `pull_request`. Measured 2026-09-15:

| Fact | Value |
|---|---|
| Last configured meeting | `2028-01-26` (`config/fomc.yml:38`) |
| Runway remaining today | **498 days** |
| Date this test starts failing | **2027-09-28** |
| What fails with it | every CI job on every branch and PR, repo-wide |

So the current design is a time bomb that, on a known date, turns every
unrelated pull request red until someone hand-edits a config. The alarm is
correct to exist; wiring it into the blocking path of all other work is the
defect. `docs/handoffs/fomc_calendar_scraper.md` §9 already anticipates the
intended usage — "run `--check` from the same place that notices the
120-day runway test failing" — but that place is a human noticing a red X,
and nothing automates it.

This is also a concrete instance of the calendar-edge risk
[`specs/spec005`](../spec005/README.md) §2.3 flags generically ("a few tests
read `date.today()`/current year directly").

### 2.3 The scraper is ready to be driven, and has a machine-readable contract

Already built and tested against fixture HTML:

| Capability | Location |
|---|---|
| `--check` mode, exit 1 on drift | `scripts/scrape_fomc_calendar.py:190-208` |
| Future-only diff, both directions | `catalogs/fomc_calendar.py:354-373` (`diff_against_config`) |
| Paste-ready YAML, items only | `catalogs/fomc_calendar.py:375-393` (`format_yaml_block`) |
| Raises rather than returning `[]` | `catalogs/fomc_calendar.py:249-` (`parse_fomc_calendar`) |
| Saves fetched HTML | `--save-html` |
| Exit codes | `0` in sync · `1` drift · `2` scrape/parse failure |

`CalendarDiff` already separates the two drift directions that need
different handling — `missing_from_config` (the Fed published more; safe to
propose) and `absent_upstream` (a configured date vanished; a meeting may
have *moved*, needs a human).

### 2.4 The parser has never seen the real page

`tests/fixtures/fomccalendars.html` is hand-built, and says so in its own
header comment:

> FIXTURE — hand-built to the structure documented in
> `docs/handoffs/fomc_calendar_scraper.md` §4.1. This is NOT a capture of
> the live federalreserve.gov page: egress to that host was blocked from
> the environment where the scraper was written... **REPLACE THIS FILE on
> the first successful live run.**

The handoff's §7 acceptance gate — "First live run is the acceptance test"
— has not been performed. `config/fomc.yml:8-9` was verified by hand on
2026-07-17, a month *before* the scraper existed, so even the config's
provenance is not scraper-derived. The Selenium fallback has never executed
successfully anywhere (handoff §8).

**Everything in this spec is therefore gated on Phase 0.** Automation on
top of an unvalidated parser converts a manual guess into an automatic one.

### 2.5 `config/fomc.yml` is canonical, and deliberately so

FRED's own FOMC release is not a usable substitute. `config/release_calendar.yml:15-23`
records that `release_id: 101` was live-tested and rejected — it degenerates
into a placeholder for nearly every business day instead of the ~8 scheduled
meetings. There is no upstream API to switch to; scraping the Fed's page is
the only route, which raises the stakes on handling its structure changing.

## 3. Non-Goals

- **Auto-committing to `config/fomc.yml` on `main`.** The existing decision
  that a rate-path config deserves a human diff (handoff §2, and the reason
  `format_yaml_block` emits items rather than a whole file) is correct and
  is preserved. This spec automates everything *up to* the merge button.
- Unscheduled/emergency meetings. Not on the forward calendar by definition.
- Predicting dates the Fed has not published. The Fed publishes ~2 years
  out; no automation creates data that does not exist upstream (handoff §9).
- Replacing the scraper's parser or its §4 parsing contract.
- Wiring the calendar check into `fred_pipeline run` — see §5 decision 1.
- Historical meeting metadata (statements, minutes, projections links).
- Making the Selenium backend a supported production path. It stays the
  break-glass fallback it is today.

## 4. Design Facts

Facts that constrain the design, all verified:

1. **The cadence mismatch is extreme.** The Fed extends the schedule roughly
   once a year; a check that runs daily does ~365 fetches to catch one
   event. Cost is near zero but *noise* is not: a job that cries wolf gets
   muted, and a muted job is the same as no job.
2. **Drift is not symmetric.** `missing_from_config` is additive and safe to
   propose mechanically. `absent_upstream` means a date the Fed listed is
   gone — most likely a moved meeting — and must never be auto-removed.
3. **A structure change and an empty calendar are indistinguishable from the
   outside** unless you assert on them. The parser already resolves this by
   raising on zero meetings rather than returning `[]`.
4. **The dev container cannot reach `federalreserve.gov`** (handoff §8), and
   neither could the environment that wrote the scraper. GitHub Actions
   runners *can*. CI is therefore not merely a convenient host — it is the
   only place in this project's toolchain where this job can run at all.
5. **`config/fomc.yml` provenance lives in comments, not fields.** The
   "verified live against ... (2026-07-17)" note at `config/fomc.yml:8-9`
   is prose. Nothing can read it, so nothing can tell you the config is
   stale-by-provenance rather than stale-by-expiry.
6. **The repo already has an alerting fabric, but it is run-scoped.**
   `governance/alerting.py::send_run_alert` and `config/alerting.yml` route
   on `EtlRun` verdicts (`success`/`partial`/`failure`). A calendar check is
   not a pipeline run and has no `EtlRun` to attach to.

## 5. Design Decisions

### Decision 1 — The job lives in a scheduled GitHub Actions workflow, not in `fred_pipeline run`

**`.github/workflows/fomc-calendar.yml`, on a `schedule:` cron, plus
`workflow_dispatch:` for on-demand runs.**

Rejected alternatives, with reasons:

| Option | Why not |
|---|---|
| A stage inside `fred_pipeline run` | Puts a `federalreserve.gov` dependency in the hot path of every pipeline run to service a once-a-year event. A Fed outage or a proxy block would degrade unrelated production runs. Wrong cadence, wrong blast radius. |
| Keep it as a blocking unit test | This is the status quo defect (§2.2): the alarm fires by breaking everyone else's CI. |
| A cron on an operator's laptop | Invisible, unowned, and dies with the laptop. The current de-facto design. |
| A `pre-commit` hook | Fires on commit frequency, needs network in the dev container (blocked, per fact 4), and punishes contributors for an unrelated maintenance state. |

The scheduled workflow is the only option that matches the cadence (time-
based, not activity-based), has network egress (fact 4), has a natural home
for credentials it does not need, and can *act* — open a PR, open an issue —
rather than merely report.

**Cadence: weekly, Mondays.** Weekly is ~52 fetches/year against a public
page — negligible load, honest UA already in place — and bounds worst-case
detection latency at 7 days against an event with months of slack. Daily
buys nothing; monthly risks aliasing against an announcement during a
quiet-period.

### Decision 2 — Machine proposes, human merges: drift opens a pull request

When `--check` reports `missing_from_config`, the workflow does not print
and hope. It:

1. edits `config/fomc.yml` — inserting the new dates into `meeting_dates`
   in ascending order, and updating the provenance comment's date;
2. commits on a deterministic branch, `automation/fomc-calendar-refresh`;
3. opens (or force-updates) a PR titled with the new coverage horizon;
4. lets normal CI run on it — including the runway test, which the PR fixes.

This preserves the reason behind "the tool prints, a person pastes": the
human diff still happens, on the PR, with the surrounding commentary intact
and the full file visible. What is removed is the toil, the transcription
risk, and the dependency on someone remembering.

A deterministic branch name means repeated runs update one PR instead of
opening 52 of them.

**`absent_upstream` never edits anything.** It opens an issue instead
(Decision 3). A vanished date means a meeting moved; silently dropping it
would shorten the modelled rate path, which is exactly the failure the
handoff §5 calls "worse than crashing".

### Decision 3 — Structure changes open an issue *with the evidence attached*

Exit code `2` (scrape or parse failure) is the Fed-restructured-the-page
signal. The workflow then:

1. re-runs with `--save-html` to capture what it actually received;
2. uploads that HTML as a workflow artifact (retention 90 days);
3. opens a GitHub issue containing the failing command, both backends'
   error text, the artifact link, and a pointer to the §4.1 parsing
   contract;
4. labels it `fomc-calendar`, `broken-parser`.

The point is that the person who picks this up gets the page *as it was
when it broke*, not an invitation to re-fetch a page that may have changed
again. This is the single highest-value piece of automation here, because
a structure change is both the likeliest failure and the one where evidence
decays fastest.

Issues are deduplicated on a stable title so a persistent breakage does not
open a weekly issue.

### Decision 4 — Escalation is driven by runway, not by drift

Drift and urgency are different axes. The Fed can go months without
publishing (no drift, shrinking runway); the job must get louder as the
cliff approaches even when there is nothing to propose.

| Runway remaining | Job behavior |
|---|---|
| > 270 days | Log only. Green check, no noise. |
| ≤ 270 days | Open/refresh a tracking issue: "FOMC calendar refresh due". |
| ≤ 120 days | Escalate the issue (label `priority`), and the existing unit test's threshold is reached — but see Decision 5. |
| ≤ 45 days | Escalate further; issue title marked `URGENT`. This is the last window in which a normal review cycle still fits. |
| ≤ 0 | Gold tables are already empty. Issue marked `EXPIRED`. |

Thresholds are config, not literals — see Decision 6.

### Decision 5 — The blocking unit test becomes a floor, not the alarm

`test_fomc_meeting_dates_have_runway` stays, because a genuine last-resort
gate is worth having, but its role changes:

- Its threshold drops from **120 days to 45 days**. Above 45 days the
  scheduled job owns the warning; below it, the situation is a real defect
  and blocking a release is proportionate.
- Its failure message is rewritten to point at the workflow and the
  auto-PR branch first, and hand-editing second.
- It is marked so it can be selected/deselected explicitly (`maintenance`
  marker, registered per spec005 §6.1 if that lands first; a plain marker
  otherwise).

Net effect: the repo-wide CI time bomb moves from 2027-09-28 to
2027-12-12, and by then the scheduled job will have been opening a PR for
months. The test should never be what tells anyone.

### Decision 6 — Structured provenance in `config/fomc.yml`

Add machine-readable fields alongside `meeting_dates`:

```yaml
calendar_provenance:
  source_url: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
  last_verified: 2026-07-17      # date the dates were confirmed against the page
  verified_by: manual            # manual | scraper
  published_through: 2028-01-26  # the Fed's own horizon, not our last row
```

`published_through` is the field that makes "the Fed has not extended yet"
distinguishable from "nobody has checked lately" — today those two states
look identical from inside the repo (fact 5). The scraper sets these when
it proposes a PR; `FOMCConfig` parses them; a new test asserts
`last_verified` is not absurdly old.

This block is additive and optional-on-read, so an older config still
loads (NFR-compatible with `FOMCConfig.__post_init__` at
`gold_config/fomc_config.py:52-65`).

### Decision 7 — The live capture becomes self-maintaining

Every successful scheduled run uploads the fetched HTML as an artifact.
When a run parses successfully *and* the markup differs materially from
`tests/fixtures/fomccalendars.html`, the refresh PR also updates the
fixture.

This retires the §2.4 problem permanently: the fixture stops being a
hand-built guess and becomes a rolling capture of real markup, refreshed by
the same mechanism that watches for drift — and any parser change forced by
a Fed redesign arrives with the real page attached to the PR that needs it.

## 6. Proposed Approach

```text
                    weekly cron (Mon) ─────────┐
                    workflow_dispatch ─────────┤
                                               v
                    scrape_fomc_calendar.py --check --save-html
                                               │
                 ┌─────────────┬───────────────┼────────────────┐
              exit 0        exit 1          exit 1           exit 2
             in sync   missing_from_config  absent_upstream  parse/fetch
                 │             │               │                │
                 v             v               v                v
          runway check   edit config      open issue      save HTML artifact
                 │        + open PR       (moved date?)   + open issue with
                 │             │               │           both backends' errors
                 └─────────────┴───────────────┴────────────────┘
                                               │
                                               v
                                  escalate by runway (Decision 4)
```

New code is deliberately thin. The parser, differ, YAML renderer, fetch
backends and exit codes all exist. What this spec adds:

| Piece | Where | Roughly |
|---|---|---|
| `--json` output mode for `--check` | `scripts/scrape_fomc_calendar.py` | small — serialize `CalendarDiff` + runway so the workflow reads structured data instead of scraping stdout |
| Config editing (insert dates, bump provenance) | new `catalogs/fomc_calendar.py` function | moderate — must preserve comments; insert into the list, never rewrite the file |
| `calendar_provenance` parsing | `gold_config/fomc_config.py` | small |
| The workflow | `.github/workflows/fomc-calendar.yml` | moderate |
| Runway/escalation helper | `catalogs/fomc_calendar.py` | small, pure, fully testable |
| Test updates | `tests/test_fomc_calendar_scraper.py`, `tests/test_fomc_probability.py` | moderate |

The config editor is the only genuinely delicate part, and it is pure
string-in/string-out — testable without network, browser, or GitHub.

## 7. Acceptance Criteria

| ID | Given / When / Then | Evidence |
|---|---|---|
| AC-001 | Given the live Fed calendar page, when the scraper runs against it for the first time, then the parsed dates match the page read by a human, and the saved HTML replaces the hand-built fixture. | live run output + fixture diff |
| AC-002 | Given the refreshed real-markup fixture, when the existing parser suite runs, then it passes — or the parser is corrected and the correction is covered by a test. | `pytest tests/test_fomc_calendar_scraper.py` |
| AC-003 | Given a page listing meetings the config lacks, when the workflow runs, then it opens exactly one PR on `automation/fomc-calendar-refresh` inserting those dates in ascending order with provenance updated and all existing comments intact. | workflow run + PR diff |
| AC-004 | Given a second run while that PR is open, when drift is unchanged, then the existing PR is updated in place and no second PR is opened. | workflow run log |
| AC-005 | Given a configured date absent from the live page, when the workflow runs, then it opens an issue and makes **no** edit to `config/fomc.yml`. | workflow run + unchanged config |
| AC-006 | Given HTML that parses to zero meetings, when the workflow runs, then it exits non-zero, uploads the received HTML as an artifact, and opens a labelled issue naming both backends' failures. | synthetic-fixture workflow test |
| AC-007 | Given runway at each Decision 4 threshold, when the escalation helper runs, then it returns the documented level; boundaries are tested at exactly 270/120/45/0 days with an injected clock, not `date.today()`. | unit tests |
| AC-008 | Given a config whose last meeting is 46+ days out, when the unit suite runs, then `test_fomc_meeting_dates_have_runway` passes; at 44 days it fails with a message naming the workflow first. | unit tests with injected clock |
| AC-009 | Given a `config/fomc.yml` with no `calendar_provenance` block, when `FOMCConfig` loads it, then it loads successfully and the Gold tables build unchanged. | back-compat test |
| AC-010 | Given the workflow file, when inspected, then it requests only the permissions it needs (`contents: write`, `pull-requests: write`, `issues: write`), uses no secrets beyond the default token, and never runs on `pull_request` from a fork. | workflow review |
| AC-011 | Given a merged refresh PR, when the pipeline next builds Gold, then `gold.fomc_probability` and `gold.fomc_meeting_path` are non-empty and cover the extended horizon. | local run against the merged config |
| AC-012 | Given the Fed page is unreachable (proxy/DNS/5xx), when the workflow runs, then it fails loudly with the `requests` cause reported first and does not open a spurious drift PR or issue. | injected-failure test |

## 8. Phased Implementation Plan

### Phase 0 — Live validation (blocks everything else)

Nothing in this spec is trustworthy until the parser has met the real page.
Run from a machine with egress to `federalreserve.gov`:

```bash
python scripts/scrape_fomc_calendar.py --save-html /tmp/fomc-live.html
```

Then: eyeball every parsed date against the page; confirm the two-day rule
and any month-boundary meeting; commit the saved HTML over
`tests/fixtures/fomccalendars.html`; re-run the parser suite; fix the parser
if it breaks and cover the fix.

Also confirm `config/fomc.yml`'s 2028-01-26 entry, which
`config/fomc.yml:34-37` flags as taken from secondary reporting rather than
read off the Fed page.

**Exit gate:** AC-001, AC-002. If the parser needs changes, they land here,
before any automation depends on it.

### Phase 1 — Structured output and provenance

`--json` for `--check`; the `calendar_provenance` block and its parsing;
the runway/escalation helper with injected-clock tests; the unit test
threshold change (Decision 5).

**Exit gate:** AC-007, AC-008, AC-009. All offline, all deterministic.

### Phase 2 — The config editor

Comment-preserving insertion into `meeting_dates` plus provenance bump.
Pure function, tested against the real committed config as a fixture,
including: insertion into the middle, append at the end, no-op when already
present, and a golden test proving every comment survives.

**Exit gate:** a config edited by the tool differs from a hand-edit only in
the dates added.

### Phase 3 — The workflow

`.github/workflows/fomc-calendar.yml`: weekly cron, `workflow_dispatch`,
the four-way branch of §6, PR creation/update, issue creation with
deduplication, artifact upload, least-privilege permissions.

Validate by `workflow_dispatch` against a pinned fixture served locally
before pointing it at the Fed.

**Exit gate:** AC-003 through AC-006, AC-010, AC-012.

### Phase 4 — Close the loop and document

Merge a real refresh PR end to end; confirm Gold rebuilds non-empty
(AC-011); update `docs/handoffs/fomc_calendar_scraper.md` to describe the
automated path as primary and the manual command as break-glass; retire the
handoff's §7 "first live run is the acceptance test" warning, which Phase 0
will have discharged.

## 9. Risks and Mitigations

| ID | Risk | Impact | Mitigation |
|---|---|---|---|
| R-01 | The Fed restructures the page and the parser silently returns *fewer* dates rather than zero. | Modelled rate path quietly shortens — the worst failure in the system. | Parser already raises on zero; keep the per-year "fewer than 6 meetings" warning and promote it to a non-zero exit in `--check --json`. AC-006. |
| R-02 | Automation opens a PR with wrong dates and a reviewer rubber-stamps it. | Bad dates in a rate-path model. | PR body shows the raw page labels beside parsed dates (`FOMCMeeting.raw_label` exists for this), so review is a comparison, not an act of faith. |
| R-03 | The weekly job becomes background noise and gets muted. | Same end state as no automation. | Green runs are silent (Decision 4, >270 days = log only). The job only speaks when it has something actionable. |
| R-04 | `absent_upstream` fires because the Fed reorganized, not because a meeting moved. | Spurious issue. | Issue, never an edit; text explains both causes and asks for a human read. Deduplicated on title. |
| R-05 | GitHub Actions cron is unreliable/delayed under load. | Detection latency beyond 7 days. | Slack is measured in months; a delayed weekly run is immaterial. `workflow_dispatch` covers urgency. |
| R-06 | The default `GITHUB_TOKEN` cannot open PRs if the org restricts it. | Automation silently does nothing. | AC-010 verifies permissions explicitly; Phase 3 validates by dispatch before relying on cron. A failed PR creation must fail the job, not pass quietly. |
| R-07 | Phase 0 never happens because egress stays blocked. | Whole spec stalls on an unvalidated parser. | Phase 0 needs any machine with normal internet — an operator laptop suffices; it is one command. Do not proceed past it. |
| R-08 | The fixture auto-refresh (Decision 7) overwrites a good fixture with a broken capture. | Tests start passing against wrong markup. | Fixture updates ride the same reviewed PR and only when the parse *succeeded*; never on the exit-2 path. |

## 10. Open Decisions

1. **Cron cadence.** Weekly/Monday proposed (Decision 1). Confirm, or pick
   fortnightly.
2. **Escalation thresholds.** 270/120/45/0 proposed (Decision 4). The 270
   figure assumes the Fed's ~annual extension plus a comfortable review
   window; adjust if that feels early.
3. **Issue vs. email.** This spec uses GitHub issues because the job lives
   in Actions and the audience is whoever merges PRs. If the FOMC calendar
   should instead reach the `data-oncall@` route in `config/alerting.yml`,
   that needs a non-`EtlRun` alert path (fact 6) — a small but real addition
   to `governance/alerting.py`. Decide before Phase 3.
4. **Unit-test threshold.** 45 days proposed (Decision 5). Alternative: drop
   the blocking test entirely once the workflow is proven, on the grounds
   that a release-blocking test for a data-freshness problem is a category
   error. Recommend keeping it at 45 for one full refresh cycle, then
   revisiting with evidence.
5. **`published_through` maintenance.** Populated by the scraper from the
   page's furthest panel. Confirm the page states its own horizon
   unambiguously — resolvable only during Phase 0.
6. **Fixture auto-refresh.** Decision 7 proposes it ride the refresh PR.
   Alternative: a separate PR so parser-affecting markup changes are never
   bundled with date additions. Cleaner, noisier.

---

## Appendix A — Why not just use an upstream API

Recorded so this is not re-litigated. FRED's FOMC release
(`release_id: 101`) was live-tested and rejected — it degenerates into a
near-daily placeholder rather than the ~8 scheduled meetings/year. The
finding and the reasoning are preserved at `config/release_calendar.yml:15-23`.
There is no other free, authoritative, machine-readable source for forward
FOMC decision dates; the Fed's own calendar page is the primary source, and
scraping it is the only route. That is precisely why the structure-change
handling in Decision 3 carries more weight here than it would for a source
with an API to fall back on.

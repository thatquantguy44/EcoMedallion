# FOMC Calendar Scraper — Spec & Plan

**Status:** spec approved, implementation shipped alongside this document.
**The scraper is built but nothing runs it** — see §10 and
[`specs/spec008`](../../specs/spec008/README.md), which automates that gap.
**Owner:** pipeline / governance
**Consumes:** <https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm>
**Produces:** the `meeting_dates` block of [`config/fomc.yml`](../../config/fomc.yml)
**Code:** `src/fred_pipeline/catalogs/fomc_calendar.py` (pure parser +
`requests`/Selenium fetch) · `scripts/scrape_fomc_calendar.py` (CLI)
**Related:** [`powerbi_report_build_plan.md`](powerbi_report_build_plan.md) §Report 6

---

## 1. Why this exists

`config/fomc.yml` declares the scheduled FOMC decision dates that
`gold.fomc_probability` and `gold.fomc_meeting_path` are built from. The list is
hand-maintained and **expires**: `compute_fomc_probability` filters to
`d >= today`, so once the last configured meeting passes, both Gold tables emit
nothing and the Power BI Fed Policy Watch report renders blank with no error
raised anywhere.

`tests/test_fomc_probability.py::test_fomc_meeting_dates_have_runway` already
fires when fewer than 120 days remain. This scraper is what you run **when that
alarm goes off** — it turns "go read a web page and retype twelve dates" into
one command, and removes the transcription errors that a hand-edit invites.

The Fed publishes roughly two years ahead and extends the schedule about once a
year, so the expected cadence of use is **once or twice a year**. This is a
maintenance tool, not a pipeline stage. It is deliberately not wired into
`fred_pipeline run`.

## 2. Scope

**In scope**
- Fetch the Fed's FOMC calendar page over plain HTTP or with Selenium (§3).
- Parse every *scheduled* meeting into a decision date (see §4.2).
- Emit YAML ready to paste into `config/fomc.yml`, or diff against what is
  already there.
- A `--check` mode suitable for CI or a cron: exit non-zero when the live page
  contains meetings the config lacks.

**Out of scope**
- Editing `config/fomc.yml` in place. The file carries hand-written commentary
  and provenance comments that a naive rewrite would destroy, and a config that
  drives a rate-path model deserves a human diff. The tool prints; a person
  pastes.
- Unscheduled/emergency meetings. They are not on the forward calendar by
  definition, and the probability engine models the scheduled path.
- Historical meeting metadata (statements, minutes, projections links).

## 3. Two fetch backends

The Fed's calendar page is **server-rendered static HTML**, so the two backends
have a clear division of labour:

| Backend | When it is right | Cost |
|---|---|---|
| **`requests`** *(default)* | Today. The page is static, so a plain GET is sufficient. | None — `requests` is already in `requirements.txt` |
| **`selenium`** | If the Fed makes the calendar client-rendered, or starts refusing plain HTTP clients | An optional `pip install`, plus a chromedriver/Chrome pair that must match |
| **`auto`** *(what you get by default)* | Try `requests`; fall back to `selenium` | — |

`auto` also falls back when the page **fetches fine but parses to zero
meetings**, which is the signature of client-side rendering — precisely the case
Selenium exists for. That check lives in the CLI, which is where fetch and parse
meet.

When both backends fail, the error reports **both** causes. The `requests`
failure is usually the informative one (a 403, a DNS error, a proxy block), and
burying it under a driver error sends you debugging the wrong layer.

Whichever backend fetches, everything downstream is pure string-in/data-out, so
the parser is fully testable without either (§7), and `--html-file` runs the
whole chain against a saved page with no network at all.

**User-Agent.** The `requests` backend identifies itself as the tool it is and
links to the repository, rather than impersonating a browser. This runs once or
twice a year against a public page; there is no reason to be evasive about it.

## 4. The parsing contract

### 4.1 Page structure

The calendar page is organised as one panel per year, each containing one row
per meeting with a month heading and a day range. The shapes that matter:

| On the page | Meaning | Decision date |
|---|---|---|
| `January 28-29` | two-day meeting within one month | **29 January** |
| `April/May 29-1` | two-day meeting spanning a month boundary | **1 May** |
| `November 4-5*` | asterisk = Summary of Economic Projections | **5 November** |
| `March 3` | one-day meeting | **3 March** |
| `(unscheduled)` | ad-hoc meeting, historical only | **skipped** |
| `notation vote` | not a rate decision | **skipped** |

### 4.2 The decision date rule

**The decision date is the LAST day of the meeting.** Two-day meetings decide on
day two; the statement lands that afternoon. This is the single most important
rule in the parser and the one a hand-edit most often gets wrong — it is why
`config/fomc.yml` says "decision dates only (the *second* day of each 2-day
meeting)".

The month-boundary case (`April/May 29-1`) is where a naive parser silently
produces a wrong date: the second day belongs to the **second** month in the
`April/May` pair. The parser handles this explicitly and it is covered by a
test.

### 4.3 Output contract

`parse_fomc_calendar(html)` returns `list[FOMCMeeting]`, each carrying:

| Field | Meaning |
|---|---|
| `decision_date` | `datetime.date` — what goes in `config/fomc.yml` |
| `start_date` | first day of the meeting (`None` if unparseable) |
| `year` | calendar year of the panel it came from |
| `is_projection_meeting` | the asterisk — SEP/dot-plot meeting |
| `raw_label` | the source text, kept for debugging a bad parse |

Results are **sorted ascending and de-duplicated** on `decision_date`, matching
what `FOMCConfig.__post_init__` enforces.

## 5. Failure behaviour

The Fed will restructure this page eventually. Every failure mode below is
loud, because a scraper that silently returns fewer dates than it should is
worse than one that crashes — it would quietly shorten the modelled rate path.

| Condition | Behaviour |
|---|---|
| HTTP fetch fails (non-200, timeout, empty body) | raise `FOMCScrapeError` naming the status/cause; under `auto`, fall back to Selenium first |
| Both backends fail | raise `FOMCScrapeError` reporting **both** causes, `requests` first |
| Page fetch fails / driver missing | raise `FOMCScrapeError` with the remediation (install selenium, driver path) |
| Fetch succeeds but parses to 0 meetings under `auto` | retry once via Selenium — the signature of a page that became JavaScript-rendered |
| chromedriver/browser version mismatch | raise `FOMCScrapeError` naming the mismatch specifically — the raw Selenium message reads like the scraper is broken when it is a local toolchain problem |
| Zero meetings parsed | raise `FOMCScrapeError` — treated as a structure change, never as "no meetings scheduled" |
| A year panel parses to fewer than 6 meetings | warn loudly on stderr, keep going (the Fed holds 8/year; a partial year is normal only for the current and final years) |
| A day range is unparseable | warn with the raw label, skip that row, continue |
| `--check` finds missing dates | exit code 1, print the YAML block to add |
| `--check` finds config dates absent upstream | exit code 1 — a date the Fed no longer lists may have moved |

## 6. Interface

```bash
# Print every scheduled meeting the Fed lists (plain HTTP; falls back to Selenium)
python scripts/scrape_fomc_calendar.py

# Only meetings the config is missing, as a paste-ready YAML block
python scripts/scrape_fomc_calendar.py --missing-only

# CI / cron mode: non-zero exit when config and page disagree
python scripts/scrape_fomc_calendar.py --check

# Parse a saved page — no browser, no network. Debugging and testing.
python scripts/scrape_fomc_calendar.py --html-file tests/fixtures/fomccalendars.html

# Save the fetched page while parsing it (how you refresh the test fixture)
python scripts/scrape_fomc_calendar.py --save-html /tmp/fomc.html

# Limit to future meetings (what actually matters for the model)
python scripts/scrape_fomc_calendar.py --from-year 2027

# Pin the backend explicitly
python scripts/scrape_fomc_calendar.py --backend requests   # no browser, ever
python scripts/scrape_fomc_calendar.py --backend selenium   # force the browser
```

Exit codes: `0` success / no drift · `1` drift found in `--check` ·
`2` scrape or parse failure.

## 7. Testing strategy

**The parser and the `requests` backend are tested; the browser is not.** Tests
run against fixture HTML in `tests/fixtures/` and intercept HTTP with
`responses` (already a dev dependency), so the suite stays hermetic — no
network, no driver, no flake — consistent with the rest of this repo.

Covered:
- one-day, two-day, and month-boundary (`April/May 29-1`) meetings
- the projection asterisk
- unscheduled/notation rows are skipped
- ascending sort and de-duplication
- empty/garbage HTML raises rather than returning `[]`
- the diff logic that powers `--check`
- the `requests` backend: success, 403/404/500/503, empty body, the honest
  User-Agent, and fetch→parse end to end
- backend dispatch: `auto` uses `requests` on the happy path and never touches
  Selenium; `auto` falls back and reports both failures; explicit
  `--backend requests` never falls back; an unknown backend is rejected

> ✅ **The acceptance test has been run (2026-09-15).** This section used to
> carry a warning that the fixture was hand-built and the parser had never met
> the real page. That is discharged: `tests/fixtures/fomccalendars.html` is now
> a real capture, and the first live run found two genuine parser bugs, both
> fixed. See §11.
>
> Two fixtures now, deliberately — `fomccalendars.html` (real capture, carries
> the main assertions) and `fomccalendars_synthetic.html` (the old hand-built
> page, kept solely for `(unscheduled)` rows, which have aged off the live
> calendar). Neither contains a one-day meeting; `test_label_shapes` covers
> that shape with its own inline HTML.

## 8. Operational requirements

**The default path needs nothing extra.** `requests` is already a core
dependency, so `python scripts/scrape_fomc_calendar.py` works out of the box.

Selenium is **not** a pipeline dependency — the pipeline does not import it, CI
does not need it, and `requirements.txt` does not carry it. Install it only if
you need the browser backend:

```bash
pip install selenium>=4.15
```

The driver and browser are resolved in this order, first hit wins:

1. `--chromedriver` / `--chrome-binary` CLI arguments
2. `FOMC_CHROMEDRIVER` / `FOMC_CHROME_BINARY` environment variables
3. Selenium Manager's own auto-resolution (Selenium ≥ 4.6)

Headless by default; `--no-headless` to watch it run when debugging a parse.

### ⚠️ Verified state of the development container

Both a driver and a browser are present, **but they are not a compatible pair**:

| Component | Path | Version |
|---|---|---|
| chromedriver | `/opt/node22/bin/chromedriver` | **147**.0.7727.24 |
| Chromium | `/opt/pw-browsers/chromium-1194/chrome-linux/chrome` | **141**.0.7390.37 |

Driving them together fails with
`session not created: This version of ChromeDriver only supports Chrome
version 147`. Selenium Manager cannot rescue it either — resolving a matching
driver requires downloading from `googlechromelabs.github.io`, which the egress
policy also blocks.

**So the Selenium path has never been executed successfully — not against the
Fed, and not against a local page.** That is a much smaller problem now that
`requests` is the default backend and Selenium is only the fallback.

What *has* been verified here:

- the parser, the differ, and the YAML renderer, against fixture HTML
- the **`requests` backend end to end over real HTTP**, by serving the fixture
  from `python -m http.server` and running the CLI against it — fetch, parse,
  diff and YAML output all execute
- backend dispatch, including that `auto` never touches Selenium when
  `requests` succeeds, and that a failure of both reports both causes
- the CLI's `--html-file`, `--missing-only`, `--check` and exit codes
- the driver-launch error path, which produced exactly the specced
  `FOMCScrapeError` with remediation text

On any normal machine — a laptop with Chrome installed, or CI with network —
`pip install selenium` and Selenium Manager resolve a matching driver
automatically and none of this applies. And unless the Fed changes how the page
is served, the browser backend should never be reached at all.

To smoke-test the browser half anyway, point it at the fixture over `file://`:

```python
from pathlib import Path
from fred_pipeline.catalogs.fomc_calendar import (
    fetch_calendar_html_selenium, parse_fomc_calendar,
)

html = fetch_calendar_html_selenium(
    "file://" + str(Path("tests/fixtures/fomccalendars.html").resolve())
)
print(len(parse_fomc_calendar(html)))   # expect 19
```

If that prints 19, the browser wiring is sound and any later failure is the
network or the Fed's markup, not the driver.

## 9. What this does not solve

The Fed publishes ~2 years out, so even a perfect scraper cannot produce dates
that do not yet exist. As of 2026-08-13 the preliminary schedule runs through
**January 2028 only** — 2027 complete, 2028 with a single January meeting. The
scraper's value is that the *next* extension is a one-command update instead of
a manual retype, and that `--check` can tell you the moment the Fed publishes
more.

Suggested cadence: run `--check` from the same place that notices the 120-day
runway test failing. The two together mean the config cannot expire unnoticed.

## 10. Next: automating all of the above (spec008)

The sentence above is the weak point of this whole design, and it went
unnoticed until it was written down: *"the same place that notices the
120-day runway test failing"* **does not exist.** There is no such place.
The only thing that notices is
`tests/test_fomc_probability.py::test_fomc_meeting_dates_have_runway`, and
the only way it notices is by failing `pytest -q` — which
`.github/workflows/ci.yml` runs on every push to every branch and on every
pull request.

Measured 2026-09-15: the last configured meeting is `2028-01-26`, leaving
**498 days** of runway. That test therefore starts failing on
**2027-09-28**, and when it does it turns *every unrelated pull request in
this repo red* until someone hand-edits `config/fomc.yml`. The alarm is
right to exist; routing it through everyone else's CI is not.

Two more gaps of the same kind:

- **The parser has still never seen the real page.** §7's warning stands —
  `tests/fixtures/fomccalendars.html` is hand-built, and the live
  acceptance run it calls for has not happened. `config/fomc.yml`'s own
  provenance comment (2026-07-17) predates the scraper by a month, so even
  the shipped dates are not scraper-derived.
- **Nothing can tell "the Fed hasn't published further out yet" from
  "nobody has checked lately."** The config records provenance in prose
  comments, which nothing can read.

[`specs/spec008`](../../specs/spec008/README.md) covers all three. Its
shape, briefly:

| | |
|---|---|
| **Where it runs** | a scheduled GitHub Actions workflow — the only place in this toolchain with egress to `federalreserve.gov` at all (see §8: the dev container is blocked), and the only one whose cadence is time-based rather than activity-based |
| **On new dates** | opens a reviewable PR on a fixed branch with the dates inserted and provenance bumped — preserving §2's "a person reviews the diff" rule while removing the retyping |
| **On a moved date** | opens an issue, edits nothing — a vanished date must never be auto-removed |
| **On a structure change** | saves the HTML it actually received as an artifact and opens an issue with both backends' errors, so whoever picks it up has the page *as it broke* |
| **On shrinking runway** | escalates on its own at 270/120/45/0 days; silent above 270 so it never becomes noise |
| **The blocking test** | drops to a 45-day floor and stops being the alarm |

Decision 7 there also retires §7's fixture caveat permanently: successful
runs refresh the committed fixture from real markup, so it stops being a
hand-built guess.

**Phase 0 gates everything else and needs one command from any machine with
normal internet** — `python scripts/scrape_fomc_calendar.py --save-html
tests/fixtures/fomccalendars.html`, then eyeball the dates and commit the
capture. Automating a parser that has never met its input would just
industrialize a guess.

> ✅ **Phase 0 is done as of 2026-09-15.** See §11 — it found two real bugs,
> which is the entire argument for having gated on it.

## 11. The first live run (2026-09-15) — what it found

Run from a machine with egress to `federalreserve.gov`, which is what had
been missing. The parser worked on real markup, and produced **two meetings
that do not exist**:

| Parsed | Real source text | Why |
|---|---|---|
| `2027-01-26` | *"Note: A two-day meeting is scheduled for January 25-26, **2028**."* | right day-range, wrong year — the explicit `2028` in the sentence was ignored and the enclosing 2027 panel's year used instead |
| `2027-08-19` | *"Last Update: **August 19, 2026**"* (page footer) | a footer timestamp read as a meeting |

**Root cause, shared:** the last year panel runs to end-of-document, so it
swallowed the page's footer and notes; and the meeting regex was matched as
a *substring* of any line, so any prose sentence containing a month and a
number became a meeting.

`2027-08-19` is the more dangerous of the two. The Fed does not meet in
August, but nothing would have caught it — the per-year sanity check only
fires on *too few* meetings, never too many. Had spec008's automation
shipped before this run, its first action would have been opening a PR
injecting two fabricated meetings into a rate-path model.

**The fix** (`catalogs/fomc_calendar.py`), two parts:

1. `_MEETING_LINE_RE` — the inline-label fallback is now anchored to the
   whole line. A meeting row is a bare label (`January 28-29`); a sentence
   that merely mentions a month is not. This alone kills both phantoms.
2. `_ADVANCE_NOTICE_RE` — but anchoring also drops the Jan 2028 meeting,
   which appears *only* in that note and nowhere as a panel row. So the
   note is now parsed deliberately, in one narrow shape
   (`"...meeting is scheduled for <Month> <d>-<d>, <YYYY>"`), and **only**
   with an explicit year, which is what stops it re-admitting prose.

**What the run confirmed about the config:** `config/fomc.yml` was already
exactly right — all 12 future meetings match the page. That includes
`2028-01-26`, which the config had flagged as *"taken from secondary
reporting... confirm on the next calendar refresh"*. Confirmed now, against
the Fed's own note. `--check` reports in sync, exit 0.

**Two notes for whoever refreshes this next:**

- The 8-meetings-per-year assumption in `_PARTIAL_YEAR_WARN_THRESHOLD` held
  on every complete year 2021-2027.
- `published_through` (spec008 open decision #5) is now answerable: **the
  page does not state its horizon as a structured field.** The furthest
  meeting lives in that prose note, so anything reading the horizon depends
  on the same fragile sentence that produced one of these two bugs. Treat it
  accordingly.

## 12. In the pipeline run (2026-10-07)

`python -m fred_pipeline run` now records a `fomc_calendar` stage just before
Gold. Two behaviours, deliberately independent.

### The check: always on, offline

Reads only the local `config/fomc.yml` (no network, so no new way for a run to
fail) and records one of:

| Calendar state | Stage status | Run verdict |
|---|---|---|
| more than 120 days of runway (including the 121-270 "due" band) | succeeded | unaffected; "due" is logged only |
| 120 days or fewer | **warned** | `partial` (and so the run-alert email) |
| file missing, malformed, or expired | **failed** | `partial`; Gold still builds |

Why those thresholds: a `WARNED` stage makes the *whole run* `partial`, so
warning from 270 days out would make every run for months look degraded. The
weekly workflow already owns that nudge (it opens an issue at 270). The run
speaks up at 120, where the old blocking test used to, and the stage is
`required=False`, so even a `failed` calendar never costs you the other 50+
Gold tables.

The failure it exists for: **a missing or expired calendar used to produce
empty `gold.fomc_probability` / `gold.fomc_meeting_path` with a run that
reported success.** Now it is on the record.

The default path `config/fomc.yml` is **relative to the working directory**.
Running from anywhere but the repo root therefore finds no file, and the
message says so, naming the path it looked at and the cwd. Set
`FRED_FOMC_CONFIG_FILE` to pin it.

### The refresh: opt-in, can never stop a run

```bash
python -m fred_pipeline run --refresh-fomc-calendar
```

Before the check, fetches the Fed page (plain HTTP, 15 s timeout, never
Selenium) and adds newly published meetings to the file. Guarantees:

- **Fallback on anything.** Fetch, parse, validate or write failing, for any
  reason, leaves the file byte-identical, logs a warning, marks the stage
  `warned`, and the run continues on the calendar already on disk.
- **Additive only.** A configured date missing from the Fed page is reported,
  never removed (that usually means a meeting moved). A lost advance-notice
  sentence is reported as a *parser* problem and the file is not touched on
  that account.
- **Validated, atomic write.** The new text must load as a valid config and
  still contain every date the old one had, then replaces the file through a
  temp file and `os.replace`.

**It edits a tracked file and does not commit.** A refreshing run leaves
`config/fomc.yml` modified in the working tree for a human to review and
commit. On a deployment where the config is baked into an image or bundle, the
edit lives only until the next deploy; there the scheduled workflow's PR is the
durable path, and this flag is just a convenience.

The cost, stated plainly: it puts `federalreserve.gov` in the run's path. The
fallback means that costs a warning, not a failure, but it is a dependency the
default run does not have, which is why it is off by default.

Verified against the live Fed page on a scratch copy of the config with its
last two meetings removed: it restored both (2027-12-08, 2028-01-26). With the
network deliberately broken it left the file byte-identical and reported a
warning.

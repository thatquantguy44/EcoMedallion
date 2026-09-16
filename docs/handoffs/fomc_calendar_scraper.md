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

> ⚠️ **The committed fixture is hand-built to the structure documented in §4.1,
> not a capture of the live page.** It could not be captured from the authoring
> environment, where egress to `federalreserve.gov` is blocked by network
> policy. It exercises the parser's logic honestly, but it is **not** evidence
> that the parser matches today's real markup.
>
> **First live run is the acceptance test.** Run with `--save-html`, eyeball the
> parsed dates against the page, then commit the saved HTML over the hand-built
> fixture. Until that happens, treat a successful test run as "the logic is
> right", not "the scraper works".

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

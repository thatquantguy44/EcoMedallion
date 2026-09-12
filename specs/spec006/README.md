# Spec 006: Free Source Expansion — Evaluating New APIs and Scrapeable Data

Status: evaluation framework + ranked candidate list (no implementation).
IMF/OECD endpoints **live-verified 2026-09-12** (§5.1); Kenneth French
licensing still unverified.
Last verified: 2026-09-12
Primary owner: TBD
Target: decide which free sources are worth adding next, and on what evidence
Recommended first build: **OECD** (`DSD_STES@DF_CLI`) — see §7

## 1. Goal

The pipeline ingests 13 sources today. The question this spec answers is not
"what other data exists" — that list is effectively infinite — but **"which
free sources are actually worth the maintenance cost here, judged against a
rubric rather than enthusiasm."**

Two things make that question answerable rather than open-ended:

1. **This repo has a licensing gate with teeth.** `config/data_licensing.yml`
   + `fred_pipeline validate --licensing-review` **fail the build** while any
   source that permits redistribution sits at `review_status: provisional`.
   U.S. federal sources are exempt (public domain by statute, 17 U.S.C. 105).
   That gate is the single strongest filter on this question, and it bites
   hardest exactly where scraping is most tempting. A source that can't clear
   it is not a candidate, however good its data.
2. **This repo already has a scraper, and it encodes a philosophy.**
   `scripts/scrape_fomc_calendar.py` (spec: `docs/handoffs/fomc_calendar_scraper.md`)
   establishes the house pattern — and one rule in particular that any new
   scraping work must inherit, quoted from its own docstring: *"This tool never
   edits `config/fomc.yml`... a config driving a rate-path model deserves a
   human diff — so this prints and a person pastes."*

This document produces a rubric, applies it to concrete candidates, and ranks
them. It does **not** build anything.

**Verification caveat, stated up front.** The candidate assessments in §5/§6
are drawn from prior knowledge of these sources, **not** from live endpoint
checks performed while writing this spec. That distinction matters in this
repo: this session's own ECB work repeatedly found series that were
structurally plausible but did not actually exist, and the
`market_terminal_gold_views.md` handoff shipped ~15 FRED series IDs assembled
from documentation that had to ship `active: false` with a
`⚠️ VERIFY BEFORE ACTIVATING` header for exactly this reason. **Every
candidate below is a hypothesis to verify, not a fact.** §7 makes verification
the first implementation step, before any client code is written.

## 2. Current Coverage (what "additive" has to mean)

13 source clients in `src/fred_pipeline/sources/`: `bea`, `bis`, `bls`,
`census`, `ecb`, `eia`, `fred`, `ishares`, `sec`, `stooq`, `tiingo`,
`treasury`, `worldbank`.

By domain, with the gap that matters:

| Domain | Covered by | Conspicuous gap |
|---|---|---|
| US macro (broad) | FRED (~2,570 active series) | Very little — FRED is a superset of most US macro |
| US labor / prices | BLS, FRED | — |
| US national accounts | BEA, FRED | — |
| US fiscal | Treasury (FiscalData), FRED | — |
| US energy | EIA | — |
| US demographics / business | Census | — |
| Company fundamentals | SEC (XBRL `companyconcept`) | Bank-level regulatory data (call reports) |
| Euro area | ECB (20 active + candidates) | — |
| Global policy rates | BIS (36 countries) | Everything *except* policy rates |
| Global development | World Bank (annual, 37 series) | Higher-frequency global macro |
| Equities | Tiingo (total return), Stooq (price return) | **Factor returns**, positioning |
| Index membership | iShares holdings CSV | — |

**The honest read: US macro is saturated.** FRED alone covers most of what a
new US-macro source would add, and this pipeline already ingests thousands of
its series. The real gaps are (a) non-US, higher-frequency macro, (b) factor
and positioning data for the equity/ML Gold tables that currently have no
authoritative input, and (c) bank-level regulatory data. Candidates are ranked
against those gaps, not against "is this interesting."

There is no existing general "sources we could add" backlog to duplicate —
only `docs/catalog/ecb_candidate_flows.md`, which is ECB-flow-specific. This
spec is new ground.

## 3. Non-Goals

- **Paid or freemium-with-a-cliff feeds.** Nasdaq Data Link (ex-Quandl) is the
  cautionary case: much of what people remember as free has moved behind
  paid subscriptions. Anything whose free tier is a trial is out.
- **Sources requiring per-user OAuth or a human login.** The pipeline runs
  unattended; a source that can't authenticate from a config value or
  environment variable doesn't fit the `SourceClient` contract.
- **Anything whose ToS forbids automated access.** Not a judgment call to make
  at implementation time — it's a gate in §4.
- **Re-scoping ECB or BLS expansion.** Covered by `specs/spec002` and
  `docs/catalog/ecb_candidate_flows.md`; adding more series to an
  already-integrated source is a manifest edit, not a new source.
- **Building any client or scraper.** This spec evaluates and ranks. The
  chosen candidate gets its own implementation slice (§7).
- **Index membership scraped from third parties.** `sources/ishares.py`'s own
  docstring already names the problem: *"Index membership is licensed data
  with no good free API"* — the holdings-CSV route is a deliberate,
  documented workaround, and scraping a constituents list off a wiki or
  vendor page is a licensing regression from that, not an improvement.

## 4. The Rubric

### 4.1 Hard gates (fail any one → not a candidate)

1. **Licensing survives the governance gate.** Either public domain by statute
   (U.S. federal), or an explicit open license (CC BY etc.) a human can
   actually read at a `terms_url` and sign off on with their name in
   `reviewed_by`. "Probably fine" is `provisional`, and provisional blocks
   redistribution.
2. **ToS and `robots.txt` permit automated retrieval** at the cadence we'd
   use. For scraping specifically, this must be checked *and recorded*, not
   assumed from the absence of a blocking response.
3. **Authentication is automatable** — no key, or a key from config/env. (Note
   this repo's own precedent that a "keyless" source can still be blocked:
   BLS's flat-file server and SEC EDGAR both reject a User-Agent without an
   `@`, a live-verified finding from earlier this session.)
4. **Stable, addressable identifiers.** The `SourceClient` contract is
   `get_observations(series_id, ...)` → raw payload → `normalize()` → canonical
   Silver rows. A source whose data can't be named by a stable ID doesn't fit
   without inventing a synthetic ID scheme (which several existing clients do —
   `treasury` uses `<dataset>:<field>`, `worldbank` uses `<country>:<indicator>` —
   so this is surmountable, but it's design work, not free).

### 4.2 Scoring (for candidates that clear the gates)

| Criterion | Why it matters here |
|---|---|
| **Additive value** | Does it fill a §2 gap, or duplicate FRED? Duplication is not worthless (cross-source reconciliation is a real Gold table) but it's a much weaker case. |
| **Frequency & freshness** | World Bank's annual cadence is the reason global macro is still a gap despite being "covered." |
| **History depth** | Point-in-time correctness is a stated repo principle; a source with no history serves dashboards but not backtests. |
| **Structural fit** | Does it map to `SourceClient` cleanly? **SDMX sources score highest** — see §5.1. |
| **Maintenance fragility** | An API contract is a promise; an HTML page is not. Scrapers carry standing cost. |
| **Egress reality** | Live-verified this session: ECB's WAF blocks broad wildcard queries, and the build environment blocks FRED/BLS/BEA outright. Assume verification will be harder than it looks. |

## 5. Candidate API Sources

### 5.1 Tier 1 — SDMX sources (highest structural fit)

**This is the key insight of this spec.** The pipeline already has a working
SDMX client and, more importantly, a *discovery tool* for SDMX metadata:
`discover-ecb` (`src/fred_pipeline/catalogs/ecb_discovery.py`), with dataflow
inspection, dimension enumeration, and bounded candidate generation — plus
hard-won operational knowledge from this session (the `FREQ=B` vs `D`
convention trap, the "structurally valid ≠ actually published" problem, the
wildcard-query technique for finding what really exists). Another SDMX source
reuses that machinery and that experience rather than starting cold.

**✅ VERIFIED LIVE 2026-09-12** — IMF and OECD were probed end to end (structure
*and* data queries). Findings below are measured, not assumed. The probe
changed the recommendation: these two are **not** equivalent first builds.

| Source | Fills | Verified status (2026-09-12) |
|---|---|---|
| **OECD** (SDMX **2.1**) | Composite Leading Indicators, quarterly national accounts, G20/COICOP consumer prices, cross-country unemployment | **✅ Best first build — reuses existing machinery essentially as-is.** Base URL `https://sdmx.oecd.org/public/rest` (the legacy `stats.oecd.org/restsdmx` path is **dead — 404**). Returns `application/vnd.sdmx.structure+xml; version=2.1` — *the exact Accept type `ecb_discovery.py:24` already requests*. **`parse_dataflows_xml()` parsed all 1,546 OECD dataflows with zero modification** (its `_NS` map matches by namespace URI, so OECD's `structure:` prefix vs ECB's `str:` is irrelevant). Data query verified: `DSD_STES@DF_CLI` (Composite Leading Indicators) returned real US values (99.54 for 2025-06). Data comes back as SDMX-CSV close to the shape `sources/ecb.py` already parses. |
| **IMF** (SDMX **3.0**) | Global macro monthly/quarterly — COFER, BOP/IIP, International Liquidity, Labor Statistics, quarterly GDP | **✅ Alive, but a bigger build than OECD.** Base URL `https://api.imf.org/external/sdmx/3.0` (legacy `dataservices.imf.org` is **dead — connection refused**). Serves **SDMX 3.0 JSON** structures, *not* 2.1 XML — so `ecb_discovery.py`'s parser does **not** transfer; a new structure parser is required. Data query verified (COFER 2024 → real reserve values). **191 dataflows, of which only 77 have stable IDs — the other 114 are vintage-suffixed** (e.g. `MFS_FMP_2026_JAN_VINTAGE`, annotated `VINTAGE: "Vintage for 2026-M01"`), which rotate monthly and therefore fail rubric gate 4.1.4 (stable identifiers). Build against the 77 stable flows; treat the vintage ones as a separate, interesting point-in-time question (§9 #5). |
| **Eurostat** (SDMX 2.1) | EU data ECB doesn't republish | *Not probed in this pass.* Notable: this pipeline *already* consumes Eurostat data indirectly — `ECB:LFSI_PUB` runs on Eurostat's `EUROSTAT_LFS1` structure (found this session, including its `I9`-not-`U2` area-code convention). Given OECD's 2.1 XML parsed unmodified, Eurostat (also 2.1) is likely the same story — verify before assuming. |

### 5.2 Tier 2 — high-value, non-SDMX, clean APIs

| Source | Fills | Notes / risk |
|---|---|---|
| **NY Fed** (`markets.newyorkfed.org`) | Reference rates (SOFR/EFFR) **at the source** rather than FRED's mirror; primary dealer positions; repo operations; Survey of Consumer Expectations | Free, no key. Primary-dealer positioning is genuinely uncovered. Publishing-at-source also makes it a natural input to `gold.fred_source_reconciliation`. |
| **Bank of Canada Valet** | Non-euro G10 macro/rates beyond BIS's policy-rate-only coverage | Cleanest of the national-central-bank APIs: free, no key, well-documented, stable. Good template if other central banks follow (BoE, RBA, SNB). |
| **FDIC BankFind / FFIEC** | Bank-level regulatory data (call reports) — a genuine §2 gap | U.S. federal → public-domain, clears the licensing gate cleanly. Larger modeling question: bank-level panel data doesn't fit the one-series-per-ID shape as naturally. |
| **CFTC Commitments of Traders** | **Futures positioning** — nothing in the current 13 covers it | Free, public, weekly, bulk-downloadable. High analytical value and no licensing friction. Format is fixed-width/CSV bulk files rather than a series API — ingestion looks more like `ishares.py`'s CSV-explode pattern than a REST client. |

### 5.3 Tier 3 — evaluate only if a specific need appears

USDA NASS (agriculture; keyed, niche), OpenFIGI (identifier mapping — a
utility, not a time series), Alpha Vantage / Finnhub / Marketaux (already the
sibling `market_terminal`'s Tier-B live feeds; tight free quotas; mostly
duplicates Tiingo for prices), SEC *Financial Statement Data Sets* (bulk
quarterly ZIPs — a richer surface than the `companyconcept` API already used,
worth it only if company fundamentals become a priority).

## 6. Scraping — When It's Justified, and the House Rules

### 6.1 The standing bar

Scraping is a **last resort**, justified only when the data has real
analytical value *and* no API exists. It is not justified to avoid reading API
docs. Two costs are permanent: HTML structure breaks without warning (an API
contract is a promise; a page is not), and **scraped data is the hardest case
for the licensing gate in §4.1** — there's usually no `terms_url` granting
reuse, which means `provisional` at best, which means no external
redistribution.

### 6.2 The house pattern (already established — inherit it, don't reinvent)

From `scripts/scrape_fomc_calendar.py` + `catalogs/fomc_calendar.py`:

- **`requests` + stdlib `re` for parsing.** No BeautifulSoup, lxml, or scrapy
  anywhere in this repo — and `requests` is already a core dependency. Don't
  add a parsing dependency without a real reason.
- **Selenium is a lazy, optional fallback only** (`backend="auto"` tries
  `requests` first; `selenium>=4.15` is a commented-out, not-installed line in
  `requirements-dev.txt`), for pages that become JS-rendered.
- **A descriptive User-Agent, with a comment explaining why** — the existing
  code notes *"python-requests UA is sometimes refused; this says what it is
  and why."* Consistent with the BLS/SEC `@`-in-UA finding.
- **A saved-page fixture for offline tests** (`--html-file`, fixture at
  `tests/fixtures/`), so the test suite never depends on the live site.
- **A `--check` drift mode with a non-zero exit code**, so CI catches the page
  changing before a human discovers it via silently-empty output.
- **Never write automatically.** The scraper prints; a human pastes. For
  anything feeding a model, this is the rule.

### 6.3 Scrape/bulk-file candidates worth evaluating

| Target | Value | Assessment |
|---|---|---|
| **Kenneth French Data Library** (Dartmouth) | Fama-French factor returns, momentum, industry portfolios | **Strongest candidate in this section, by a clear margin.** This pipeline already ships `gold.equity_factor_attribution` and `gold.equity_factor_implied_return` — factor tables whose canonical input is exactly this dataset. It's published as static, versioned ZIP/CSV files on a stable academic URL, freely distributed for research — so it's closer to a bulk download than a scrape, and far less fragile than parsing HTML. **Verify the current license/attribution terms before treating output as redistributable.** |
| **Shiller / Yale** (CAPE, long-run series) | Multi-century equity and rate history | Static published spreadsheet, freely available, high history depth. Same "download, don't scrape" shape. Low fragility. |
| **Damodaran / NYU** (ERP, betas, industry stats) | Valuation inputs | Freely published spreadsheets, updated annually — low cadence, low fragility, moderate value. |
| Fed release pages (H.4.1, H.8) | Balance sheet detail | **Mostly redundant** — already on FRED, which is an API. Doesn't clear §6.1's bar. |
| University of Michigan sentiment | Consumer sentiment | Partially on FRED already; the full detail is behind a subscription. Not worth it. |

**Pattern worth naming:** the three best entries above aren't really scrapes —
they're *stable file downloads*. That's a meaningfully different risk profile
from HTML parsing, and the distinction should drive the design: a file
fetcher with a checksum and a fixture is far more durable than a DOM parser.

## 7. Suggested First Implementation Slice

**Verification before code** — the same wildcard/inspect discipline that
worked for ECB this session:

1. ~~Confirm the current base URL and one working request for **IMF** and
   **OECD** SDMX.~~ **✅ DONE 2026-09-12 — see §5.1.** Both are alive at new
   endpoints; both legacy endpoints are dead. The probe *changed the
   recommendation* (below).
2. Pull one **Kenneth French** file and confirm its current license terms.
   *(Still outstanding.)*
3. ~~Record findings in this spec.~~ **✅ DONE for IMF/OECD** (§5.1).

**Recommended first build — now evidence-backed: OECD, specifically.** The
original draft of this spec said "IMF or OECD" as if they were equivalent.
They are not. Verification showed:

- **OECD is SDMX 2.1 XML and `ecb_discovery.py` parsed its full 1,546-dataflow
  catalogue with zero code changes.** That is the single strongest structural-fit
  signal available, and it was measured rather than assumed. Its data also
  returns as SDMX-CSV close to what `sources/ecb.py` already handles, so the
  data client is likely a small delta too.
- **IMF is SDMX 3.0 JSON** — genuinely valuable (COFER, BOP/IIP, International
  Liquidity are real gaps), but it needs a new structure parser, and only 77 of
  its 191 dataflows carry stable IDs. It's a second build, not a co-equal first.

Concretely: start with OECD `DSD_STES@DF_CLI` (Composite Leading Indicators) —
verified working, genuinely additive (no CLI equivalent exists in the pipeline
today), and small enough to prove the path end to end.

Kenneth French remains the strongest *standalone-value* candidate and a good
parallel track, since it feeds Gold tables that already exist with no
authoritative input.

Follow the existing per-source checklist: new client in `src/fred_pipeline/sources/`,
a `config/data_licensing.yml` entry (honest `review_status`), a manifest
shipping `active: false` with the `⚠️ VERIFY BEFORE ACTIVATING` header, a
`docs/catalog/<source>.md` page (required — `tests/test_catalog_docs.py`
fails CI for an active source with no catalog page), and tests with recorded
fixtures rather than live calls.

## 8. Acceptance Criteria

- §5/§6 candidate rows carry a verified/assumed marker and a date — no
  unmarked assertions about endpoints nobody checked.
- Any source that reaches implementation has a `data_licensing.yml` entry
  before its first `active: true` series, and `validate --licensing-review`
  still passes.
- A new source's first manifest ships inactive; activation is a separate,
  deliberate commit (the pattern `df3e6ad` already followed for BLS/ECB).
- Any scraper ships with an offline fixture, a `--check` drift mode, and
  writes nothing automatically.

## 9. Open Decisions

**🔴 #1 — Is non-US macro actually wanted?** Tier 1 is built on the premise
that the §2 "global macro above annual frequency" gap is worth filling. If
the near-term consumer is `market_terminal` (US-centric macro + rates +
equities per its own module list), IMF/OECD may be lower value than Kenneth
French factor data despite scoring better structurally. **This ordering
should be confirmed against actual downstream demand before building.**

**🔴 #2 — Does positioning data (CFTC COT) matter enough to justify a
different ingestion shape?** It's the clearest "nothing covers this" gap, but
weekly bulk fixed-width files fit the `SourceClient` series model
awkwardly. Worth it only if positioning is genuinely wanted downstream.

**🔴 #3 — Licensing posture for academic data.** Kenneth French / Shiller /
Damodaran are freely published for research, but this pipeline's gate asks a
sharper question: may we *redistribute* derived Gold tables built on them? If
the answer is "internal use only," they can still be ingested — they just
need an honest `redistribution_allowed: false`, like `tiingo`/`stooq`/`ishares`
already carry. **Decide the posture before ingesting, not after.**

**🟡 #5 — IMF's 114 vintage dataflows: ignore, or exploit?** Verification
found IMF publishes most of its catalogue as month-stamped vintage flows
(`MFS_FMP_2026_JAN_VINTAGE`, annotated `historySettingType: FULL_HISTORY`).
Rotating IDs fail the stable-identifier gate, so the straightforward answer is
"build against the 77 stable flows and ignore them." But this repo treats
point-in-time correctness as a first-class principle and already carries
`realtime_start`/`realtime_end` vintage machinery — a source that publishes
explicit monthly vintages is an unusually clean PIT input. Worth a deliberate
decision rather than a default skip, though **not** in the first build.

**🟡 #4 — Should `docs/adding_a_source.md` exist?** `docs/deployment/deployment_runbook.md`
references it as the place to look when adding a source, but **the file does
not exist**. Whichever source is built next is the natural moment to write it
from the actual steps taken, rather than reconstructing them later.

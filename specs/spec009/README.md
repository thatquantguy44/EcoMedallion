# Spec 009: Money-Market Rate Feed for the Terminal's MMKT Optimizer

Status: **in progress — Phase 0 complete; Phase 0.5 partially complete; Phase 1 implemented across SQLite/Postgres/Spark, with Spark runtime verification pending.**
The 2026-10-18 live probe resolved all 14 configured series, and all 14 were
ingested into PostgreSQL and written to `gold.mmkt_rate_snapshot` at an explicit
2026-10-18 cutoff. The probe could not verify the quoting basis of either
commercial-paper series, so AC-006 remains open. Databricks/Spark persistence
has not been implemented or verified.

Last verified: 2026-10-18 (live probe, PostgreSQL refresh, and focused tests)
Primary owner: TBD
Target: give the money-market optimizer (QuantMeridian `MMKT`, InventoryBallast
`specs/0018-live-poc-showcase`, owner decision 2) rates it can price off without
ever mistaking a quoting convention for a yield or a hole for a number.

## 1. Goal

QuantMeridian's MMKT module allocates cash across T-bills, repo, the Fed RRP,
commercial paper and money funds. The owner decided (2026-10-17) that its real
rates come from **this pipeline's Gold layer** (Postgres `macro_medallion`,
reached through `MACRO_DB_URL`), with an explicit, user-selected synthetic `SIM`
set and **no silent fallback** — when the Gold DB is unreachable, or a rate is
missing or stale, the terminal shows `ERR`, not a plausible number.

This spec defines what the Gold layer must publish for that, and the work to get
there. It is deliberately narrow: **a rate snapshot**, not a market-data product.

### 1.1 The consumer contract

The terminal reads `gold.mmkt_rate_snapshot` and passes a **rate snapshot**
(series, value, as-of, run) into each MMKT solve request; the optimizer service
has no database dependency, stays deterministic, and records a snapshot hash with
each run. One row per configured instrument, **always** — a configured
instrument is never omitted:

| Column | Meaning |
| --- | --- |
| `instrument_id`, `label`, `instrument_class`, `tenor_days` | What it is (`config/mmkt_instruments.yml`) |
| `series_id` | The source series, for provenance (REQ-030 in InventoryBallast `0018`) |
| `quoted_basis`, `quoted_rate_pct` | The rate **as the source publishes it** |
| `normalized_basis`, `mm_yield_pct` | The same rate on **one basis**: simple ACT/360 money-market yield |
| `conversion` | Human-readable statement of the conversion applied |
| `observation_date`, `as_of_date`, `staleness_days`, `max_staleness_days` | Freshness |
| `status`, `status_detail` | `ok` \| `stale` \| `missing`, and why |

`stale` rows still carry their value, with the status saying not to price off it;
`missing` rows carry none. A consumer must check `status` — the terminal's
`ERR` rule is the consumer's half of this contract.

## 2. Current State (Evidence)

### 2.1 Source inventory and live MMKT evidence (2026-10-18)

The live probe report is captured at
`tests/fixtures/mmkt_live_probe_2026-10-18.json`. All 14 configured MMKT series
resolved on FRED as daily Percent series, and the probe confirmed the expected
discount basis for `DTB4WK`, `DTB3`, and `DTB6`, and the expected
bond-equivalent basis for `DGS1MO`, `DGS3MO`, and `DGS6MO`. The overnight,
policy, repo, and RRP titles do not state a basis; the probe accepts these as
unverifiable for configured `money_market` series. FRED's metadata also does
not state a basis for `DCPF3M` or `DCPN3M`; these two require manual confirmation
before their normalized yields are treated as basis-verified.

All 14 configured series were ingested successfully into PostgreSQL. The
snapshot was rebuilt from Silver with `as_of=2026-10-18`: 14 rows, all status
`ok`, with observations from 2026-10-13 through 2026-10-18. The October 19
IORB observation was excluded by the cutoff. `DCPN3M` was activated after the
live probe resolved it. The Treasury probe also captured the live auction
fields, including `auction_date`; no Treasury client change has been made yet.

The broader inventory below includes candidate series outside the 14-instrument
MMKT snapshot; those additional candidates were not validated by this probe:

| Need | Series | Manifest | Expected quoted basis |
| --- | --- | --- | --- |
| Overnight / policy | `SOFR`, `EFFR`, `OBFR`, `IORB`, `DFF`, `SOFR30DAYAVG` | `rates.yml`, `fed_funding.yml` | money-market ACT/360 |
| Repo | `TGCRRATE` | `fed_funding.yml` | money-market |
| Fed RRP | `RRPONTSYAWARD`, `RRPONTSYD` (volume) | `fed_funding.yml` | money-market |
| T-bills, secondary | `DTB4WK`, `DTB3`, `DTB6` | `rates.yml` | **discount** |
| Treasury CMT | `DGS1MO`, `DGS3MO`, `DGS6MO`, `DGS1` | `rates.yml` | **bond-equivalent** |
| Commercial paper | `DCPF3M` (90-day AA financial), `DCPN3M` (90-day AA nonfinancial) | `fed_funding.yml`, `money_market.yml` | **discount** (CP basis still needs manual confirmation) |
| Money funds | `WRMFNS` (weekly assets), `MMMFFAQ027S` (quarterly) | `money_banking.yml` | n/a (assets, not yields) |

Existing Gold views `benchmark_rate_board`, `funding_tape_daily`,
`funding_stress_daily` are not changed. **Correction to earlier notes:** `BGCR`
does not exist on FRED and the repo rate is `TGCRRATE` — this repo's own
`manifests/fed_funding.yml` records that, verified against live FRED
2026-07-17.

### 2.2 Gaps MMKT needs

| Gap | Why it matters | Where it lands |
| --- | --- | --- |
| Nonfinancial CP (`DCPN3M`) | Second CP point | Activated and ingested 2026-10-18 after the live probe resolved the ID; discount basis still needs manual confirmation |
| Treasury bill auction results | Primary-market rate and size | Phase 2 (client change) |
| Money-fund **yield, WAM, WAL, daily/weekly liquid assets** | REQ-017's liquidity floors and WAM caps need realistic fund parameters | Phase 3: SEC Form N-MFP client |
| Repo by tenor and collateral | Term repo is not on FRED | Phase 3: OFR Short-term Funding Monitor client |
| CDs, bilateral repo, bank deposits, per-issuer spreads | No free daily series | Not planned — licensed data, or an explicit `SIM` spread over a real benchmark |
| Other CP tenors | No free 1M/2M series known | Not planned |

## 3. The quoting-basis problem (why this is not just another board)

The sources disagree about what "the rate" means:

* `DTB*` and the Fed's CP series are quoted on a **discount basis** — `d`, ACT/360,
  on face value.
* `DGS*` constant-maturity yields are on a **bond-equivalent (investment) basis**
  — a simple ACT/365 yield on price.
* SOFR, EFFR, repo and the RRP award are **simple ACT/360** money-market rates.

Compared raw, a 3-month bill quoted at a 5.00% discount ranks *below* SOFR at
5.05% yet yields **more** once both are on one basis. A wrong basis fails nothing;
it silently mis-ranks instruments by basis points, which is the whole decision an
optimizer makes. The snapshot normalizes to simple ACT/360 and shows both numbers.

```text
discount         y = d / (1 - d * tenor / 360)
bond_equivalent  y = y_bey * 360 / 365                  (tenor <= 182 days)
money_market     y = y
```

Worked values, exact: a 91-day bill at a 5.00% discount is `36000/7109 %` =
5.0640034%; a 28-day bill at 4.00% is `9000/2243 %` = 4.0124833%. The two
formulas agree — a bill's bond-equivalent yield is `365 d / (360 - d t)`, and
scaling by 360/365 returns the discount formula — and
`tests/test_mmkt_rate_snapshot.py` checks that identity.

Limits, stated: `bond_equivalent` is refused beyond 182 days (a longer maturity
needs the compounded formula, which is not implemented, so `DGS1` is not in the
map); the ACT/365-versus-actual/actual leap-year effect is ignored (well under a
basis point at these tenors) and said so in `conversion`; `DGS1MO`'s
`tenor_days` is a nominal 30 and does not enter its conversion.

**The basis labels in `config/mmkt_instruments.yml` are taken from FRED's
published titles and must be confirmed by the probe** — the probe reads each
series' title/notes back, and fails on a disagreement. The live probe confirmed
the T-bill and Treasury CMT labels; FRED's text was silent for both commercial-
paper series, so those configured discount bases remain unverified.

## 4. Non-Goals

* Not a general market-data layer; only what MMKT prices off.
* Not changing `benchmark_rate_board`, `funding_tape_daily` or any existing view.
* No new licensed data (Bloomberg, Crane, iMoneyNet) — a separate adapter question.
* No wiring into SQLite/Postgres/Spark storage in Phase 0 (see Phase 1).
* No automatic manifest activation. `DCPN3M` was activated by explicit human
   approval after its ID resolved in the live probe; its basis remains a separate
   verification item.

## 5. Acceptance Criteria

| ID | Criterion | Status |
| --- | --- | --- |
| AC-001 | Discount and bond-equivalent conversions equal hand-worked exact values, and a bill's two quotes normalize to one number | **done** (`tests/test_mmkt_rate_snapshot.py`) |
| AC-002 | Every configured instrument yields a row; an uningested series is `missing`, a late one `stale`, with a reason — none silently dropped | **done** (same file) |
| AC-003 | `as_of` makes the snapshot point-in-time safe: later observations are never read | **done** |
| AC-004 | Every series named by `config/mmkt_instruments.yml` is declared in a manifest | **done** (same file; the `TGCR`/`TGCRRATE` lesson) |
| AC-005 | The probe fails on an unresolved id, a quoting-basis disagreement, wrong units/frequency, or staleness, and never treats silence as agreement | **done** (`tests/test_probe_mmkt_series.py`; logic only) |
| AC-006 | The probe has been **run live** and its report saved as a fixture; every configured series passes | **partial** — report captured; all IDs resolve with daily Percent data, but the CP discount basis is unverified and the probe flags both CP series |
| AC-007 | `gold.mmkt_rate_snapshot` exists in SQLite, Postgres and Databricks and matches the engine row-for-row | **partial** — SQLite/PostgreSQL parity passes; Spark writer, Delta DDL, schema-parity and value assertions are implemented, but the Spark test could not start locally because Java is unavailable |
| AC-008 | The Treasury client reads auction results keyed by their real date field, from a captured fixture | **open — Phase 2**; live probe captured the `auction_date` field, but client/fixture work remains |
| AC-009 | Each new source (N-MFP, OFR) clears `spec006` §4.1's hard gates, recorded in `config/data_licensing.yml` | **open — Phase 3** |

## 6. Phased Plan

**Phase 0 — done (this spec's branch).** `config/mmkt_instruments.yml` +
`gold_config/mmkt_config.py` (validated loader); `writer/mmkt_views.py::
compute_mmkt_rate_snapshot` (pure, no I/O); `manifests/money_market.yml`
(`DCPN3M`, activated 2026-10-18 after live ID resolution); `scripts/probe_mmkt_series.py`;
tests for all of it.

**Phase 0.5 — partial (AC-006).** The live probe was run on 2026-10-18 and
saved as `tests/fixtures/mmkt_live_probe_2026-10-18.json`. All 14 IDs resolved;
`DCPN3M` was activated and all 14 active series were ingested into PostgreSQL.
The probe exits nonzero because FRED's metadata does not state a quoting basis
for `DCPF3M` or `DCPN3M`. Manually verify the CP discount convention before
claiming AC-006 complete or relying on those normalized yields.

**Phase 1 — implemented; Spark verification pending (AC-007).** SQLite and
PostgreSQL storage wiring is implemented. PostgreSQL reuses the shared Gold
builder and schema; its integration test compares the MMKT snapshot with
SQLite row-for-row. The Spark/Databricks writer, Delta DDL, schema-parity test,
and persisted-value assertion are implemented. Focused Postgres/MMKT tests
passed on 2026-10-18, and PostgreSQL currently contains all 14 configured
instruments at an explicit 2026-10-18 cutoff. The local Spark test could not
start because this environment has no Java runtime; execute the Spark
integration job to complete verification.

**Phase 2 — open (AC-008).** The live Treasury probe captured both `record_date`
and `auction_date`, along with rate/yield fields. The Treasury client change
must use the captured auction dataset fields and fixture; no client or manifest
work has been completed yet. `docs/catalog/treasury.md` notes this is "a matter
of hand-writing a manifest entry" — true only when the date key matches.

**Phase 3 — new sources (AC-009).** SEC Form N-MFP (fund-level and class-level
yield, WAM, WAL, daily/weekly liquid assets; point-in-time by filing date like
`sources/sec.py`) and the OFR Short-term Funding Monitor (repo by tenor and
collateral). Each must clear `spec006` §4.1: licensing (both are U.S. federal
work, public domain by statute — to be confirmed at the terms page and recorded
with a `reviewed_by`), ToS/robots permit automated retrieval, automatable auth
(SEC requires a descriptive User-Agent *with an `@`*, per this repo's own
finding), and stable identifiers. **Not yet assessed live** — N-MFP's download
page and the OFR's exact mnemonics were not confirmed from this environment.

## 7. Risks

| Risk | Mitigation |
| --- | --- |
| A wrong basis label silently mis-ranks instruments | Probe reads FRED's own text back and fails on disagreement; silence is "unverifiable", never "match" |
| A mistyped id empties a table without an error (the `TGCR` incident) | `missing` rows are explicit; AC-004 cross-checks every id against the manifests |
| The terminal prices off a stale rate | `status = stale` plus `staleness_days`/`max_staleness_days`; the terminal's `ERR` policy |
| Config drifts from what the optimizer assumes | One snapshot hash per solve (terminal side) |
| Unverified work looks finished | Keep AC-006 partial until the CP basis is manually confirmed; keep AC-007 partial until Databricks/Spark is implemented and tested |

## 8. Open Decisions

1. **Stale values: emit or null?** Currently a `stale` row still carries its
   value, with the status saying not to price off it, so the terminal can show
   "last known" alongside `ERR`. The alternative — null the value — is safer for
   a careless consumer and loses the display. Recommended: keep as is.
2. **View or table?** A computed Gold *table* (like the others) versus a SQL view
   over silver. Recommended: table, for parity with `benchmark_rate_board` and so
   the snapshot is stamped with a run id.
3. **N-MFP granularity.** Fund-level, share-class-level, or category aggregates.
   MMKT's REQ-017 has per-fund AUM-share caps, which argue for fund level.
4. **DGS1MO's nominal tenor** (30 vs 28 days). Does not affect conversion; affects
   only MMKT's tenor bucketing.

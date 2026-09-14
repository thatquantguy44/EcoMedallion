# Pipeline performance handoff: Gold rebuild fixed + incremental; extraction is the next lever

**Status: Phases 1–2 done. Phase 3 (Incremental Gold) has 5 tables/groups
wired and the pattern is proven; the remaining candidate tables are
deliberately deferred (see "Phase 3: what's left, and why it's parked"
below) pending real profiling data. A new, likely-bigger lever — due-date
gating on extraction — is scoped in [`specs/spec007`](../../specs/spec007/README.md)
and not yet started.**

**Audience:** an agent working in **this** repo (`fred-bronze-to-gold-pipeline`).
Read this first. For Phase 3 continuation, read
[`specs/spec003/README.md`](../../specs/spec003/README.md) (the authoritative
design doc — Phase 3's "Status (2026-09-13)" note there has the full
rationale for every decision summarized here) before writing any code. For
the extraction-gating work, read `specs/spec007/README.md` first — it's a
design doc, not started, and needs a real decision on scope before code.

## What was found and fixed (Phases 1–2, both done)

1. **A correctness bug was blocking any real-scale measurement**: `gold_dim_date`
   gained three columns without a matching `_ADDED_COLUMNS` migration entry,
   failing every `gold` run against a pre-existing db. Fixed, with a
   regression test (`tests/test_pipeline.py::test_gold_failure_logs_the_real_error_not_noneType_none`
   also fixed a related "NoneType: None" logging bug that made this
   undiagnosable).
2. **`gold_fred_point_in_time` is now a SQL VIEW**, not a materialized table
   — it's a pure 1:1 mirror of `silver_fred_observation` with no independent
   data and no internal Python reader, so a view costs nothing to keep
   current and eliminates its rebuild step entirely. (This was flagged as an
   open question in spec003 §7 for a while; it's resolved now.)
3. **`_build_gold_inner()`'s ~18 `_compute_parallel`-dispatched engines**
   (the ECON dashboard, Curve Lab, regime/stats lab, FOMC, global views,
   etc.) now get their input pre-filtered to just their curated series via a
   `latest_by_series` index built once, instead of each engine independently
   scanning all of `latest` (tens of millions of rows) — mirroring the Spark
   backend's existing `_collect_latest()` SQL-pushdown pattern, which never
   had this problem.
4. `merge_silver`'s upsert now gates the update on a genuine `row_hash`
   change, not just a matching key — a prerequisite for Phase 3 below, not
   optional (see spec003 for why).

**Not yet done:** a clean, full-scale timing run against the real
`fred_local.db` to get final before/after numbers. Blocked twice this
session by this dev machine running out of free memory (a McAfee AV scan was
consuming ~30GB) — not a bug in the pipeline. Retry when the machine has
headroom; it's informational at this point, not blocking further work, since
the root causes above are already fixed by direct code inspection and the
correctness of the fixes is proven by the full test suite.

## Phase 3: Incremental Gold — in progress, this is what to continue

**The core problem this solves:** every `gold` run does a full `DELETE` +
rebuild of every Gold table from complete Silver history, even on a routine
run that only restated a handful of trailing observations per series.
**The naive fix doesn't work**: nothing in `Pipeline.run()` gates which
series get restated by staleness (`expected_update_frequency` is dead for
that purpose), so `restate_last_n` touches nearly every series on nearly
every run — "skip untouched series" would provide near-zero benefit. The
design that actually works is **row-range incrementality**: checkpoint each
table's expanding computation state per entity (series/spread/instrument),
and on a routine build only extend forward from the checkpointed frontier,
falling back to a full per-entity recompute on backfill, config change, or
first-seen.

### The established pattern (5 tables/groups done, all following this shape)

Done so far, in order, each on its own commit on `spec004-postgres-deployment-runbook`:
`afa0667` (checkpoint infrastructure + prerequisites) → `0c65080`
(`gold_curve_spread_daily`) → `3dad9b4` (`gold --full` CLI flag +
integration tests) → `b43f81d` (`gold_credit_spread_daily`) → `1d87eb5`
(`gold_funding_tape_daily` + `gold_funding_stress_daily`) → `8be0de5`
(`gold_series_correlation`).

For a table that's per-entity and causal/expanding (the common case):

1. **A resumable engine function** in `src/fred_pipeline/writer/terminal_views.py`
   or `regime_stats.py`, named `resume_<table>` — takes `(entity identity
   fields, new_points_past_frontier, prior_state_or_None, ...)`, returns
   `(new_state, new_rows_only)`. Built from the shared exact resumable
   primitives in `writer/features.py`: `resume_expanding_mean_std` (Welford,
   O(1) state) and `resume_expanding_percentile` (exact via a checkpointed
   sorted-value list + `bisect`, not approximate). Check whether the table's
   original `compute_<table>` function carries any *additional* sequential
   state beyond those two — `gold_curve_spread_daily`'s `inversion_run` (a
   consecutive-count) and `gold_credit_spread_daily`'s `change_bps` (needs
   the single prior value) both did; write a dedicated test proving a naive
   resume without that extra state would silently produce wrong output for
   the first point of a resumed chunk. If a table needs a genuinely new
   primitive (not built from the two shared ones) — `gold_series_correlation`
   did, for bivariate rolling correlation — check whether the original
   algorithm uses prefix-sum subtraction rather than direct windowed
   summation. Those are mathematically equal but **not bit-for-bit
   identical** (different floating-point cancellation); a resume built on a
   raw-value ring buffer will get the right correlation value while
   silently breaking the exact-parity contract. `resume_series_correlation`
   (`regime_stats.py`) keeps a ring buffer of *cumulative-sum snapshots*
   instead and reconstructs windows via subtraction, matching the original's
   arithmetic exactly — proven with a 30-trial random-data property test
   asserting exact equality, not `pytest.approx`. Read that function's
   docstring before building another prefix-sum-based resumable engine.
2. **A `_build_<table>` orchestration method** on `LocalWarehouse` in
   `src/fred_pipeline/io/local_store.py`, pulled out of the
   `_compute_parallel` dict entirely (checkpoint reads/writes must stay
   main-thread-only — `_compute_parallel`'s `ThreadPoolExecutor` workers
   never touch `self.conn`, which is `check_same_thread=True`). Per entity:
   compute a `config_hash` (hash of just that entity's own config fields, so
   editing one spread's YAML entry doesn't invalidate every other spread's
   checkpoint); skip entirely only if a checkpoint exists, the config hash
   matches, *and* none of the entity's watched series ids are in `touched`;
   otherwise decide append (new data is strictly past the checkpoint
   frontier) vs. full-entity recompute (backfill/config-change/first-seen,
   via a scoped `DELETE ... WHERE entity_col = ?`, never a table-wide
   delete). Batch all checkpoint writes into one call to
   `_write_checkpoints_batch` at the end, not one write per entity.
3. **`full: bool` wiring**: `_build_gold_inner`/`build_gold` already thread
   it through; a new table just needs its `_build_<table>` method to accept
   `full` and branch to the old full-DELETE-and-recompute-everything path
   (clearing this table's checkpoints via `_clear_checkpoints`) when `True`.
4. **Tests**: engine-level parity tests (`resume_X` output ==
   `compute_X` output, across arbitrary split points — see
   `tests/test_features_resumable.py` for the primitive-level property tests;
   `tests/test_terminal_views.py`'s `resume_curve_spread_daily`/
   `resume_credit_spread_daily`/`resume_funding_tape_entry` tests and
   `tests/test_regime_stats.py`'s `resume_series_correlation` tests for the
   per-table pattern — the latter also has the random-property test to copy
   if you build another prefix-sum-based engine), plus integration tests
   against the real `LocalWarehouse.build_gold()` entry point in
   `tests/test_local_store.py` (skip-untouched, append, backfill-vs-from-
   scratch, and — if the table has one — a downstream-consumer check, since
   two of the five tables done so far had another Gold table reading their
   output, which needed a small fix each time; see below).

**Watch for downstream consumers when removing a table from `_compute_parallel`.**
Both `gold_credit_spread_daily` and `gold_funding_stress_daily` were read by
`compute_recession_probability` later in `_build_gold_inner` via the
`computed[...]` dict the old full-rebuild-everything code produced. Removing
a table from that dict without checking for this breaks silently until the
full test suite is run — grep for the table's old `computed["..."]` key
before deleting the `_compute_parallel` entry. Since those consumers need
the table's *full current content*, not just one build's delta, the fix is
to read the table back from the DB after the incremental build (or, if the
value is already computed fresh in Python that build — as
`_build_funding_stress_daily` is — just use that return value directly).

**Not every table fits the per-entity pattern — know the exceptions:**

- **Snapshot tables** (one row per entity, not per date — no history to
  append): `gold_benchmark_rate_board`, the ECON dashboard's
  `macro_indicator_dashboard`/`sparkline`. Not worth making incremental;
  their full rebuild is already cheap (tiny output).
- **Cross-sectional composites**: `gold_funding_stress_daily` (done —
  deliberately left as an always-full recompute, not checkpointed; see its
  docstring in `local_store.py` for the reasoning) and `gold_macro_regime_daily`
  (evaluated, deliberately left alone — see below) and, at a larger scale,
  the macro PCA/factor-score tables and `macro_category_summary`. A single
  touched entity can change *other* entities' or dates' output, breaking the
  per-entity append/backfill split. Generally: recompute fully every time,
  cheaply, rather than force-fitting the checkpoint pattern.

### Phase 3: what's left, and why it's parked (evaluated, not just unstarted)

These aren't "the next tier to do the same way" — each was looked at and
found to be either a poor fit for the checkpoint pattern or of uncertain
payoff. Don't start on these without re-deriving (or refuting) the reasoning
below first; it's not a TODO list, it's a set of conclusions.

- **`gold_macro_regime_daily`** — evaluated and left as a full rebuild.
  `compute_macro_regime` emits one row *per date* only once every pillar has
  a live input that date, using an as-of/staleness lookup (`_asof`, a
  `bisect_right` carrying forward each input's last value within
  `max_staleness_days`). A single backfilled input can therefore change
  which value gets carried into *other, already-computed* dates — not just
  extend the series forward the way every table done so far works. Forcing
  this into the per-entity append/backfill split risks silent staleness.
  It's also already fast: pre-filtered to `regime_ids` by the
  `_select_series` fix (item 3 above), its own output is bounded by unique
  dates across a handful of config-bounded pillars/inputs (small), so the
  win from checkpointing it would be marginal relative to the risk.
- **`gold_series_lead_lag` / `gold_series_structural_breaks`** — not
  started, lower priority than it looks. Both re-scan the *entire* aligned
  history on every call (a cross-correlation ladder over ±max_lag, a
  Chow-test break-date scan, a CUSUM scan) with no incremental update path
  in the algorithm itself — the realistic optimization is "skip untouched
  pairs entirely, full-recompute touched ones," not true row-range
  incrementality. That's a real, much simpler change to make — but its
  payoff depends on the same fact that ruled out whole-series skipping for
  Gold generally: `Pipeline.run()` touches nearly every series on nearly
  every run, so most configured pairs would likely count as "touched" most
  of the time too. Get a real read on this from **spec007** below before
  spending time here — if extraction stops touching every series every run,
  *then* per-pair skipping for these two tables becomes worth doing, and
  should probably be built together with that change rather than before it.
- **`gold_recession_probability_daily`** (warm-started IRLS, `last_beta`
  carried forward) and **`gold_equity_total_return_index`** (running product
  from the ticker's first date) — same shape as the lead-lag/structural-
  breaks case: full-entity-recompute-on-any-touch is the realistic ceiling,
  same open question about whether "touched" is ever a small set in
  practice.
- **Macro PCA/factor-score tables and anomaly scores** — probably not worth
  it at all. They're cross-sectionally coupled by construction (touching any
  one feature series changes covariance/loadings for every other series from
  that point forward), the same category as `gold_funding_stress_daily` but
  at a larger, harder-to-bound scale. Revisit only if profiling after
  spec007 shows them as a real bottleneck.

## New scope: due-date gating on extraction (spec007, first slice shipped)

**The bigger lever, found while researching Phase 3. Status (2026-09-13):
a first implementation slice is done, behind an opt-in flag, not yet
re-baselined against real timing** (see `specs/spec007/README.md` §8) — a
pure `_series_is_due` date-math function, a `last_ingested_at_by_series`
warehouse query (which also surfaced and closed a SQLite/Postgres
index-parity gap), and both wired into `Pipeline.run()`/`run --skip-not-due`
with a new `RunStatus.SKIPPED_NOT_DUE` audit trail. Default behavior
(flag omitted) is unchanged. Still owed before this goes further: real
extraction timing with gating on vs. off (not done).

**✅ FIXED (2026-09-13) — the two high-priority findings from validating the
cadence→interval mapping against real publish calendars are resolved in
code** (`_CADENCE_MIN_INTERVAL` in `pipeline.py`; spec007 §5/§10 item 5 and
the "Cadence validation findings" subsection have the full evidence):

1. **`quarterly` (was 85 days, a correctness gap, not a tuning nit) →
   tightened to 25 days.** `GDP`, BEA's NIPA quarterly series, and BLS
   productivity are tagged `quarterly` in this repo's manifests, but none
   of them publish once per quarter — each gets three official revisions
   per quarter (advance/second/third estimates at ~30/55-60/90 days after
   quarter-end). Real gap between updates is ~25-35 days, not ~91. The old
   85-day interval would have silently **skipped the second and third
   revisions entirely** for a GDP-like series — stale data served for up
   to two months longer than intended. Resolved by converging `quarterly`
   onto the same interval as `monthly`, a deliberate safety-first choice
   (see spec007 §10 item 5 for the full reasoning and what would justify
   revisiting it).
2. **`monthly` (was 27 days, zero safety margin) → tightened to 25 days.**
   The real 2026 CPI release calendar has a minimum observed gap of
   exactly 27 days (Oct 14 → Nov 10) — identical to the old interval, not
   looser than it as the design intends. A single scheduling shift
   (holiday, agency delay) could have caused a missed print with no slack
   to absorb it.

`weekly` (6 → 5 days) and `annual` (360 → 180 days) got the same tightening
for smaller versions of the same margin problem. `daily`/`business_daily`
were confirmed already safe and left unchanged. None of this has been
re-baselined against real timing yet (see below) — these are correctness
fixes to the interval math, not a performance measurement.

**✅ Also fixed (2026-09-13): `expected_update_frequency` coverage was far
worse than documented.** Spec007 originally estimated ~490 of 3,003
manifest entries were missing this field — a grep-based count that
conflated the YAML key being *present* (mostly as `''`) with the field
having a *real* value. A corrected parse found only **164 entries had a
real value; 2,839 (94.5%) did not**, which would have made gating close
to a no-op regardless of how correct the intervals above are. Backfilled
via the new `scripts/backfill_expected_update_frequency.py` (maps each
entry's existing `frequency` code the same way `bls_discovery.py`/
`ecb_discovery.py` already do); all 3,003 entries now have a real value,
verified to change nothing else in the manifests.

**✅ Follow-up (2026-09-14), now fully closed: 121 of 170 `fred`/`annual`
entries were mistagged with the same silent-staleness risk as the
quarterly/GDP finding above.** Found in two passes. First, 13 entries in
`manifests/money_banking.yml` turned out to be FRED's annual-frequency
transform of two Fed releases that don't publish annually at all — the
**Z.1 Financial Accounts** (quarterly) and **H.8 Assets and Liabilities of
Commercial Banks** (weekly). Then a second pass checked the remaining ~120
entries against real release calendars for each source: **82** NIPA line
items in `national_accounts_extra.yml` (BEA's own documentation confirms
NIPAs update on the same quarterly advance/second/third cycle as GDP
itself, plus an additional July annual revision layered on top — the exact
GDP failure mode, just for every NIPA component); **16** in
`production_housing.yml` split between Fed G.17 Industrial Production and
Census New Residential Sales (both monthly) and FHFA's All-Transactions
House Price Index (confirmed quarterly-only); **9** BLS "usual weekly
earnings" series in `labor_extra.yml` (confirmed quarterly); and **1** BEA
international-transactions series (confirmed quarterly). All retagged
directly in the manifests, no code change. The one cluster checked and
found **already correct**: 12 Census/BEA regional population entries in
`regional_aggregates.yml` — the Census Population Estimates Program really
does publish one annual vintage, with revisions bundled into that same
release rather than scattered through the year. `fred`/`annual` dropped
from 170 to 49 systemwide. Full details and sources in spec007 §10 item 6.

By default, `Pipeline.run()` still restates *every* series passed to it on
*every* invocation — nothing consults each series' `expected_update_frequency`
(present in the `meta_fred_series` schema, unused for this purpose unless
`--skip-not-due` is passed). This is *why* whole-series skipping didn't work
for Gold (see above), but
the same fact applies just as much upstream, at the 42.6-minute extraction
stage spec003 originally wrote off as "external, rate-limit-bound, largely
un-fixable in code" — that framing assumed extraction has to touch every
series every time. If a monthly series isn't due for a week, pulling it
today is pure waste, independent of any rate limit. Fixing this compounds
with everything in this doc: fewer series touched per run means extraction
itself shrinks, *and* more of Phase 3's per-entity checkpoints get skipped
rather than appended-to, on both the tables already done and (per the
deferred section above) the tables gated on "how often is a pair actually
touched."

Full design questions (windowed vs. exact due-date tracking, where the
watermark lives, interaction with `restate_last_n`, `--full` semantics,
manifest-level overrides) are scoped in
[`specs/spec007/README.md`](../../specs/spec007/README.md). Rollout and
`--full` semantics are decided (§5/§10); the cadence→interval mapping is
now validated against real publish calendars and retuned to match
(§5/§10 item 5) — nothing left blocking `--skip-not-due` on interval
safety grounds, only on the still-owed real timing re-baseline above.

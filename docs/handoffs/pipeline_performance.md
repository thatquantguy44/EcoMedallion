# Pipeline performance handoff: Gold rebuild fixed + incremental; extraction is the next lever

**Status: Phases 1–2 done. Phase 3 (Incremental Gold) has 5 tables/groups
wired and the pattern is proven. The remaining candidate tables were
re-evaluated against current code on 2026-09-14 (analysis only, no real
profiling data — see "Phase 3: what's left, and why it's parked" below):
four stay deliberately parked, one (`gold_equity_total_return_index`) was
un-parked — it fits the established pattern directly and is ready to
implement next, no new primitive needed. A new, likely-bigger lever —
due-date gating on extraction — is scoped in
[`specs/spec007`](../../specs/spec007/README.md) and not yet started.**

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

**Not yet done: a clean, full-scale timing run against the real
`fred_local.db`.** Attempted 5 times this session (2026-09-13), all blocked
by this dev machine running out of memory — every attempt died at the exact
same point, right after `feature_transforms` finishes and before
`_compute_parallel` starts (never got a single `pf.*` timing line). This is
**not a bug in the pipeline** — see the diagnosis below — and it's
informational at this point, not blocking further Phase 3 work, since the
root causes are already fixed by direct code inspection and the correctness
of every fix is proven by the full test suite (including end-to-end
integration tests against real `build_gold()` runs on small fixtures).

Diagnosis, in case this recurs: this machine has 24GB RAM and, at the time
of the attempts, ~19.5GB already committed to other running apps (VS Code
alone: 4.4GB across 43 processes; Chrome: 2.9GB across 33) plus 12 days of
uptime with no reboot (accumulated memory fragmentation). Holding the full
34M-row `silver_fred_observation` and 20.5M-row `gold_fred_latest_observation`
tables as Python `list[dict]` simultaneously — which is original,
pre-this-session pipeline behavior, not something Phase 3 introduced —
plausibly needs 15-25GB of peak heap on its own, which simply doesn't fit in
the ~4.5GB of headroom that was actually free. Things ruled out along the
way, in order tried: McAfee AV (was suspected first, but a direct RSS
measurement during a later attempt showed McAfee using only ~155MB —
negligible; an earlier `top` reading of ~30GB had shown *compressed* memory,
not live usage, which was a misleading read at the time); this tool's Bash
sandbox (retried with `dangerouslyDisableSandbox: true` — died at the
identical point, ruling this out); `_compute_parallel`'s thread concurrency
(added `FRED_GOLD_MAX_WORKERS` — capping to 2 workers made no difference,
because the crash happens *before* any `_compute_parallel` task starts at
all, so concurrency was never the mechanism). **Resolved**: a full machine
restart (no other apps reopened first) freed enough memory (settled to
~14.5GB committed vs. 24GB total, vs. the ~19.5-22GB that had been committed
on every prior attempt) that a retry got past every previous crash point on
the first try. If this recurs, restart-before-reopening-apps is the fix that
actually worked here, not any pipeline-side change. One more thing worth
knowing if a rebuild runs long unattended: macOS **sleep pauses** (does not
kill) a long-running background process — the tell is large gaps between log
timestamps next to small reported per-stage durations. Wrap long background
runs in `caffeinate -i -w <pid>` to prevent this.

**Postgres is not a workaround for this**, and don't assume it is without
re-reading this: `PostgresWarehouse._build_gold_inner()`
(`src/fred_pipeline/io/postgres_store.py`) delegates straight to
`LocalWarehouse._build_gold_inner(self)` — same shared code, same
`list[dict]` materialization, same memory footprint, regardless of which SQL
engine is underneath. Switching backends would not have avoided the crash.

**A native PostgreSQL 18 server exists on this dev machine, separately from
this project's documented Docker Compose target — both are legitimate, but
they are two different things and it's easy to conflate them.** Found while
investigating the above, and since made into a genuinely working local
capability:

- A real, running Postgres 18 process (`/Library/PostgreSQL/18/bin/postgres`)
  was installed via the official EnterpriseDB macOS installer on 2026-09-12
  (per `/Library/PostgreSQL/18/installation_summary.log`, world-readable —
  check it directly rather than re-deriving any of this by probing ports).
  It runs under a dedicated `postgres` system user via a LaunchDaemon
  (`/Library/LaunchDaemons/postgresql-18.plist`, `KeepAlive: false` — it will
  not auto-restart if it ever stops), listening on **port 5432** (the
  standard default) under **superuser role `postgres`**.
- This has **no relationship** to this project's documented local Postgres
  target (`docker compose up -d postgres`, default DSN
  `postgresql://fred:fred@localhost:55432/macro_medallion` — port **55432**,
  role `fred`, database `macro_medallion`, per `docker-compose.yml`). The two
  ports happen to look similar; they are not the same. The Docker-based
  target has still never actually been started on this machine (the Docker
  daemon itself isn't running) — everything below is about the *native*
  install instead.
- **The native instance's `postgres` superuser password was unknown** (not
  set by the user, not recoverable by guessing common defaults — several
  attempts at that were correctly refused/blocked, and repeated `sudo`
  guesses risk an account lockout, so don't loop on that if you hit this
  again). It was reset via the standard, legitimate procedure — done by the
  user themselves running each command, never by an agent handling the
  user's actual macOS password: temporarily set the `local all all` line in
  `pg_hba.conf` to `trust` auth, `pg_ctl reload`, run `ALTER USER postgres
  WITH PASSWORD '...'` from an unauthenticated `psql`, then immediately
  revert `pg_hba.conf` back to its original auth method and reload again.
  This is a one-time recovery step, not something to repeat routinely.
- **Current state (resolved):** a `fred` role and `macro_medallion` database
  now exist on this native instance (port 5432), created via
  `PostgresWarehouse`, which auto-bootstraps the `meta`/`audit`/`bronze`/
  `silver`/`gold` schemas and translates this project's SQLite DDL to
  Postgres on `__init__` (`src/fred_pipeline/io/postgres_store.py`,
  `_bootstrap_schema()`). Verified directly via `psql` +
  `information_schema.tables`: 5 schemas, 63 Gold tables created. Point at
  it with `FRED_POSTGRES_LOCAL_DSN=postgresql://fred:fred@localhost:5432/macro_medallion`
  (note **5432**, not the Docker default 55432 — see `config/warehouse.yml`
  for the same note next to the actual config). The Docker Compose path
  (55432) remains the project's documented default for anyone without this
  specific native install already present; both are fine, they're just not
  interchangeable without changing the port.
- **Known gap, not yet fixed, flagged for whoever next touches
  `postgres_store.py`:** `_bootstrap_schema()`'s DDL-translation regex
  (`_TABLE_RE`) only matches `CREATE TABLE`, not `CREATE VIEW` — so
  `gold.fred_point_in_time`, which is a SQL VIEW as of the fix earlier in
  this doc, is silently missing from the Postgres schema entirely. Confirmed
  by inspection, not yet fixed (out of scope for the work that found it);
  either extend the regex to also translate `CREATE VIEW` statements, or
  special-case this one view.
- `PostgresWarehouse._build_gold_inner()` still delegates straight to
  `LocalWarehouse._build_gold_inner(self)` (same shared code, same
  `list[dict]` materialization) — using Postgres does not reduce the memory
  ceiling discussed above, it only changes where the data ends up.

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

**Re-derived against current code, 2026-09-14** (analysis only — no real
extraction/`--skip-not-due` data exists yet; see "New scope" below for why).
Four of the five original conclusions held or got a stronger justification;
one was wrong and is now unparked below.

- **`gold_macro_regime_daily`** — confirmed, leave parked.
  `compute_macro_regime` (`writer/regime_stats.py:84-180`) emits one row
  *per date* only once every pillar is live that date (`:152-153`), using
  an as-of/staleness lookup (`_asof`'s `bisect_right`, `:130-137`) over
  per-input expanding z-scores. A single backfilled input changes that
  input's z-score, which `_asof` then serves to every later date's
  composite — this is a correctness-shape problem (cross-date coupling in
  the algorithm), not a frequency one, so **it doesn't depend on spec007 at
  all**: even if extraction stops touching every series every run, this
  table still can't be safely checkpointed. `_select_series` pre-filtering
  to `regime_ids` confirmed at `io/local_store.py:1996-1998`;
  `config/regime.yml` has only 15 series — genuinely small, confirming the
  "marginal win" call.
- **`gold_series_lead_lag` / `gold_series_structural_breaks`** — confirmed,
  leave parked, for a reason independent of spec007. Both fully rescan
  aligned history every call: `compute_series_lead_lag`
  (`regime_stats.py:809-875`, full ±max_lag CCF + Granger) and
  `compute_series_structural_breaks` (`:699-806`) via `_chow_scan`
  (`:573-652`) and `_cusum_scan` (`:655-696`) — no cross-call state exists,
  so "skip untouched pairs, full-recompute touched ones" is the real
  ceiling, as stated. But `config/stats_pairs.yml` has only **8 pairs**
  total — even a perfect skip-if-untouched implementation caps the benefit
  at skipping ≤8 recomputations. The payoff ceiling is too low to justify
  the work on its own; spec007 data wouldn't change that conclusion, so
  don't gate revisiting this on it.
- **`gold_recession_probability_daily`** — confirmed, stronger reason than
  originally written, leave parked. Warm-started IRLS (`last_beta`
  carried forward, `ml/recession_model.py:359-367`) was the stated shape,
  but it understates the risk: `_forward_labels` (`:130-153`) only grants a
  date `t` a label once `add_months(t, h) <= last_date` (`:147`), where
  `last_date` is the newest USREC print in the input. Appending a **new**
  USREC print (not a backfill) advances `last_date` and newly labels
  boundary dates up to `h` months back — `compute_recession_probability`
  (`:257-...`) recomputes `train_dates`/`X_train`/`y_train` from
  `labels_by_h` fresh on *every* `obs_date` iteration (`:344-357`),
  including already-emitted historical ones, so those rows' `beta` and
  probability silently change too. Verified directly (not just by the
  research agent): this means even a pure forward append — not just a
  backfill — can retroactively change already-computed gold rows, the same
  cross-date-coupling family as `gold_macro_regime_daily`, and equally
  independent of the touch-frequency question spec007 answers.
- **`gold_equity_total_return_index`** — **REFUTED as grouped with the
  recession-probability case; un-park this, it's a good Phase-3 candidate.**
  Verified directly: `compute_equity_total_return_index`
  (`writer/equity_views.py:206-272`) is a running product per ticker
  (`tr_index *= 1.0 + tr`, `pr_index *= 1.0 + pr`, `:248-249`) driven only
  by `prev_close` carried forward (`:271`) — unlike the recession model,
  appending a new date for a ticker never touches that ticker's past rows;
  only a genuine backfill (e.g. a restated dividend) does, and that's
  exactly the backfill case the established per-entity pattern already
  handles. `trailing_12m_dividend` (`:252-257`) is a backward-only 365-day
  window via `bisect_left` over sorted dividend dates — the same shape
  `resume_series_correlation`'s prefix-sum ring-buffer technique already
  solves elsewhere in this codebase (see "established pattern" above), so
  no new primitive is needed: entity = ticker, sequential state =
  (`prev_close`, `tr_index`, `pr_index`) exactly like
  `gold_credit_spread_daily`'s single-prior-value shape, plus that one
  rolling-window primitive for the dividend sum.
- **Macro PCA/factor-score tables and anomaly scores** — mostly confirmed,
  leave parked, with a refinement. `compute_macro_factor_scores`
  (`ml/macro_pca.py:87-198`) and `compute_macro_anomaly_scores`
  (`ml/anomaly.py:86-165`) both carry Welford state (mean/`M2`,
  `macro_pca.py:137-158`) — the same primitive family as
  `resume_expanding_mean_std`, just matrix-valued — so the state itself is
  small and cheap to hold either way; the blocker is real cross-sectional
  coupling (backfilling one feature changes `mean`/`M2` for every factor
  from that point forward), the same category as `gold_funding_stress_daily`
  but at larger scale, as originally written. Refinement the original
  write-up missed: because *many* input features feed these *few* shared
  entities (factors), P(at least one touched) saturates fast — a
  skip-if-untouched scheme would actually perform *worse* here than for
  lead-lag/structural-breaks (item 2 above), not just "the same idea at
  larger scale." **`compute_equity_factor_attribution`**
  (`ml/equity_factor_attribution.py:271-401`) is a different case: it's
  *already* incrementally checkpointed per (ticker, window) via
  Sherman-Morrison rolling-window updates — its Phase-3 exclusion is
  inherited entirely from consuming the volatile `macro_factor_scores`
  output above, not from its own shape. Revisit it only once/if the PCA
  table's own coupling problem is solved; revisit the PCA/anomaly tables
  themselves only if real profiling shows them as a bottleneck (spec007 or
  otherwise — this one's payoff-vs-risk call, unlike items 1-3, could
  plausibly change with better data).

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

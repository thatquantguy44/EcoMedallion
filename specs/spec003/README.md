# Spec 003: Local Pipeline Performance (Gold Rebuild + Full Refresh)

Status: in progress
Last verified: 2026-09-09
Primary owner: TBD
Target: `python -m fred_pipeline gold` and `python -m fred_pipeline run` against
a production-scale local SQLite warehouse (`fred_local.db`)

## 1. Goal

Characterize and reduce the wall-clock cost of the two slowest local
operations — `run --local` (extract + Gold) and `gold --local` (Gold rebuild
alone) — against `fred_local.db` at its real size, not the small fixtures
`scripts/benchmark_pipeline.py` currently exercises. Land the highest-leverage
fix first (Gold's two largest tables bypass every existing optimization), then
use it to re-baseline before deciding what else is worth doing.

This spec was written immediately after diagnosing why a `gold --local`
rebuild against the real `fred_local.db` took roughly 50 minutes and still
failed (a separate schema-drift bug, already fixed — see §2). That
investigation is the evidence base below; nothing here is speculative.

## 2. Current Performance Footprint (Evidence)

Measured directly against this repository's `fred_local.db` on 2026-08-24:

| Fact | Value |
|---|---|
| `fred_local.db` file size | 31.8 GB (WAL mode) |
| `silver_fred_observation` row count | 32,102,970 |
| `gold_fred_point_in_time` row count | 32,102,970 (1:1 mirror of Silver) |
| `gold_fred_latest_observation` row count | 20,408,541 |
| `bronze_fred_api_response` row count | 14,208 |
| Gold tables rebuilt per `gold` run | 69 (`CREATE TABLE` statements in `local_store.py`) |
| `SELECT COUNT(*) FROM silver_fred_observation` | ~21s (full scan, semi-warm cache) |
| `run --local` (2,821 series, routine incremental refresh) | 2,554s (42.6 min) extraction, 2,732 ok / 89 failed (BEA API errors, external) |
| `gold --local` standalone rebuild | ~51 min before failing near the last table (schema-drift bug, now fixed — §2.1) |

### 2.1 A correctness bug was blocking measurement (fixed, not part of this spec's remaining work)

The `gold --local` run above failed with
`sqlite3.OperationalError: table gold_dim_date has no column named is_imm_date`.
Root cause: commit `0773630` added three columns to `gold_dim_date`'s DDL
(`is_imm_date`, `is_monthly_option_expiry`, `is_triple_witching`) but never
added a matching entry to `LocalWarehouse._ADDED_COLUMNS` — the existing,
already-established mechanism for migrating columns onto a database file
created before that column existed (`CREATE TABLE IF NOT EXISTS` is a no-op
against an existing table, so a new column silently never appears). Every
`gold` rebuild against a pre-existing local db has been failing on this table
since that commit landed. Fixed by adding the three columns to
`_ADDED_COLUMNS` (`src/fred_pipeline/io/local_store.py`), with a regression
test (`tests/test_pipeline.py::test_gold_failure_logs_the_real_error_not_noneType_none`)
covering the related logging bug that made the failure hard to diagnose in the
first place (`log.exception()` called outside the exception's live context in
`pipeline.py`, which printed `NoneType: None` instead of the real error).

This is called out here, not filed as a separate spec, because it's exactly
what made this performance investigation possible: without it, `gold` never
completed, so no full-scale timing existed to act on.

## 3. Non-Goals

- Migrating off SQLite to a different local engine (DuckDB, Postgres, ...).
  Out of scope; the Spark/Delta path is the production destination and
  already exists — this spec is about the local dev/backtest loop only.
- Reducing the *number* of active series or Gold tables. The footprint (2,900+
  series, 69 Gold tables) is a product decision, not a performance problem.
- Fixing the BEA `NIPA:T20404:*` extraction failures seen in §2 — those are an
  external API error (BEA's `Error retrieving NIPA data.`), unrelated to
  pipeline performance.
- A general rewrite of the Gold-building engine. The recommendation below is
  targeted: fix the two tables that dominate row count and currently take the
  *slowest* available path, then re-measure before doing anything broader.

## 4. Root-Cause Findings

`LocalWarehouse._build_gold_inner()` (`src/fred_pipeline/io/local_store.py`)
opens with:

```python
silver = self._read("silver_fred_observation")   # 32.1M rows -> list[dict]
```

`_read()` is `cursor.fetchall()` followed by `[dict(row) for row in ...]` — a
32.1 million-object Python allocation before any Gold work starts. From
there, two of the biggest tables are built entirely in pure Python:

- **`gold_fred_point_in_time`** is a 1:1 copy of `silver_fred_observation`
  (same row count, renamed/reordered columns, nothing else). It is currently
  produced by a Python list comprehension over all 32.1M rows, then written
  via `LocalWarehouse._insert()`, which builds a Python tuple per row
  (`_encode()` per cell) and calls `executemany`. This is a pure data
  passthrough that never needs to leave the SQLite engine.
- **`gold_fred_latest_observation`** (20.4M rows) is built by
  `fred_pipeline.data.transform.latest_by_observation()` — a pure-Python
  dict-keyed groupby over all 32.1M Silver rows, followed by
  `sorted(latest.values(), ...)` over the resulting 20.4M rows, then written
  through the same slow `_insert()` path. This is a `GROUP BY` /
  `ROW_NUMBER() OVER (...)` query, done instead as an all-in-memory,
  single-threaded Python operation.

A polars-accelerated write path already exists
(`LocalWarehouse._insert_frame`, selected via `_gold_feature_impls()`) and is
real — it measurably skips the per-row dict/tuple overhead `_insert()` pays
(see the `gold_polars` module docstring and
`tests/test_gold_polars_parity.py`). But it is only wired to four downstream
builders: `gold_fred_macro_feature_daily`, `gold_fred_feature_transforms`,
`gold_fred_curve_spread`, `gold_fred_revision_stats`. The two largest tables
by a wide margin — `point_in_time` and `latest_observation`, together over
52 million rows, versus everything else combined — go through neither the
polars path nor a set-based SQL path. They are very likely the single biggest
share of the ~51-minute `gold` run.

SQLite's own tuning is **not** the bottleneck: `LocalWarehouse.__init__`
already sets `journal_mode=WAL`, `synchronous=NORMAL`, a 64 MB page cache, and
256 MB mmap — a reasonable configuration for a file this size. (An ad hoc
read-only connection opened outside the app, used to gather the table counts
in §2, showed SQLite's un-tuned defaults — 2 MB cache, `synchronous=FULL` —
which is not what the real pipeline connection runs with. Ruled out.)

`scripts/benchmark_pipeline.py` / `docs/benchmarking.md` (added in commit
`2e142fa`) benchmark stage proportions against a handful of series written to
a throwaway `/tmp/benchmark.db`. That is useful for relative stage timing but
structurally cannot see this bottleneck — it never runs against a
Silver table anywhere near 32M rows. Any performance work here needs its own
measurement step for that reason (§6).

## 5. Proposed Approach

Ordered by expected impact-to-effort ratio. Each phase should be validated
against the real `fred_local.db` (or a full-size copy), not the benchmark
tool's small fixture, since that's what has hidden this bottleneck so far.

### Phase 1: Set-based rebuild for the two dominant tables

- Replace `gold_fred_point_in_time`'s Python list-comprehension + `_insert()`
  with a single `INSERT INTO gold_fred_point_in_time (...) SELECT ... FROM
  silver_fred_observation` executed directly by SQLite. No Python
  materialization of Silver is needed for this table at all.
- Replace `latest_by_observation()` + `_insert()` for
  `gold_fred_latest_observation` with an equivalent SQL query (`ROW_NUMBER()
  OVER (PARTITION BY series_id, observation_date ORDER BY realtime_start
  DESC)` filtered to `rn = 1`, or a `GROUP BY` + correlated join, whichever
  profiles faster) inserted directly via `INSERT INTO ... SELECT`.
- `silver = self._read(...)` is still needed for the pure-Python downstream
  transforms that consume it as `list[dict]` — but once the two dominant
  tables no longer need it materialized *twice more* (once for `pit`, once
  inside `latest_by_observation`), the remaining single pass is far cheaper.
- Output must stay byte-identical to the existing behavior — write it the
  same way `gold_polars` proves parity with the pure-Python spec
  (`tests/test_gold_polars_parity.py`): a test that runs both paths against
  the same fixture and asserts equal rows.

### Phase 2: Re-baseline

- Re-run `gold --local` against the full `fred_local.db` and record new
  timing next to the §2 table. This determines whether Phases 3–4 below are
  still worth doing, or whether Phase 1 alone closes the gap.

### Phase 3: Incremental Gold (if Phase 2 shows it's still needed)

Every `gold` run currently `DELETE`s and fully rebuilds all 69 tables from
complete Silver history, even though a routine `run` only restates the
trailing ~90 observations per series (`config.restate_last_n`). Silver rows
already carry `run_id`/`ingested_at`; a Gold rebuild could scope itself to
series touched since the last successful Gold build and leave untouched
series' Gold rows in place, falling back to a full rebuild only on explicit
request (e.g. after `replay`, or a new `--full` flag on the `gold` command
mirroring `run --full`).

### Phase 4: Parallelize independent table builds (if Phase 2 shows it's still needed)

With Phase 1 removing the single largest sequential cost, the remaining ~65
smaller tables are still built one at a time in one connection. Several are
independent of each other once `silver`/`latest` are ready; map the real
dependency graph and evaluate running independent branches concurrently
(SQLite allows one writer, so this likely means parallelizing the compute
step and serializing only the final `_insert` calls).

## 6. Benchmarking Gap To Close Alongside This Work

Extend `scripts/benchmark_pipeline.py` / `docs/benchmarking.md` with a
`gold`-only mode that runs against a full-size local db (or a synthetic one
built to the same row-count order of magnitude) rather than only the small
default series set. Without this, a future regression on this exact path —
someone adding a table build that Python-materializes Silver again — has no
way to be caught before it reaches a database this size.

## 7. Explicitly Out of Scope For This Pass, Flagged As An Open Question

`gold_fred_point_in_time` is a full duplicate of `silver_fred_observation` —
same row count, most columns unchanged. Whether it needs to be a materialized
table at all, versus a SQL view over Silver, is a real data-modeling
question: a view would remove roughly half of Gold's row-writing volume
outright, but changes the read contract for anything already querying
`gold_fred_point_in_time` directly (index behavior, `ATTACH`-ability from
Spark/Delta parity checks, etc.). Worth a deliberate decision before Phase 1
lands, not a default assumed here.

## 8. Suggested First Implementation Slice

1. Phase 1 (`gold_fred_point_in_time` as `INSERT ... SELECT`;
   `gold_fred_latest_observation` as a window-function query), with a parity
   test against the current pure-Python output.
2. Phase 2 re-baseline, written back into this spec's §2 table.
3. Decide from the re-baseline whether Phase 3, Phase 4, both, or neither are
   worth doing — do not build them speculatively.

Progress on 2026-09-09:

- Phase 1 is implemented on the `spec003-performanceupgrade` branch:
  `LocalWarehouse` now rebuilds `gold_fred_point_in_time` and
  `gold_fred_latest_observation` with set-based SQLite statements before
  loading Silver/Latest rows for downstream Python engines.
- A local parity regression test covers the SQL output against the previous
  pure-Python latest-observation behavior.
- Phase 2 is still pending: run a full-size `fred_local.db` re-baseline and
  record the before/after timing here before deciding on incremental Gold or
  further parallelization.

## 9. Follow-Ups

- Confirm whether the 42.6-minute extraction stage (§2) is genuinely rate-
  limit-bound (external, largely un-fixable in code) versus leaving headroom
  in per-source `--source-workers` / `--source-rate-limits`; use the
  benchmarking tool to check whether extraction time scales as expected with
  those knobs before assuming it needs its own project.
- Revisit whether `config.restate_last_n` (default 90 observations, applied
  uniformly regardless of series frequency) is inflating per-run Silver churn
  more than necessary for daily series — a secondary contributor to
  extraction volume, not a Gold-rebuild driver.

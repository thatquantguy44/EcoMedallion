# Spec 010: Next Pipeline Performance Pass

Status: **Phase 0 implemented; Phase 1 equity pilot integrated and correctness-tested** (2026-10-18).
No representative large-seed speedup has been measured yet; the fixture smoke
is a harness check, not a production baseline. Live API and Spark measurements
remain unverified.
The due-date parity change on `perf/enable-due-gating-spark-postgres` is the
starting point; its Spark and PostgreSQL integration tests still need a runner
with working services.

## Goal

Reduce the elapsed time and peak memory of a routine Bronze → Silver → Gold
run on SQLite, PostgreSQL, and Databricks without changing point-in-time
results, release freshness, or the behavior of `--full`. Choose work from
measured stage costs rather than from total runtime alone.

## Evidence and current limits

| Finding | Evidence | Consequence |
| --- | --- | --- |
| Due-date gating already skips most low-frequency series on a routine day | `docs/handoffs/pipeline_performance.md` records a 204-series sample: 94.5s → 39.1s and 120 skips; the 87.6% full-manifest skip estimate is extrapolated, not measured | Keep the default gate and measure its cost on each backend; do not count the extrapolation as a production result |
| Local full Gold build materializes large inputs | The same handoff records `read_silver` 391.2s and `read_latest` 114.6s on a 34M-row Silver / 20.5M-row Latest dataset; `_build_gold_inner()` holds both as Python `list[dict]` | This is a large fixed cost and a peak-memory risk; PostgreSQL shares this orchestration |
| Two Gold stages are already prepared for incrementality | The handoff records `equity_total_return_index` at 234.8s and `recession_probability` at 141.8s; `resume_equity_total_return_index()` and `resume_recession_probability()` exist but are not wired to checkpoints | Integrate only after proving touched-entity detection and exact output parity |
| PostgreSQL Silver upsert is row-oriented | `PostgresWarehouse._insert()` uses `cursor.executemany()` for upserts, including `merge_silver()` | Benchmark a staging-table `COPY` plus set-based merge at realistic row counts and conflict ratios |
| Spark does extra work during ingestion and planning | `merge_delta()` calls `source_df.count()` before each merge; the new due-date lookup aggregates `MAX(ingested_at)` across Silver | Measure the action and grouped scan before changing them; repeated full-table scans may erase part of the extraction savings |
| Benchmark guidance had drifted from the code | The old harness used a read-only connection as the warehouse, `Environment.dev`, and a nonexistent `audit_etl_stage` table | Repaired in Phase 0; fixture output is validated, and old timings are labeled historical in `docs/benchmarking.md` |

The old full Gold measurement was ~3250s of active stage time. The handoff
projects ~2030s after the measured z-score stage improvement, but does not
record a new full run. Do not treat the projected total as a measured baseline.

## Phase 0: Reliable baseline and regression harness (first slice)

1. Make `scripts/benchmark_pipeline.py` construct a real `LocalWarehouse`
   (and optionally `PostgresWarehouse`) through the same configuration path as
   the CLI. Use `Environment.DEV`. Read stage durations from
   `FredPipeline._stage_tracker` or a supported structured audit record,
   never from a table that does not exist. Preserve the run's actual selected,
   due, succeeded, and changed-row counts in JSON output.
2. Support three comparable workloads: an unchanged routine run, a small
   restatement with changed rows, and an explicit `--full` backfill. Use a
   copied seeded database or replayed source responses for repeatable local
   tests; record a separate live-API benchmark only when quotas permit.
3. Record wall time by `plan`, `extract`, Bronze/Silver write, Gold table,
   release calendar, and persistence; peak RSS; rows read/written; API calls;
   due/skip counts; backend and data size. Enable `FRED_GOLD_STAGE_TIMING=1`
   for local Gold stage comparisons. Report median and range across at least
   three repeats on the same hardware and input snapshot.
4. Update `docs/benchmarking.md` to show commands and JSON fields the repaired
   harness actually supports. Label its current example timings as historical
   or illustrative until re-measured.

**Status: met for the SQLite fixture path (2026-10-18).** The documented
three-repeat unchanged smoke completed with three distinct run IDs, 2 selected,
2 due, 2 succeeded per run, zero changed Silver rows, all pipeline tracker
stages, 49 Gold timing records per run, and a 0.138s median (0.134–0.254s
range). These tiny-fixture timings validate the harness only; they are not a
production performance claim. Restatement and `--full` fixture workloads also
pass focused tests. PostgreSQL changed-row counts are reported as unavailable
where the backend exposes only submitted merge rows; Spark and live API
measurements have not been run. Source-call counts are logical client method
calls, not low-level HTTP attempts/retries. Keep correctness tests separate
from timing thresholds.

## Phase 1: Integrate the existing resumable Gold engines

**Pilot status (2026-10-18):** `gold_equity_total_return_index` is now
checkpointed per ticker on SQLite and PostgreSQL. Tests cover no-op skipping,
forward append, backdated dividend restatement, config-hash invalidation,
`full=True` reset/reseed, and interrupted-build rollback/recovery on both local
backends. The large-seed performance gate is still pending, so this records
correctness integration, not a measured speedup. `gold_recession_probability_daily`
remains unintegrated.

Wire `gold_recession_probability_daily` into the existing
`gold_incremental_checkpoint` and `gold_build_watermark` pattern from
spec003. Skip an entity only when its watched Silver inputs have no changed
rows and its configuration hash matches. Append from a checkpoint only for
strictly forward data; backfills, corrections at/before the frontier, config
changes, and `gold --full` rebuild the affected entity. PostgreSQL currently
has no `build_gold(full=...)` argument, so thread that flag through its adapter
as part of this phase. Write output and checkpoint in the same database
transaction. Apply the shared orchestration to SQLite and PostgreSQL;
measure Spark separately because its Gold builder does not use the local
checkpoint path.

**Remaining acceptance:** for each pilot table, exact row parity with a full
rebuild for no-change, forward append, revision, backfill, config edit, and
interrupted-build recovery on both local backends. Benchmark each stage and
end-to-end Gold on the same large seed. Keep only changes that improve the
target stage materially (suggested gate: at least 20% median) without increasing
peak RSS or changing output. The equity pilot has correctness coverage on these
paths on SQLite and PostgreSQL; the performance gate and recession-probability
integration remain open.

## Phase 2: Reduce full-history reads and peak memory

Inventory each Gold table's actual Silver/Latest columns, series, and history
window. Prototype a read plan that fetches only needed columns/series for
bounded tables and streams long histories by series for engines that can
consume them. Avoid holding the complete Silver and Latest `list[dict]` at
the same time; do not change a table's computation order or point-in-time
semantics. PostgreSQL must use a bounded cursor/iterator rather than a
client-side `fetchall()` in the streaming path. Preserve the current full
build as a reference for result comparison.

**Acceptance:** exact Gold output parity on a fixture containing revisions,
missing values, and multiple sources; measured reduction in peak RSS and in
the combined `read_silver` + `read_latest` time on a large seed. Report row
counts and any added database reads. Do not ship a stream conversion that
trades lower memory for a slower end-to-end build without an explicit reason.

## Phase 3: Backend-specific ingestion and planning experiments

| Experiment | Design and correctness rule | Measurement |
| --- | --- | --- |
| PostgreSQL bulk Silver merge | `COPY` into a temporary staging table, then one `INSERT ... SELECT ... ON CONFLICT DO UPDATE ... WHERE EXCLUDED.row_hash IS DISTINCT FROM target.row_hash`; retain the existing transaction and source/key semantics | Compare 10k and 100k rows at 0%, 50%, and 100% conflicts with `executemany`; report rows/s, lock time, WAL bytes, and peak RSS |
| Spark merge action count | Pass the already known `len(rows)` from `SparkWarehouse.merge_silver()` instead of a separate `DataFrame.count()` action when the caller has it; retain an optional count for generic callers | Compare Spark job count and Silver merge wall time with identical rows; verify the returned source-row metric |
| Spark due-date state lookup | Time the `MAX(ingested_at) GROUP BY series_id` scan on production-sized Delta. If significant, maintain a small per-series change watermark after the Silver merge commits; a crash between commits may cause an extra fetch but must never advance state ahead of Silver | Compare plan time and data scanned, including first run and restart/recovery; prove state cannot make an overdue series appear not due |

The staging-table path must keep the row-hash no-op guard added by the
current branch: an unchanged observation must not advance `ingested_at`,
since both due-date gating and incremental Gold use it as a change watermark.
Never advance the due-date watermark merely because an API returned no new
observation; that could postpone a release that arrives shortly afterward.

## Rollout and decision rules

- Re-run the baseline with `--no-skip-not-due` and the default gate on the
  same seed when comparing extraction, and keep `--full` separate. Report
  series skipped, changed Silver rows, and Gold work so a fast run is not
  mistaken for a run that silently did less required work.
- Land Phase 0 first. Implement the next phase only if its measured stage is
  material on the target backend; use one branch/PR per phase for review.
- Keep parity checks on SQLite, PostgreSQL, and Spark/Delta where the code
  path changes. A skipped or unstarted integration job is not a passing test.
- Preserve current conservative cadence intervals and source rate limits.
  Tune them only with release-calendar evidence and quota measurements.

Related design: `specs/spec003/README.md` (incremental Gold),
`specs/spec007/README.md` (due-date gating), and
`docs/handoffs/pipeline_performance.md` (historical timings and caveats).

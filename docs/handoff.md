# Active Work Pickup

Last verified: 2026-10-26.

**Continue on branch `spec014-run-batched-persistence`**, based on the current
`origin/main`. This is the active workstream for
[Spec 014: Run-Batched, Series-Clustered Persistence](../specs/spec014/README.md).

## Current state

- The standard, default-safe configuration remains
  `persistence_write_mode: per_series`; operators can opt into
  `run_batched` with configuration or `FRED_PERSISTENCE_WRITE_MODE`, and can
  immediately revert without a code change.
- In `run_batched` mode, results are collected before the three data phases:
  one raw/Bronze append, one cleaned/Silver merge containing only DQ-passing
  series, and one DQ-result append. Empty batches make no writes.
- Batch write failures complete every buffered series as failed, record the
  failed phase, preserve counts for earlier successful phases, then
  best-effort persist final audit state before re-raising.
- SQLite and PostgreSQL final audit persistence now replace one run's series
  rows with a bulk insert rather than a transaction per series.
- Spark write preparation now range-repartitions and sorts batched raw,
  cleaned, and DQ-result data by series-oriented keys. It never uses
  `partitionBy(series_id)`.
- PostgreSQL runtime proof (2026-10-26): run
  `4aac45fad0a34b8c8bfc987cd22a43b1` used `run_batched`, default due-date
  gating, `--no-gold`, and `--no-ecb-assets`. It completed its data flush in
  605.0 seconds: 250 succeeded, 35 failed, and 2,611 were skipped as not due;
  it persisted 259 Bronze rows and 1,933 DQ rows. The failures were isolated
  upstream responses/quotas and DQ rejects, not a batch or PostgreSQL writer
  failure. This is a scheduled-workload smoke test, not a `--full` backfill or
  a Gold benchmark.

## Next task

Run the Spark/Delta integration job in an environment where the pinned
PySpark/Delta dependencies are available, then add an end-to-end dual-mode
parity fixture that proves identical Bronze, Silver, DQ, and final audit
records for `per_series` and `run_batched`. Capture real file/commit metrics
with a measured buffer limit before proposing `run_batched` as an environment
default. Do not change the shared default from `per_series` until that proof
is recorded.

The authoritative acceptance criteria, failure-recovery contract, and
configuration details are in [Spec 014](../specs/spec014/README.md). The
older [Dagster handoff](handoffs/dagster_orchestration.md) remains historical
context only; it is not the next task for this branch.

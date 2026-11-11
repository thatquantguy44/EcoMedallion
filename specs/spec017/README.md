# Spec 017: Pipeline performance reports

## Objective and approved scope

Implement the five-part design approved in chat: a shared timing collector,
complete section coverage, reliable concurrent measurements, visual alerts,
and persisted comparable-run history. This is one reporting capability with
three components: collection → storage/comparison → rendering. Existing stage
verdicts remain authoritative; timings do not change ingestion policies.

Questions answered: where did processing time go, which dependency/engine was
slow, how much time was spent waiting/retrying, and is this run comparable to
previous runs? Both combined CLI runs and direct library runs are supported.

## Architecture and boundaries

- Extend the existing Python standard-library decorator and stage tracker.
- Use monotonic nanoseconds for durations, UTC for timestamps, unique nested
  span IDs, explicit thread context propagation and bounded buffers.
- Instrument startup, planning, source fetch/queue/wait/retry, normalization,
  Bronze/Silver/DQ, FOMC, Gold engines/read/write/commit, release calendar, ECB
  discovery/download/signature/parse/reconciliation/publish and reporting.
- Persist additive audit tables on SQLite/PostgreSQL/Delta, with a local JSONL
  journal and JSON/HTML run artifacts as a fallback. Telemetry is best effort
  and cannot change return values, exception propagation or committed data.
- Never record function arguments, URLs, credentials, payloads or raw exception
  messages. Allowlist scalar identifiers/counters only.
- Compare only matching backend, load mode, selected-series fingerprint,
  due workload, source mix and worker settings. Require at least three prior
  successful comparable runs; display missing baselines honestly.
- Send the combined summary after ECB. Keep the existing ECB health alert.
  Delivery timing is audited after sending, outside processing duration.
- No new dependencies, live email, production refresh, vendor edits or unrelated
  schema repairs. Additive audit schemas are within the approved persistence scope.

## Commands and structure

Python >=3.10; pytest; repository Ruff/pre-commit hooks. Source lives in
`src/fred_pipeline/`, tests in `tests/`, schema definitions in `sql/` and
`io/local_store.py` (also used by PostgreSQL bootstrap). Operator documentation
belongs in `docs/handoffs/`.

```python
@timed("gold.inflation")
def build_inflation(rows):
    return compute_inflation(rows)

with span("silver.merge", source="bea") as measurement:
    measurement.count("rows", warehouse.merge_silver(rows))
```

Focused verification:
`python -m pytest -o addopts='' -q tests/test_performance.py tests/test_alerting.py tests/test_cli_ecb_assets_default.py`

Full verification: `python -m pytest -o addopts='' -q`.
Format/lint: `pre-commit run --files <changed-files>`.

## Acceptance and verification

1. Decorators/context blocks share one collector and preserve behavior for
   success, caught failures, cancellation and asynchronous functions.
2. Repeated and concurrent spans retain identity/parentage; total elapsed time
   is never computed by summing overlapping children; bounded sampling and
   dropped journal events are disclosed.
3. Completed spans flush from the orchestration thread, including partial
   failures; database failures leave usable file artifacts and cannot fail ETL.
4. Combined alerts include ECB outcome and startup/Gold/FOMC durations. Direct
   library calls still send one summary. Early failures produce partial reports.
5. Text and HTML show stages, timeline/duration bars, source request percentiles,
   retries/waits, slowest operations and comparable-run trends. Escaping and
   Outlook-compatible tables/inline styles are tested; preview is inspected.
6. Persistent snapshots are idempotent, exclude the current run from baselines,
   and distinguish full/incremental, skipped and unchanged work. Delivery
   timing appears in the durable report after the attempt.

## Build order

Collector tests/core → storage tests/adapters → section instrumentation →
complete lifecycle → report visuals/history → failure and full-suite validation.
The approved chat design supplies the requirements and implementation approval.

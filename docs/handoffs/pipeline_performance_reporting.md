# Pipeline performance reporting (Spec 017)

Last verified: 2026-11-11

The combined `run` command now measures startup, planning, source extraction,
normalization/DQ, Bronze/Silver writes, FOMC, Gold engines, the forward release
calendar, audit persistence and signed ECB eligible-assets polling. Its final
summary is prepared after ECB. Direct `FredPipeline.run()` and
`run_from_manifest()` calls retain one summary per invocation.

## Operator controls

```bash
python -m fred_pipeline run --env dev \
  --performance-dir data/performance --performance-detail

# Render a demonstration without APIs, database access or email delivery:
python scripts/preview_performance_report.py --out data/performance/example.html
```

`--performance-detail` (or `FRED_PERFORMANCE_DETAIL=1`) retains individual HTTP
attempt, queue and wait events in addition to aggregates. Coarse measurements
and source statistics are enabled by default. `--performance-dir` overrides
`FRED_PERFORMANCE_DIR`; the default is the gitignored `data/performance` directory.
`FRED_PERFORMANCE_ENABLED=0` disables the collector. `FRED_CODE_REVISION` identifies
a packaged deployment without Git metadata; otherwise the checkout's commit is
recorded. Existing `FRED_GOLD_STAGE_TIMING=1` still enables Gold timing *logs*;
it is no longer necessary to collect Gold measurements in reports.

Existing alerting configuration still controls policy, recipients, transports,
and whether the run JSON is attached. This feature does not enable email or
change routes. The existing ECB health alert is preserved and delivered after
processing; it precedes the combined summary.

## What the report means

- **Elapsed processing** starts at the outermost command/library boundary and
  freezes after data stages and final audit persistence, before rendering and
  notification delivery. It is not the sum of child durations.
- The timeline uses monotonic offsets from that boundary. Nested operations
  are inclusive and workers overlap. Source fetch windows span the first through
  last fetch; throttle/backoff and worker durations are accumulated elapsed
  time, not CPU time. Request p50/p95 exclude local throttle/backoff waits and
  include unsuccessful HTTP attempts. Rows mean normalized observations,
  including observations later rejected by DQ.
- Series outcomes remain distinct from whole-run stage outcomes. A Gold,
  required ECB, or final audit failure is visible even if all series succeeded.
  Skipped/not-due series and unchanged ECB polls remain explicit.
- Render/delivery durations are available in the final durable JSON/audit
  snapshot. An email cannot contain its own completed delivery duration. The
  standalone HTML is updated after delivery.
- Comparison history uses up to 30 prior **successful** matching runs and
  excludes the current run. At least three are needed for a median comparison.
  Matching includes backend, entrypoint, selected-series fingerprint, source
  mix, due count, actual full/restate load mix, worker/rate settings, Gold/FOMC
  flags, persistence mode, normalized-row volume band (powers of two), and the
  effective ECB mode/release workload. The code revision is recorded but not a
  filter, allowing a new implementation to be compared with its predecessor.
  The trend displays at most the latest 12 matched runs.

## Durable records and failure behavior

Each measured invocation produces `<run-id>.json`; runs reaching reporting also
produce `<run-id>.html`, and measured spans append to a JSONL journal. Early
configuration/preflight failures may have only the file snapshot. Journals
contain start/finish events and `span_updated` amendments for stage outcomes
changed after completion; audit storage upserts the most recent event per span
ID. A killed process can leave `running` rows/events;
these must not be interpreted as successful completed stages.

SQLite tables are `audit_run_performance` and `audit_run_span`. PostgreSQL and
Delta use `audit.run_performance` and `audit.run_span` (under the configured
catalog for Delta). `run_performance` stores matching metadata, the complete
bounded summary and delivery status; `run_span` stores parent IDs, offsets,
durations, outcomes and allowlisted identifiers/counters. Audit schemas are
additive. No observational or Gold data are migrated by this feature.

Workers append to a locked queue and never use the warehouse connection.
Only the collector's owner flushes it. Progress flushes batch 256 events or
five seconds; Delta uses 512 events or 30 seconds to avoid a MERGE per series.
Coarse stage boundaries and finalization force a flush. Pending events and
completed-span samples are bounded at 4,096; source/operation percentile samples
retain the last 2,048 values, with at most 512 aggregate groups. The summary
also retains 128 coarse stage records and the ten slowest operations. Normal
journals/database history contain all retained events, while JSON attachments
contain bounded recent samples. Overflow and affected sinks are disclosed as
incomplete telemetry. A database failure stops repeated flush attempts; the
journal remains available and final storage is retried. File or telemetry
failures cannot change an operation's result or undo ingestion.

No arguments, URLs, provider payloads, request parameters or raw exception
messages are collected. Only allowlisted identifiers/counters and exception
types enter performance spans. The existing operational alert's stage-error
policy is unchanged. History and artifacts are retained until the operator
applies the deployment's audit/file-retention policy; no automatic purge runs.

Example PostgreSQL investigations:

```sql
SELECT run_id, status, processing_seconds, started_at
FROM audit.run_performance ORDER BY started_at DESC LIMIT 30;

SELECT name, source, status, duration_seconds
FROM audit.run_span WHERE run_id = '<run-id>'
ORDER BY duration_seconds DESC LIMIT 20;
```

## Verification and implementation references

The follow-up fixture audit on 2026-11-11 passed the complete suite: **1,810
tests**, **eight optional skips**, and **no failures**. The four former baseline
failures are resolved: the three `test_dim_series_schema_parity.py` tests now
locate the Spark schema through the AST, and the report catalog includes
`mmkt_rate_snapshot`. The six timing/reporting modules have **99.28% combined
statement/branch coverage**. See the [test coverage and reproduction commands](../../tests/performance/README.md)
and [saved synthetic fixtures](../../tests/fixtures/performance/README.md).

Failing-first regressions also verified three runtime fixes: invalid decorator
attribute callbacks cannot interrupt work, the aggregate-group cap preserves
completion events, and stage warnings applied after a flush reach durable audit
storage on the next flush.

Focused tests cover fake clocks, nested/repeated spans, copied thread contexts,
worker-stage ownership, queue timestamps, cancellation/async functions, failed
callbacks/sinks, bounded buffers, native SQLite/PostgreSQL upserts, Delta source
deduplication, source canonicalization, final audit failures, reused pipelines,
CLI → persistence → deferred alert flow, HTML escaping and missing baselines.
The demonstration HTML is inspected in Chrome; Outlook layout follows the
existing table/inline-style renderer. A live Outlook mailbox and a Databricks
Delta cluster are not available in the development test environment; Delta
SQL/explicit-schema behavior is verified at the adapter boundary, not claimed
as a live cluster proof. No live provider refresh or email is needed for QA.

API references: [Python performance counter](https://docs.python.org/3.12/library/time.html#time.perf_counter_ns),
[thread context copying](https://docs.python.org/3.12/library/contextvars.html#contextvars.copy_context),
[Delta MERGE semantics and source deduplication](https://docs.delta.io/delta-update/#operation-semantics),
and [Spark explicit DataFrame schemas](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html).

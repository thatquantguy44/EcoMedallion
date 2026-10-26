# Spec 014: Run-Batched, Series-Clustered Persistence

Status: **Draft for review; specification only. No write-path behavior has changed.**

Date: 2026-10-26

## 1. Objective

Make a normal multi-series pipeline run persist three logical, run-scoped
batches instead of issuing Bronze, Silver, and DQ writes once per completed
series. The design preserves the existing per-series extraction and quality
decisions while making Delta files appropriately sized and ordered for later
reads.

The current pipeline extracts concurrently, but `_finish_series()` immediately
writes one Bronze row, conditionally MERGEs that series' Silver rows, and
appends that series' quality results. This creates many small Delta commits and
files when a run has many small or low-frequency series.

In `run_batched` mode the pipeline will:

1. Extract, normalize, and quality-check every due series independently.
2. Retain every completed series' raw row, normalized rows, DQ report, and
   terminal series outcome in one in-memory run buffer.
3. After collection completes, make three logical persistence flushes:
   Bronze/raw, Silver/cleaned, and quality/audit.
4. Before each Delta flush, create a deliberately repartitioned and
   series-ordered DataFrame so a run produces well-sized files rather than a
   file fragment per series.

The quality boundary is unchanged: every series is checked separately, and a
series with a failing quality report is written to Bronze and represented in
quality/audit results, but contributes **no rows** to cleaned Silver. It
remains a failed series in the run result.

This is primarily a Spark/Delta physical-layout improvement. The same run
buffer and batch interfaces may reduce statement overhead in PostgreSQL and
SQLite, but those backends must not claim Delta file-layout benefits.

## 2. Verified baseline

Repository inspection on 2026-10-26 established:

| Concern | Current behavior | Required behavior |
| --- | --- | --- |
| Extraction | Source-aware thread pools return futures out of order; the main thread handles each completion. | Retain concurrency and per-series fault isolation; buffer completed outcomes rather than immediately persist. |
| Bronze | One `write_bronze([bronze_row])` call per successfully extracted/normalized series. | One run-scoped Bronze append. |
| Silver | One `merge_silver(silver_rows)` call only after that series passes DQ. | One run-scoped MERGE containing rows only from DQ-passing series. |
| DQ | One `persist_dq(run_id, report)` call per report. | One quality/audit flush with all DQ reports and final run/series audit rows. |
| Incremental audit | PostgreSQL and SQLite persist live run/series state after each future; Spark does not advertise that capability. | Suppress incremental audit only in `run_batched`; persist one final audit snapshot. |
| Delta layout | `append_rows()` and `merge_silver()` create a DataFrame directly from a single series' rows, with no layout shuffle or sort. | Repartition and order every run batch; do not use high-cardinality `partitionBy(series_id)`. |

No part of this spec changes normalizers, quality rules, Silver natural keys,
vintage semantics, source clients, Gold calculations, or due-date policy.

## 3. Buffered outcome and quality contract

Introduce an internal typed buffered outcome populated after one future
finishes. It contains the source/series/load identity, raw Bronze row,
normalized Silver rows, complete `QualityReport`, terminal `EtlSeriesRun`, and
exception metadata where applicable. It is not a durable table or a cross-run
cache. The buffer preserves manifest order for result/audit presentation even
when futures complete in another order. No worker thread writes a warehouse.

Per-series behavior remains exactly:

| Outcome | Bronze | Silver | DQ/audit | Series status |
| --- | --- | --- | --- | --- |
| Extraction or normalization exception | No constructed raw row if construction did not complete | No rows | Failed audit record; no fabricated DQ | `FAILED` |
| DQ passed | One raw row | Its normalized rows, through existing idempotent merge rules | Passed DQ and final audit | `SUCCEEDED` |
| DQ failed | One raw row | **No rows** | Failed DQ and final audit | `FAILED` |
| Not due | No write | No write | `SKIPPED_NOT_DUE` final audit record | `SKIPPED_NOT_DUE` |

Bronze remains the raw system of record. A DQ failure never discards an
otherwise retrieved raw response. The Silver collection is assembled solely
from reports whose `passed` value is true; a batch-level success must not
re-admit a failed series. Per-series write-count fields become final only after
the applicable batch succeeds; buffering alone cannot mark a series successful.

## 4. Three logical persistence phases

After every extraction future has completed and results are buffered, flush in
this order:

1. **Raw phase** — append all Bronze rows once.
2. **Cleaned phase** — MERGE all Silver rows from DQ-passing series once.
3. **Quality/audit phase** — append flattened DQ rows once and persist final
   `etl_run` plus all `etl_series_run` rows as one run-scoped audit operation.

“Three runs” means three logical, run-scoped write phases, not one cross-table
transaction. Delta commits are atomic per table, not across Bronze, Silver, and
audit. The backend must expose batch operations so each destination table gets
at most one write invocation per phase; the quality/audit phase may require
separate table-level statements for `data_quality_result`, `etl_run`, and
`etl_series_run`.

Gold remains after the Cleaned phase because it reads Silver. The final
quality/audit phase happens after Gold/stage status is known, preserving a
truthful final snapshot. No incremental per-series audit write occurs in
`run_batched` mode.

If any phase fails, the pipeline must not silently switch to per-series writes
mid-run. It records the phase failure and preserves table-level atomicity. A
successful Bronze phase can legitimately exist when a later Silver or audit
phase fails; the recovery runbook must say so. The operator may choose the
configuration-only legacy mode on the next run.

## 5. Delta layout and file sizing

Do not use `partitionBy("series_id")`: its high cardinality would create a
directory/file problem. “Organized by series” means cluster records during the
write shuffle, with source and series as leading keys.

For every non-empty Spark batch:

- create one DataFrame from the complete in-memory list;
- repartition by range using `source`, `series_id`, then layer time keys;
- sort within partitions on those same keys before append or MERGE; and
- choose a bounded target partition count from configuration or an `auto`
  resolver based on estimated bytes and target file size.

A very large series may span adjacent range partitions by date; that is better
than an unbounded file. Recommended key order is:

| Batch | Repartition/sort keys |
| --- | --- |
| Bronze | `source, series_id, ingested_at` |
| Silver | `source, series_id, observation_date, realtime_start` |
| DQ | `series_id, check_name` (or existing stable DQ identity fields) |

The exact Spark API (`repartitionByRange` or an equivalent) is an
implementation decision only after integration proof preserves schema/casts
and yields the expected distribution. Do not use driver-side `collect()` to
prepare a write: the requested in-memory buffer already resides on the driver
and is handed to Spark once per phase.

Record per-phase buffered rows/estimated bytes, target and actual partitions,
source rows, Delta files added/removed where available, commit duration, and
records per file. This makes the small-file improvement measurable.

## 6. Configuration and immediate fallback

Add these settings to the standard checked-in configuration surface, example
configuration, environment/config loader, and operator documentation:

```yaml
persistence:
  write_mode: run_batched          # run_batched | per_series
  delta_target_file_size_mb: 256  # used by auto partition selection
  delta_target_partitions: auto   # positive integer | auto
  batch_max_buffer_bytes: 2147483648
```

- `run_batched` is the intended default after the acceptance criteria are
  proven. It implements this specification.
- `per_series` is compatibility mode, preserving immediate write ordering and
  live per-series audit updates. It is selectable in deployed configuration or
  its environment override before a run, with no code change or redeploy.
- `batch_max_buffer_bytes` is a hard pre-flush guard. Maintain a conservative
  serialized-size estimate as outcomes are buffered. If it is exceeded, fail
  before any of the three data phases and direct the operator to
  `write_mode: per_series` or a deliberately increased capacity. Never emit
  mixed batch/per-series output silently.
- Invalid modes, non-positive limits, and unavailable `auto` sizing inputs
  fail configuration validation. They never silently select legacy behavior.

The shown 2 GiB is a starting setting, not a production capacity claim. Before
enabling batching in an environment, set its limit from measured payload size
and leave driver headroom for normalization, Python objects, and Spark
DataFrame creation. The rollout PR must record the selected production value
and memory proof.

## 7. Existing default load-selection behavior

No new "should this series load" logic is required: it already exists and is
the default for `python -m fred_pipeline run --env dev`.

- `FredPipeline.run()` and `run_from_manifest()` default `skip_not_due=True`,
  and the CLI's `--skip-not-due` default is true.
- The gate reads the selected warehouse's latest *changed* Silver
  `ingested_at` per series and compares it with manifest
  `expected_update_frequency`: daily/business daily 20 hours, weekly 5 days,
  monthly/quarterly 25 days, annual 180 days. No prior row or an
  missing/unrecognized cadence fails open: the series is loaded.
- `--no-skip-not-due` attempts every active series. `--full` bypasses the gate
  and forces full-history extraction.
- `_plan_extract()` already uses a full load for first load, `load_type: full`,
  `--full`, or no warehouse. Otherwise it restates the configured trailing
  observations and relies on the idempotent Silver merge for revisions.
- An unchanged API response does not advance the Silver watermark, so an
  already-due series can be checked again next run. That is deliberate.

Spec 014 begins only after this planning step. It must not alter selected
series, load windows, default CLI behavior, or due-date semantics.

## 8. Implementation boundaries and acceptance

Implement in this order:

1. Add pure typed buffer/flush-planning helpers with unit tests; refactor
   `_finish_series()` into collection/finalization paths without changing
   `per_series` behavior.
2. Add explicit warehouse batch operations. Provide Local, PostgreSQL, and
   Spark adapters; Spark owns repartition/sort behavior.
3. Add configuration, environment override, diagnostics, example config, and
   runbook, then implement the three flush phases and batched audit handling.
4. Add Spark integration proof for data parity and file layout. Stage rollout
   with a measured buffer limit before changing any shared environment default.

| ID | Acceptance criterion |
| --- | --- |
| AC-001 | All due-series outcomes are collected before the first Bronze/Silver/DQ write; worker threads never write a warehouse. |
| AC-002 | A normal non-empty run invokes one raw batch, one cleaned batch, and one quality/audit batch; empty layer batches do not write. |
| AC-003 | `run_batched` and `per_series` fixture runs have identical Bronze payloads, Silver keys/values/vintages, DQ records, series statuses, skipped records, and Gold inputs. |
| AC-004 | A failing-DQ fixture writes raw and failed DQ data but no Silver row for that series, in both modes. |
| AC-005 | An extraction failure remains isolated and creates no fabricated raw/DQ data. |
| AC-006 | Spark tests prove series-clustered write preparation, preserve casts and MERGE idempotency, and show fewer Delta files/commits than `per_series`; they must not use `partitionBy(series_id)`. |
| AC-007 | `write_mode: per_series` is selectable without code change and reproduces current immediate-write behavior; invalid settings fail closed. |
| AC-008 | A buffer-cap breach happens before the first data-phase write and recommends the configuration-only fallback, with no mixed-mode output. |
| AC-009 | Tests preserve default due-date gating, `--no-skip-not-due`, `--full`, first-load, unchanged-watermark, and restate-window behavior. |
| AC-010 | Metrics/runbook document buffer size, partition choice, counts/durations, file counts, failure recovery, and the rollback setting. |

Out of scope: changing cadence policy, streaming raw payloads out of memory in
batched mode, automatic mid-run fallback, physical partitioning by individual
series, Gold algorithm changes, and cross-table transaction guarantees Delta
cannot provide.

## 9. Next task

Start with the pure outcome-buffer contract and dual-mode orchestration tests.
Do not change a warehouse writer until tests establish parity for passed DQ,
failed DQ, extraction failure, not-due, and manifest-order behavior. Then add
the Spark batch-layout adapter and integration proof before changing a shared
default configuration.

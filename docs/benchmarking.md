# Pipeline Benchmarking Guide

`scripts/benchmark_pipeline.py` reports repeatable pipeline timings and workload
counters. Use fixture mode for code comparisons; use live mode only when the
source API quota and target warehouse are available.

## Fixture Workloads

The checked-in fixture seeds a fresh SQLite database for each iteration. Seeding
is excluded from measured wall time. No API key or network access is used.

```bash
PYTHONPATH=src python scripts/benchmark_pipeline.py \
  --fixture tests/fixtures/benchmark_pipeline.json \
  --workload unchanged --iterations 3 --output /tmp/benchmark-unchanged.json
```

Run the other comparable workloads by changing `--workload`:

```bash
PYTHONPATH=src python scripts/benchmark_pipeline.py \
  --fixture tests/fixtures/benchmark_pipeline.json \
  --workload restatement --iterations 3 --output /tmp/benchmark-restatement.json

PYTHONPATH=src python scripts/benchmark_pipeline.py \
  --fixture tests/fixtures/benchmark_pipeline.json \
  --workload full --iterations 3 --output /tmp/benchmark-full.json
```

`unchanged` replays the same observations as the seed, `restatement` changes one
existing observation, and `full` forces full-history extraction. Each iteration
starts from the same seeded SQLite state. Fixture mode currently supports the
local backend only.

## Live Workloads

Live mode defaults to the configured local backend and a six-series sample. It
uses real source clients and the selected warehouse. Source calls are logical
client method calls, not low-level HTTP retries.

```bash
PYTHONPATH=src python scripts/benchmark_pipeline.py \
  --series GDPC1,UNRATE,CPIAUCSL,DGS10,SOFR,BAMLH0A0HYM2 \
  --backend local --iterations 3 --no-skip-not-due \
  --db-path /tmp/benchmark-live.db --output /tmp/benchmark-live.json
```

Use `--backend postgres` to use the PostgreSQL settings in `config/warehouse.yml`.
Use `--full-catalog` only for a deliberate large run; it can make many API calls.
`--workload full` passes the pipeline's full-history extraction flag. `--dry-run`
is available only in live mode and excludes warehouse writes and Gold work, so it
is not comparable with fixture or persisted runs.

For before/after comparisons, keep the same hardware, fixture or source snapshot,
backend, series, cadence-gate setting, and workload. Use at least three repeats;
compare medians and ranges, not one run's wall time. Compare `--no-skip-not-due`
with the default gate as separate workloads.

## Report Fields

The JSON report has `schema_version`, a `summary`, and one object per run.

| Field | Meaning |
|---|---|
| `summary.median_total_wall_seconds`, `min_...`, `max_...` | Timing range across repeats |
| `run_id`, `workload`, `backend`, `status` | Run identity and outcome |
| `total_wall_seconds`, `process_peak_rss_bytes` | End-to-end duration and process high-water RSS |
| `selected_series`, `due_series`, `succeeded_series`, `failed_series`, `skipped_not_due` | Actual selection and pipeline outcomes |
| `rows.observations_extracted`, `bronze_rows_written` | Observations and Bronze writes |
| `rows.silver_rows_merge_reported` | Rows submitted to the Silver merge, as reported by the pipeline |
| `rows.silver_rows_changed` | Actual SQLite inserts/updates; `null` where the backend does not expose an exact count |
| `rows.silver_rows_read`, `latest_rows_read` | Gold builder input row counts when available |
| `warehouse_write_seconds` | Measured Bronze write and Silver merge durations |
| `stages` | Tracker durations/status/details for plan, extract, Gold, release calendar, and persistence |
| `gold_table_seconds` | Opt-in per-stage timings from the local Gold builder |
| `source_calls` | Logical client calls by source and method |
| `database_size_bytes` | SQLite file size after the run; `null` for other backends |

`process_peak_rss_bytes` is the process high-water mark (on fixture runs this
includes setup/seeding); it is not an isolated per-stage memory profile. Use a
fresh process for independent memory comparisons. Gold per-stage timings require
`FRED_GOLD_STAGE_TIMING=1`, which the benchmark enables.

## Historical Results

Older timings in `docs/handoffs/pipeline_performance.md` and previous versions
of this guide are historical measurements, not current baselines. In particular,
the old 45-second six-series example was illustrative and came from a harness
that read timings from a nonexistent audit table. Re-measure with this script
before asserting speedups or setting performance gates.

## Validation

The fixture smoke tests are in `tests/test_benchmark_pipeline.py`. Run them with:

```bash
PYTHONPATH=src python -m pytest tests/test_benchmark_pipeline.py -q
```

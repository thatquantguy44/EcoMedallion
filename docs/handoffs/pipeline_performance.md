# Pipeline performance handoff: Gold rebuild is the bottleneck, root cause identified

**Status: Investigation done, fix not yet implemented.** A blocking
correctness bug that prevented any real-scale measurement has been fixed and
merged (see below). The actual performance fix — Phase 1 of
[`specs/spec003`](../../specs/spec003/README.md) — is scoped but not started.

**Audience:** an agent working in **this** repo (`fred-bronze-to-gold-pipeline`).
**Why:** a routine local refresh (`run --local --db-path fred_local.db`) was
reported as slow. Extraction took 42.6 minutes for 2,821 series (external,
mostly rate-limit-bound — not addressed here). The bigger surprise: a
standalone `gold --local` rebuild against the real `fred_local.db` (31.8 GB,
32.1M Silver rows) took **~51 minutes and then failed**, which is what
triggered this investigation.

## What was found

1. **The Gold rebuild was failing outright**, not just slow. Root cause:
   `gold_dim_date` gained three columns (`is_imm_date`,
   `is_monthly_option_expiry`, `is_triple_witching`) in commit `0773630`
   without a matching entry in `LocalWarehouse._ADDED_COLUMNS`
   (`src/fred_pipeline/io/local_store.py`) — the mechanism this repo already
   uses to migrate columns onto a pre-existing local db file (`CREATE TABLE
   IF NOT EXISTS` is a no-op against an existing table). Every `gold` run
   against a database created before that commit has been failing since.
   **Fixed** — the three columns are now registered in `_ADDED_COLUMNS`.
2. **The failure was nearly undiagnosable from the logs.** `pipeline.py`
   logged `Gold refresh failed for run <id>` followed by `NoneType: None`
   instead of the real error, because `log.exception()` was called *after*
   the stage tracker's exception-swallowing `with` block had already exited
   — there was no live exception left for `log.exception()` to report, even
   though the tracker had already captured the real `error_type`/
   `error_message`. **Fixed** — the log line now uses the tracker's captured
   fields directly. Regression test:
   `tests/test_pipeline.py::test_gold_failure_logs_the_real_error_not_noneType_none`.
3. **Why the rebuild takes ~51 minutes even when it doesn't fail:**
   `LocalWarehouse._build_gold_inner()` materializes all 32.1M Silver rows
   into a Python `list[dict]`, then builds two of the largest Gold tables —
   `gold_fred_point_in_time` (32.1M rows, a straight 1:1 copy of Silver) and
   `gold_fred_latest_observation` (20.4M rows, a Python dict-groupby + sort
   over all 32.1M rows) — entirely in pure Python, writing each through the
   slow per-row `_insert()` path (Python tuple + `_encode()` per cell,
   `executemany`). A polars-accelerated write path already exists in this
   codebase (`_insert_frame`) but is wired to four smaller tables, not these
   two — which together are more rows than everything else in Gold combined.
   Full detail, evidence, and the phased fix: **[`specs/spec003`](../../specs/spec003/README.md)**.

## What's next (not started)

Phase 1 of spec003: rewrite `gold_fred_point_in_time` as a single
`INSERT INTO ... SELECT` (it's a pure copy — never needs to leave SQLite) and
`gold_fred_latest_observation` as a `ROW_NUMBER()`/`GROUP BY` SQL query
instead of Python. Re-baseline against the real `fred_local.db` afterward;
only pursue incremental Gold rebuilds or parallelizing the remaining ~65
smaller tables (Phases 3–4) if the re-baseline shows they're still needed.

Also flagged in spec003 as an open question worth a deliberate decision, not
a default: whether `gold_fred_point_in_time` needs to be a materialized table
at all, versus a SQL view over Silver — that would remove roughly half of
Gold's row-writing volume outright but changes the read contract for anything
already querying it directly.

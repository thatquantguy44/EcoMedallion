# Performance test coverage

The extended tests complement the existing flat `tests/test_performance*.py`
suites and follow the repository's pytest conventions. Shared local fixtures
live in `conftest.py`; synthetic wire cases are in
[`../fixtures/performance/`](../fixtures/performance/README.md).

| Area | Tests |
| --- | --- |
| Concurrent timing, parentage, batching, bounded samples and journal completion | `test_collector_edges.py` |
| Callable metadata, async results, broken callbacks and instrumentation failures | `test_timing_decorator.py` |
| Completed/partial/early-failure alerts and a real retried FRED transport | `test_saved_reports.py` |
| Journal replay, history filters/limits and Delta error cleanup | `test_store_history.py` |
| Planning/Gold/calendar/audit failures and telemetry-storage outages | `test_pipeline_failures.py` |
| Deployment revision metadata and outer command scopes | `test_run_scope.py` |
| Stage verdicts and late warning persistence | `test_stage_outcomes.py` |
| Bounded large failure reports and summary adaptation | `test_alert_limits.py` |
| Native PostgreSQL journal replay and outcome amendments | `test_postgres_replay.py` |

```bash
python -m pytest -o addopts='' -q tests/performance
python -m pytest -o addopts='' -q
```

No tests refresh production data or send email. Provider responses, clocks and
notification boundaries are controlled. SQLite tests use temporary files;
PostgreSQL tests reuse the existing disposable-database fixture and skip when
that optional service is unavailable. Delta tests verify the adapter boundary;
they do not claim proof against a live Databricks cluster.

Regression fixes found by this audit are kept small and covered by failing-first
tests: malformed timing attributes must not interrupt work, the aggregate cap
must not leave completed journal events running, and late stage warnings must
reach the database. The previously failing schema-parity source lookup now
uses the AST, and the money-market snapshot is represented in the report catalog.

## Verified results

On 2026-11-11, the complete suite passed **1,810 tests**, with **eight optional
skips** (Spark integration and seven ECB PostgreSQL-lock checks requiring an
explicit DSN). The new PostgreSQL replay test ran against a disposable database.
The audit added 63 test cases, including the schema-lookup regression, and
resolved the four former baseline failures.

Combined statement/branch coverage across the six timing/reporting modules was
**99.28%**: the collector and stage lifecycle rounded to 99%; storage, decorators,
performance rendering and email rendering each reached 100%. This is coverage
of these modules, rather than a claim of whole-repository coverage.

Reproduce the measured run with:

```bash
python -m pytest -o addopts='' -q \
  --cov=fred_pipeline.performance \
  --cov=fred_pipeline.performance_store \
  --cov=fred_pipeline.timing \
  --cov=fred_pipeline.governance.performance_report \
  --cov=fred_pipeline.governance.stages \
  --cov=fred_pipeline.governance.email_report \
  --cov-report=term-missing:skip-covered
```

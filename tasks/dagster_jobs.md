# Configurable Dagster jobs

Implements the manual job/configuration slice of [Spec 016](../specs/spec016/SPEC-dagster-integration.md).
The requested scope includes a standalone FOMC refresh job. Production cutover,
crash-alert deduplication, host monitoring and restore evidence remain separate
acceptance gates; this work does not activate production automation.

Build order:

- [x] Add validated per-job configuration, an explicit job registry and independent
  launch files for configured dev, PostgreSQL and local development.
- [x] Reuse the CLI's pipeline library and stage tracker; retain CLI defaults
  and map partial/required-stage failures to failed jobs.
- [x] Protect PostgreSQL ECB initialization, polling, explicit ingestion and live
  archive promotion using the existing writer-session lock.
- [x] Implement main, ECB polling/health, warehouse health, Gold rebuild, FRED
  metadata reconciliation and standalone additive FOMC refresh execution.
- [x] Verify real in-process Dagster execution with isolated fixtures, import
  safety, read-only monitoring, configuration errors and failure events; run
  the existing regression suite.
- [x] Document launch commands, protected configuration, concurrency and the
  remaining production gates. Schedules remain stopped.

Configuration contract: launch config contains environment, backend selection,
workload filters and bounded tuning only. Pipeline, warehouse, manifests and
FOMC paths are controlled by existing server environment variables (plus
`FRED_DAGSTER_MANIFESTS`). Credentials never belong in launch files. Durable
jobs select exactly one configured backend and never fall back. Production
backend overrides must agree with server configuration. The main Dagster job
does not poll ECB; FOMC refresh defaults on for main runs and is offline for
Gold-only rebuilds. Whole-job retries are disabled.

Verification on 2026-11-13: full suite **1,892 passed, 1 skipped**; Ruff and
documentation/whitespace checks passed. A real Dagster CLI code-location load
listed all seven jobs, and a multiprocess offline FOMC job succeeded in a
temporary instance. PostgreSQL 16 tests used disposable databases to prove
main ingestion, actual Gold rebuild, signed ECB fixture commits/no-ops,
read-only monitoring and every live writer boundary. The operator's separate
working-calendar edit was preserved; fixed calendar tests use a pinned fixture.
No production schedules were activated. See the
[runbook](../docs/deployment/dagster_runbook.md) for remaining production gates.

Follow-up job testing added PostgreSQL health-job executions with empty and
healthy persisted evidence, no source credentials, no query-log writes and a
missing Gold relation followed by a healthy relation in the same snapshot.
Those regressions exposed and verified a fix for a savepoint name that the
PostgreSQL adapter incorrectly translated as a Gold relation. All 52 tests in
the dedicated Dagster/runtime/ECB health and writer-lock set pass.

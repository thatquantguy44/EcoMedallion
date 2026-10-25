# Dagster Orchestration Handoff

Status: Specification drafted 2026-10-25; pending review. Implementation has
not started. No services, dependencies or schedules changed.

Last verified: 2026-10-25 (repository/specification consistency and document
links only; Dagster runtime and production gates remain unverified).

Branch: `spec012-dagster-orchestration`, created from `origin/main` at `5077ef8`.

Start with [Spec 012](../../specs/spec012/README.md), then
[the implementation plan](../../tasks/plan.md) and
[the task checklist](../../tasks/todo.md).

## Next pickup

Review the first-release scope, then execute T1 (optional dependency/runtime
compatibility) and T2 (shared ECB execution service). Prove T3 writer exclusion
before allowing Dagster to write. The first runnable slice is a manual poll
with schedules stopped and fixtures/isolated PostgreSQL.

The planned production shape is Dagster OSS on Linux, with private webserver,
one daemon, a code-location service, a separate PostgreSQL metadata database,
and the existing `macro_medallion` business destination. PostgreSQL remains
the configurable authority. Existing SMTP/Graph routes deliver business alerts;
a failure sensor handles startup/crash gaps. Independent host monitoring must
detect daemon outage.

Important contracts for implementation:

- CLI dedicated poll and main-run ECB hook share the same library service.
  Lock explicit ingestion and historical live promotion too.
- Only accepted database commits create materializations. No-op polls record
  observations and still run freshness/quality checks.
- Three stopped schedules mirror 17:45, 18:25 and 21:20 weekdays in Europe/Berlin.
- A separate checks-only job exposes health without source polling.
- Main pipeline schedules use `--no-ecb-assets` after Dagster takes ownership.
- Disable/drain the old timer before activating Dagster; reverse that sequence
  on rollback. Do not remove the existing systemd resources.
- Keep all credentials outside tracked files and event metadata.

## Production facts still needed

The external Linux host, access method, protected configuration paths, runtime
capacity, private UI route, metadata backup target and independent monitor
route have not been established in this specification work. Resolve them before
deployment/activation; they are not prerequisites for local T1–T8 work.

Spec 011's successful 2026-10-25 repository PostgreSQL proof is not an external
host deployment proof. Its [handoff](ecb_eligible_assets.md) and
[runbook](../deployment/ecb_eligible_assets_runbook.md) still govern the current
poller and database verification. If Dagster becomes the selected initial host
scheduler, satisfy those database/alert gates through the Dagster cutover rather
than activating a second timer first.

RSS event triggering, archive backfill orchestration and the macro/FRED graph
are deferred modules with explicit addendum requirements. Do not expand the
first-release implementation to them by implication.

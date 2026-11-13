# Dagster Orchestration Handoff

Status: Optional runtime plus seven configurable manual jobs are implemented.
The PostgreSQL session lock now guards initialization, polling, direct release
ingestion and live archive promotion. Imports, flat launch configuration,
read-only monitoring and failure/event behavior have isolated tests. Three ECB
schedules are defined and stopped. Production deployment and the remaining
Spec 013/016 operational gates are still pending.

Last verified: 2026-11-13 (isolated jobs, SQLite, disposable PostgreSQL and recorded
source fixtures; no production activation, source polling or live email).

## Manual job configuration

Start with the [Dagster runbook](../deployment/dagster_runbook.md) and
[`resources/dagster/jobs`](../../resources/dagster/jobs). Main pipeline, ECB poll,
ECB health, warehouse health, Gold rebuild, FRED metadata reconciliation and
standalone FOMC refresh each have a validated schema and launch file. Main
refreshes FOMC by default and delegates ECB ownership to the separate job.
Credentials and paths remain in server configuration; durable jobs reject
fallback. See the [implementation record](../../tasks/dagster_jobs.md).

Remaining release work includes native asset-check definitions, complete
maintenance email/crash-sensor ownership and deduplication, stale queued-run and
missed-tick recovery, production services, independent daemon monitoring,
metadata backup/restore and actual cutover/rollback evidence. The historical
phase checklist below is not a claim that those gates are complete.

The expanded confirmed job scope and current initiative are in
[Spec 016](../../specs/spec016/README.md). It supersedes the earlier ECB-only
branch and workload scope; retain the safety contracts in
[Spec 013](../../specs/spec013/README.md). See the
[implementation plan](../../tasks/plan.md) and [checklist](../../tasks/todo.md).

## Shared ECB execution

The dedicated poll command and main-run ECB hook now call the same library
function, which checks backend authority and borrows the caller's warehouse
and network client. Its typed poll failure report retains results for releases
committed before a later failure; the report excludes source exception text.
The exception text and cause retain the existing CLI diagnostic and must not be
emitted as Dagster metadata. The report now includes a restricted repository
snapshot and health verdict and records the result of an injected notification
attempt. Both CLI poll paths now supply their existing alert sender to that
service; pre-service startup failures retain the CLI alert path. Health uses
the configured ECB alert thresholds. The CLI still owns network and warehouse
cleanup. The Dagster runtime owns its warehouse, selected network sessions and
writer scope; both entry points reuse `execute_ecb_poll`. Complete the remaining
release gates above before activating production schedules.

## Writer-lock implementation status

`ECBWriterLock` uses the writer's PostgreSQL session and a fixed dataset key.
Independent sessions in a disposable PostgreSQL 16 container verified
contention, commit survival, exception and aborted-transaction cleanup,
cleanup failure, and process-death release. It now guards the library entry
points. PostgreSQL warehouse construction normally executes schema
DDL and a data backfill before the caller can take a lock; simply wrapping the
poll after construction is therefore insufficient. The new `initialize=False`
option opens a connection without those writes. The Dagster ECB writer acquires
the session lock and requires existing schema before source access. CLI schema
initialization uses the same lock, and public poll, ingest and promotion
boundaries hold it on the writing session across commits. Read-only jobs add
database-enforced read-only transactions separately. See the
[PostgreSQL advisory-lock documentation](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)
for session lock semantics.

To rerun the isolated integration tests, set `TEST_ECB_POSTGRES_DSN` to a
disposable PostgreSQL database and run:

```bash
python -m pytest -q tests/test_ecb_writer_lock.py
```

The tests open independent sessions and one child process; they do not use or
change the business warehouse.

## T1 runtime verification

The optional extra pins Dagster 1.13.26, Dagster webserver 1.13.26 and
Dagster PostgreSQL integration 0.29.26. Clean Python 3.12 installs were verified
in isolated Linux and local environments, including a locked reinstall,
dependency checks, CLI smoke checks and a core-only install without Dagster.
The [development runbook](../deployment/dagster_runbook.md) records the exact
scope and commands. Definitions import safety now has a separate no-I/O test.

## Architecture reminders

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

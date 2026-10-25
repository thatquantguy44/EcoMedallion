# Spec 012: Dagster Orchestration

Status: **Draft for review; specification only. No Dagster dependencies,
definitions, services, or schedules have been installed or activated.**

Date: 2026-10-25

Review status: Pending user review

Depends on: [Spec 011](../spec011/README.md), its PostgreSQL verification
contract, and the existing warehouse and alerting configuration.

Implementation plan: [tasks/plan.md](../../tasks/plan.md)

Task checklist: [tasks/todo.md](../../tasks/todo.md)

Pickup point: [Dagster handoff](../../docs/handoffs/dagster_orchestration.md)

## 1. Objective and scope

Give the operator one place to launch ECB polls, inspect release provenance,
see data health, investigate failures, and manage the polling schedule. Reuse
the existing signed MID ingestion and PostgreSQL persistence paths. Success is
a verified production ECB cycle through Dagster with the same accepted data,
point-in-time history, quarantine behavior, and real email delivery as the CLI.

The first release is **ECB forward polling on PostgreSQL through Dagster OSS**.
The broader pipeline is a roadmap, not an implicit requirement to migrate all
sources in this change. The current request authorizes this specification and
branch; implementation, infrastructure provisioning, and schedule activation
are subsequent work.

### Capability map and build order

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| ecb-execution | Shared poll service, configuration guard, cross-process writer exclusion | Existing ECB pipeline |
| dagster-ecb | Definitions, poll job, health job, asset events and checks | ecb-execution |
| dagster-operations | Schedules, failure notification, deployment, verification and rollback | dagster-ecb |
| ecb-event-trigger | Optional RSS sensor and independent SLA check | dagster-operations proven |
| archive-orchestration | Optional archive inventory and staging backfills | dagster-operations proven |
| macro-orchestration | Optional main pipeline execution and later granular lineage | dagster-operations proven |

Only the first three modules are first-release scope. Their requirements and
boundaries are specified below. Later modules require an addendum before
implementation. Build order is execution → local Dagster path → health and
alerts → deployment rehearsal → production cutover.

### Included

- Shared library entry point used by dedicated poll CLI, main `run` ECB hook,
  and Dagster, with unchanged command defaults and compatible JSON output.
- Optional dependencies and an independently deployable Python code location.
- One logical ECB dataset asset, a poll job, and a read-only health/check job.
- Three disabled-by-default schedules matching the existing weekday cadence.
- PostgreSQL writer lock covering every supported authoritative entry point.
- Structured run/release correlation, checks, and existing SMTP/Graph delivery.
- Local development, isolated PostgreSQL integration, Linux deployment
  templates, production evidence checklist, and scheduler rollback procedure.

### Excluded from the first release

- Dagster+, Kubernetes, Databricks orchestration, Spark/Delta writer changes,
  FRED/other-source scheduling, and automated archive promotion.
- RSS sensors, declarative automation, per-security assets, and partitioning
  the mutable forward dataset by date.
- New eligibility tables, revised SCD2 semantics, schema normalization,
  signature exceptions, gap overrides, or automatic quarantine acknowledgement.
- Exposing the UI to the public internet or putting credentials in run config.

## 2. Verified baseline and unresolved deployment facts

The branch starts from `origin/main` at `5077ef8` (PR #90). Repo inspection:

| Existing component | Integration implication |
| --- | --- |
| `ECBEligibleAssetsPipeline.poll()` and `ingest_release()` in `src/fred_pipeline/ecb_eligible_assets.py` | Reuse signed ingestion, chronological processing and per-release transaction boundaries |
| `_cmd_ecb_assets()` and `_poll_ecb_assets_for_run()` in `src/fred_pipeline/cli.py` | Consolidate poll orchestration without importing CLI internals from Dagster |
| `WarehouseFactory`, authority setting and fallback checks | Reject a production backend mismatch before writes; Dagster cannot silently fall back |
| `governance/ecb_alerting.py`, `ecb_calendar.py` | Reuse TARGET calendar, expected-release cutoff, grace period, quarantine and change-volume assessment |
| `resources/ecb-eligible-assets.service` and `.timer` | Preserve 17:45, 18:25 and 21:20 weekdays in `Europe/Berlin`; systemd remains available for rollback |
| Main `run` defaults to polling ECB | When Dagster owns ECB scheduling, explicitly use `--no-ecb-assets` for separately scheduled main pipeline runs |
| PostgreSQL Gold bounded-read implementation in Spec 010 | Orchestration must not reintroduce full-history Python materialization |
| `tests/test_postgres_warehouse.py` and PostgreSQL CI service | Extend isolated integration coverage; never point CI at production |

The 2026-10-25 local PostgreSQL production-profile proof accepted the 2026-10-23
release, including three hash-proven source-no-op updates, and returned no work
on immediate repoll. This does **not** prove an external production host is
configured. Its hostname, deployment paths, UI access route, backup location,
capacity, and production DSNs still need to be supplied at deployment time.
Spec 011's external-host activation gate remains open.

## 3. Architecture decisions

### 3.1 Shared execution service

Create `src/fred_pipeline/ecb_execution.py` without a Dagster dependency.
The service accepts typed configuration and injected warehouse/client/clock
factories for tests; production resolves them through the existing loaders.
It owns authority validation, lock lifetime, polling, health assessment,
notification attempt, and cleanup. Callers format results or emit events.
When the main pipeline supplies an already-open warehouse, the service borrows
it and releases only its lock; it must not close the caller's warehouse. When
it constructs the warehouse itself, it closes it on every exit.

Contract for the proposed poll result:

- Operation correlation ID and optional external Dagster run ID.
- Actual selected backend and a non-secret destination identifier.
- Per-release results using `IngestResult.to_dict()` fields, including release
  ID, N/U/D counts, `source_noop_count`, coverage and reconciliation status.
- Final repository status and health verdict, poll outcome (`accepted`,
  `no_new_release`, or `busy`), start/end times, and notification outcome
  (`not_required`, `accepted`, `failed`, or `disabled`).
- Failures raise a typed exception with a sanitized execution report after
  the health/alert attempt. Accepted releases committed earlier in a multi-release
  poll remain accepted. The failed release stays quarantined; do not pretend
  the entire poll was one database transaction.

Preserve CLI exit codes and main-run failure semantics in the extraction
slice. Dagster independently maps the report to its own status. Missing
configuration or disabled/non-authoritative production destination is a failed
Dagster run, never a successful empty result. Do not open database connections,
resolve secrets, send mail, or fetch RSS while importing definitions.

### 3.2 Data authority and asset model

The configurable warehouse authority remains the authority. The first Dagster
production deployment requires PostgreSQL with the configured primary and
authoritative backend matching; no automatic fallback is permitted. SQLite
remains a test/development option. Other production authorities require a new
lock and deployment proof before support is claimed.

Use stable asset key `['ecb', 'eligible_securities']`, group `ecb`, for the
accepted release state and its existing relations. Record fully qualified
Bronze/Silver/Gold names as metadata. This initial one-asset boundary reflects
the existing coupled write path and avoids suggesting that individual views
can be independently rebuilt. Later table-level assets require a separate
design with truthful dependency and transaction semantics.

`ecb_eligible_assets_job` runs a poll op and then health checks. Emit an asset
materialization only for each newly accepted release, after its database
commit. Never emit a materialization on discovery, lock contention, no-op,
quarantine, or signature failure. On no-new-release runs emit an observation
with latest accepted publication time and run health. Retrieval time and
Dagster execution time must not masquerade as ECB publication freshness.

Use explicit observations/materializations from the poll op so an ordinary
successful no-op does not automatically create a materialization. The second
job, `ecb_eligible_assets_health_job`, selects only checks on the same asset
and performs no poll or ingestion writes. Both jobs run without passing
security rows between Dagster steps or storing them via an I/O manager.

### 3.3 Writer exclusion and recovery

Dagster concurrency limits alone do not protect against CLI/systemd writers.
Use a PostgreSQL session advisory lock on the same database session that
performs the live writes, with a documented fixed dataset-specific integer key.
Hold it from before reading the accepted frontier until all release writes
complete. A session lock survives per-release commits; explicitly unlock it on
every exit, and close the session if cleanup fails. Losing that session also
prevents writes through it; automatic reconnection must reacquire the lock and
reread the frontier. A separate lock-only connection would leave a race after
its loss and is therefore insufficient. Bootstrap/migrations are an explicit
exclusive operation before scheduler activation. Routine warehouse setup must
either be non-mutating on an initialized schema or acquire this lock before
its initialization writes; characterize that lifecycle in T3 before selecting
the implementation boundary.

Every supported writer participates: standalone poll, explicit ingest-release,
the main pipeline ECB hook, and historical live promotion. Stage-only archive
work does not need the live-dataset lock. Resolve helpers once and reuse them;
do not recursively reacquire the lock inside the same operation. Direct SQL
outside the application is an operational maintenance responsibility.

A contender exits promptly without source downloads or data writes. Dagster
records a `busy` observation and continues to a health assessment; CLI reports
the skip clearly while retaining compatible command behavior. A busy outcome
cannot satisfy a release-freshness gate by itself. Test contention, normal
release, exception cleanup, and process-death release with independent
PostgreSQL sessions. Apply a Dagster run queue limit of one ECB poll as an
additional resource control.

Keep existing bounded HTTP retries. Disable whole-job automatic retries in
v1: deterministic validation failures need investigation. A new scheduled or
manual run can retry transient failures. A crash after commit but before the
Dagster event is repaired by reading accepted state and emitting an observation;
the database ledger, not Dagster event presence, determines acceptance.

### 3.4 Scheduling contract

All schedules ship stopped. Set `execution_timezone='Europe/Berlin'` explicitly.

| Name | Cron | Target | Purpose |
| --- | --- | --- | --- |
| `ecb_poll_prepublication` | `45 17 * * 1-5` | Poll job | Discovery before stated publication |
| `ecb_poll_postpublication` | `25 18 * * 1-5` | Poll job | Publication retry |
| `ecb_poll_sla` | `20 21 * * 1-5` | Poll job | Final poll plus late-release assessment |

Each poll performs health checks even when it finds no new release. The health
job is available for independent manual checks. TARGET holidays retain the
existing expected-release logic rather than assuming every weekday publishes.
Test winter/summer UTC conversion, DST transition weeks, weekends, TARGET
holidays and grace boundaries with an injected clock.

Do not assume Dagster replays missed ticks like systemd `Persistent=true`.
On daemon restart, inspect tick history and perform one explicit recovery
poll/check if a tick was missed. Recovery polls process available RSS releases
chronologically using current pipeline rules; missing RSS history is a coverage
incident, not permission to download guessed historical URLs or allow gaps.
Reject stale queued poll runs older than two hours with an explicit skip reason
and request one current recovery poll. Acceptance tests must verify this
behavior using the pinned Dagster version.

### 3.5 Health and alert delivery

Implement bounded SQL checks using the existing runbook's verification logic.
Evaluate a consistent accepted frontier in a read-only snapshot so concurrent
commit timing cannot produce mixed-release counts.

| Check | Passing rule | Failure handling |
| --- | --- | --- |
| `expected_release_present` | Latest accepted publication date meets `expected_release_date()` | WARN/partial; late email according to existing policy |
| `no_unresolved_quarantine` | No newer unresolved quarantine under current assessment rules | ERROR and failure email |
| `accepted_release_validated` | Stored signature statuses and reconciliation agree with acceptance | ERROR; never advance state |
| `current_matches_release` | Current unique keys/count match the accepted full snapshot count | ERROR |
| `history_intervals_valid` | At most one open version/key; no overlapping or reversed intervals | ERROR |
| `coverage_declared` | Accepted coverage is known; `complete` passes, `starts_here` is explicit limited history, `gapped` warns | WARN for gap; ERROR for unknown/inconsistent state |
| `source_noop_auditable` | Audited source-no-op updates have null computed changes and no new interval attributed to that update | ERROR |

No accepted release is an ERROR, not a healthy empty dataset. Count checks
must not hardcode today's security count. Signature checks inspect stored
verification evidence; full cryptographic re-verification remains the explicit
`ecb-assets verify` recovery/proof command. WARN checks remain visible and
trigger the appropriate business alert without claiming corruption. ERROR
checks must explicitly fail the health/job outcome, including checks-only
runs; do not assume nonblocking check results automatically fail a Dagster run.

Retain shared SMTP/Brevo and Microsoft Graph transports and route configuration.
Domain alerts are sent by the execution/health service. Record the transport
outcome against the Dagster run. The failure-status sensor sends a fallback
email only when there is no recorded provider-accepted domain failure alert,
covering initialization failure and worker crashes. Provider acceptance is not
mailbox receipt. Persist notification receipts as sanitized Dagster events;
do not add an ECB business-table migration solely for notification tracking.

Avoid duplicate sends for ordinary exceptions and sensor reevaluation. A crash
between provider acceptance and receipt persistence can still duplicate mail;
document at-least-once delivery rather than promising exactly once. Failed
delivery is visible as a separate operational failure, never swallowed as
successful alert proof. The poll's accepted data must not be rolled back just
because email failed. A run that accepted data but cannot deliver a required
alert ends failed with explicit committed-release metadata.

A daemon cannot alert on its own complete outage. Production readiness requires
an independent host/service monitor for daemon, code location and overdue poll
execution, using the existing email transport if desired. Keep UI access private
and prove a stopped-daemon incident through that monitor before cutover.

## 4. Configuration, deployment and operations

Use Python 3.12 for the initial orchestration runtime. Keep the repository's
Python 3.10–3.12 core support intact. Add an optional `dagster` dependency group
and a reproducible deployment constraints file. Resolve compatible exact
versions of `dagster`, `dagster-webserver`, and `dagster-postgres` together in
the implementation spike (their version numbering need not match). Record
Python support, docs version and resolved artifacts; do not invent a pin from
the changing docs site or force Dagster onto core-only users.

Proposed deployment is a single Linux host with systemd-managed webserver,
daemon, and gRPC code-location services using an immutable application release
and dedicated virtual environment. Use a process run launcher with bounded
concurrency initially; do not run `dagster dev` as production infrastructure.
The daemon is a single active instance. All services share compatible workspace,
instance config and release version. A container deployment remains an
alternative requiring a documented decision before changing the deployment
templates.

| Setting | Contract |
| --- | --- |
| `FRED_WAREHOUSE_CONFIG` | Existing protected warehouse config path |
| `FRED_POSTGRES_SERVICE_DSN` | Existing business PostgreSQL credential reference |
| `FRED_ALERTING_FILE` | Existing protected alert-route config |
| `FRED_SMTP_PASSWORD` / configured Graph secret reference | Existing transport-specific environment secrets |
| `DAGSTER_HOME` | Persistent instance directory outside the checkout |
| `DAGSTER_POSTGRES_DSN` | Proposed separate orchestration metadata database secret |
| Job environment | Fixed deployment environment, default dev locally; prod resource config cannot be overridden by arbitrary UI DSNs |

The deployment uses a separate `dagster` database and role for Dagster run,
event and schedule storage. `macro_medallion` retains business data. Sharing a
PostgreSQL server is permitted; monitor connections/disk and avoid granting
the metadata role business write access. Provision/upgrade metadata storage as
a deployment operation with backup and restore verification.

Run services under a dedicated OS account. Load mode-0600 environment/config
files outside Git. Never log passwords, full DSNs, mail bodies or recipient
addresses in tracked evidence. UI/run metadata contains counts, hashes,
release timestamps, sanitized errors and correlation IDs only. Bind webserver
to loopback and access by SSH tunnel initially; a shared UI requires an
authenticated proxy before nonlocal exposure. Verify OpenSSL/CA availability
for existing signature validation.

Default planned retention: 90 days of Dagster event/tick/log history, with
redacted cutover evidence retained with release records. Apply supported purge
controls without touching immutable business artifacts. Take daily metadata
backups; target metadata RPO ≤24h and restore RTO ≤4h, subject to host proof.
Loss of Dagster metadata must not alter the accepted ECB frontier. Stage a
restore with automation stopped so restored schedule state cannot launch a
second poller. Measure disk/RSS, database connections, queue delay and poll
duration during rehearsal; do not declare sizing proven from fixture tests.

## 5. Project structure and commands

Planned files (not implemented in this branch):

```text
src/fred_pipeline/ecb_execution.py          shared orchestration, no Dagster imports
src/fred_pipeline/io/ecb_writer_lock.py     PostgreSQL writer exclusion
src/fred_pipeline/orchestration/
    __init__.py                           lazy/optional integration boundary
    definitions.py                        exported Definitions object: defs
    resources.py                          warehouse/config resource lifecycle
    ecb_jobs.py                            poll and checks-only jobs, asset events
    ecb_checks.py                          bounded health checks
    ecb_schedules.py                       three stopped schedules
    ecb_notifications.py                   fallback run-failure sensor
resources/dagster/                         instance/workspace/service examples
requirements-dagster.lock.txt              exact compatible deployment constraints
tests/test_ecb_execution.py                CLI/service characterization
tests/test_ecb_writer_lock.py              independent-session integration tests
tests/test_dagster_ecb_*.py                 jobs/checks/schedules/alerts tests
docs/deployment/dagster_runbook.md          deployment, incident and rollback proof
```

Follow typed Python, snake_case and lazy optional imports. Illustrative
service boundary, not implemented API:

```python
def poll_ecb_assets(config: PipelineConfig, *, correlation_id: str) -> ECBPollReport:
    """Poll through the configured authority and return a sanitized report."""
    ...
```

Existing runnable baseline commands (require configured warehouse for writes):

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets poll --env prod
PYTHONPATH=src python -m fred_pipeline ecb-assets status --env prod
PYTHONPATH=src python -m fred_pipeline run --env dev --no-ecb-assets
python -m pytest -q tests/test_ecb_eligible_assets.py tests/test_cli_ecb_assets_default.py tests/test_ecb_alerting.py tests/test_ecb_schedule_resources.py
git diff --check
```

Proposed commands become available only after implementation, in an isolated
virtual environment and with development/staging config loaded:

```bash
python -m pip install -e '.[postgres,dagster,dev]' -c requirements-dagster.lock.txt
dagster dev -m fred_pipeline.orchestration.definitions
dagster job execute -m fred_pipeline.orchestration.definitions -j ecb_eligible_assets_job
dagster job execute -m fred_pipeline.orchestration.definitions -j ecb_eligible_assets_health_job
python -m pytest -q tests/test_ecb_execution.py tests/test_ecb_writer_lock.py
python -m pytest -q tests/test_dagster_ecb_jobs.py tests/test_dagster_ecb_checks.py tests/test_dagster_ecb_schedules.py tests/test_dagster_ecb_notifications.py
python -m pytest -q
```

The implementation must verify CLI flags against its pinned version and
document exact service, schedule enable/disable, backup and restore commands
in the runbook. No proposed command here is production activation evidence.

## 6. Testing and acceptance

Use pytest with injected client/clock/transport for offline tests and the
existing dedicated PostgreSQL test service for database integration. Add an
optional Dagster CI job on Python 3.12; keep core CI without the Dagster extra
to prove optionality. Run lock tests with independent processes/sessions and
job tests with persistent Dagster test storage where event history matters.

| ID | Acceptance evidence |
| --- | --- |
| AC-01 | Fresh core-only installation imports/runs CLI without Dagster; importing definitions performs no network/DB/SMTP work |
| AC-02 | CLI and Dagster produce identical accepted release, history and current state for accepted, no-op, source-no-op, multi-release and quarantine fixtures; existing CLI defaults/exit codes pass |
| AC-03 | Authority disabled/mismatch or missing production DSN fails before writes; no fallback database receives data |
| AC-04 | Concurrent poll/main-hook/explicit-ingest/promotion writers serialize or skip; crash releases lock; no duplicate/open overlapping intervals |
| AC-05 | Accepted commits emit materializations; no-op/busy/crash recovery emit observations only; failed release emits no success event and accepted earlier releases remain truthful |
| AC-06 | All health rules are tested, checks-only work on no-op days, ERROR fails the job, WARN sends policy-appropriate alerts, holidays/DST/grace use existing calendar |
| AC-07 | Three schedules are stopped by default with exact timezone/cron; stale queues and missed-tick recovery are proven |
| AC-08 | Normal failure yields one accepted alert; startup/crash failure uses fallback; failed delivery and sensor reevaluation are tested; real recipient confirms drill mail |
| AC-09 | Linux services restart cleanly, UI is private, monitor detects daemon outage, metadata and business databases have distinct roles; restore runs with automation stopped |
| AC-10 | Manual production-host poll plus Spec 011 verification SQL pass; main pipeline has no duplicate ECB trigger; one full scheduled operating-day cycle is evidenced |
| AC-11 | Fault/restart after DB commit recovers without new history; SQL counts and hashes remain correct; rollback to systemd is rehearsed without two active pollers |
| AC-12 | Queries/events are bounded and contain no security-row payloads; staging measurements record overhead, resource use and host capacity; runbooks identify actual versions/config and evidence |

First-release completion requires AC-01 through AC-12. Unit tests alone do
not satisfy the production gates. Skipped PostgreSQL/live delivery/host tests
remain pending and are explicitly reported.

## 7. Rollout, verification and rollback

1. **Characterize and extract:** capture current CLI behavior; consolidate the
   service and apply writer exclusion before adding Dagster consumers.
2. **Local/staging proof:** run the same recorded release fixtures through CLI
   and Dagster against separate isolated databases; compare ordered row/hash
   results and accepted frontiers. Schedules and live mail remain off.
3. **Deploy with automation stopped:** record hostname, release SHA, package
   lock, Python/OpenSSL versions, config references, service states, backup and
   access plan. Initialize only the dedicated Dagster metadata database.
4. **Manual production proof:** pause/drain the existing scheduler and any main
   run ECB hook before one Dagster write. Run the existing [Spec 011 verification
   SQL](../../docs/deployment/ecb_eligible_assets_runbook.md#verification-and-quarantine-recovery)
   from the runbook, plus a no-op
   repeat and checks-only job. Verify unchanged observation-series rows. Resume
   the old scheduler if cutover will not immediately follow.
5. **Cutover:** disable and drain the systemd timer/service, configure other main
   schedules with `--no-ecb-assets`, confirm no running writer, then enable only
   the three Dagster schedules and the fallback alert sensor. Retain the old
   service files. Record actual schedule states; defaults alone are insufficient.
6. **Observe:** capture every tick/run/check over one complete ECB operating-day
   cycle, publication frontier/counts/coverage, real alert receipt, crash/restart,
   independent daemon monitor and metadata restore proof. A no-op run is valid
   execution but does not prove a new-release materialization; capture one too.

Rollback order is explicit: stop Dagster schedules, disable its trigger sensors,
cancel/drain queued and running writer jobs, verify lock release and accepted
frontier, then enable the existing systemd timer. Revert main-run flags only
when that entry point is intended to own polling. Do not undo accepted ECB
releases or downgrade Dagster storage in place. A failed check/quarantine is
investigated using the existing ECB recovery procedure; changing schedulers
does not resolve a source-data incident.

Performance gate: using identical recorded source responses and staging
hardware, compare five warm CLI and five warm Dagster runs for no-op and
new-release cases. Record median/range, peak worker RSS, total service idle RSS,
SQL reads, lock contention and queue-to-start delay. Target added poll-step
overhead ≤2 seconds or 10% of CLI median, whichever is larger; idle-service
cost and cold start are reported separately. Investigate any unbounded scan,
overhead regression, or service memory pressure before activation.

## 8. Later scope and decision gates

RSS sensor: discovery only, stable run key from release identity, bounded
cursor, chronological releases, retry/recovery for a failed run with an already
used run key, and an independent missing-release schedule. A run key prevents
duplicate launch requests for that sensor; it does not guarantee successful
ingestion or cross-process exclusion. Deploying this mode replaces the two
discovery schedules; it must not silently create a second scheduler.

Archive orchestration: inventory and stage by known archive year/snapshot, keep
schema reviews/quarantine decisions, serialize chronological SCD2 rebuild and
promotion, and use the same live writer lock. No partition-per-year job may
independently mutate live SCD2 history. Require explicit reviewed promotion.

Macro orchestration: first wrap the main pipeline with `--no-ecb-assets` using
library calls, preserve due gating and bounded PostgreSQL Gold, then consider
separate Bronze/Silver/Gold assets. Define how existing Databricks schedules
interact before activating any new macro scheduler.

Open deployment inputs are hostname/access, capacity, protected config paths,
metadata backup target, and independent monitor route. These do not block
spec review or local implementation; they block the production activation
checkpoint until verified. Dagster version selection is a measured dependency
spike, not an unresolved architecture choice.

Always preserve the authority, point-in-time and quarantine contracts and
update evidence with actual results. Seek new direction before expanding v1
to other production backends, public UI hosting, paid services, or automatic
promotion. Never commit secrets, edit the vendored library, bypass signatures,
or mark an unexecuted production gate complete.

## 9. Sources and alternatives

Official references checked 2026-10-25 (the live site served differing version
labels across pages; resolve and record one compatible package set in Task 1):

- [OSS architecture](https://dagster.io/docs/deployment/oss/oss-deployment-architecture):
  webserver, daemon, code location and configurable PostgreSQL run storage.
- [Schedules](https://dagster.io/docs/guides/automate/schedules): cron and explicit timezone.
- [Sensors](https://dagster.io/docs/guides/automate/sensors): external polling and per-sensor run-key deduplication.
- [Asset metadata](https://dagster.io/docs/guides/build/assets/metadata-and-tags): observations and provenance metadata.
- [Asset checks](https://dagster.io/docs/guides/test/asset-checks): checks and UI results.
- [Run-status sensors](https://dagster.io/docs/guides/automate/sensors/run-status-sensors): failure notifications.
- [Concurrency](https://dagster.io/docs/guides/operate/managing-concurrency): deployment and cross-run controls.

Retaining only systemd remains a valid low-overhead fallback but lacks the
proposed combined run/check UI. An RSS sensor first adds cursor and failure
recovery complexity before the basic integration is proven. A full asset
rewrite expands the correctness surface beyond the scheduler migration.
Dagster+ may reduce host maintenance but introduces a separate service/cost
decision. The selected OSS integration accepts the operational cost of three
services and metadata storage in exchange for execution and data-health visibility.

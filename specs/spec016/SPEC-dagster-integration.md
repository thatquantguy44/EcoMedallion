# Module: dagster-integration

Status: Draft for review. Parent: [Spec 016](README.md).
Depends on: [outbound-network](SPEC-outbound-network.md),
[alert-email](SPEC-alert-email.md).

## Objective and relationship to Spec 013

Provide optional Dagster OSS jobs for the main Bronze-to-Gold pipeline, ECB
eligible-assets polling, warehouse health, Gold-only rebuild and metadata
reconciliation. Preserve the CLI as a supported entry point and make
Dagster outcomes reflect required-stage failures as well as series outcomes.
The user confirmed this initial scope on 2026-11-04, with detailed per-table
assets deferred. The user subsequently accepted warehouse health, Gold-only
and metadata reconciliation jobs and requested default-on FOMC refresh for
the full pipeline. These workload additions expand
[Spec 013](../spec013/README.md), which scoped its first release to ECB only.

Retain Spec 013's ECB shared execution service, authoritative PostgreSQL guard,
writer-session advisory lock, accepted-release materialization semantics,
calendar-aware checks, disabled schedules, independent daemon monitoring,
deployment proof and rollback requirements. Its package candidate research is
historical input, not a clean-install proof. None of that draft is implemented
merely because it is referenced here.

Initial scope is coarse jobs and truthful events, not a per-series/table
dependency graph or rewriting Bronze/Silver/Gold transformations as Dagster ops.
RSS triggering, automatic archive promotion, granular assets and partitioned
backfills remain deferred. No new warehouse schema is required by this spec.

## Runtime and execution contracts

- Add an optional `dagster` extra and reproducible constraints on Python 3.12.
  Start by verifying Spec 013's candidate set (`dagster==1.13.26`,
  `dagster-webserver==1.13.26`, `dagster-postgres==0.29.26`); change only after
  recording the compatibility reason. Core installs continue to omit Dagster.
- Keep definitions under `src/fred_pipeline/orchestration/`. Importing them
  must not open a warehouse, resolve secrets, select routes, fetch data or send
  email. Deployment processes share explicit instance configuration; production
  metadata uses a separate PostgreSQL database/role from the business warehouse.
  [Dagster instance guidance](https://docs.dagster.io/deployment/oss/oss-instance-configuration).
- Introduce shared library execution services rather than importing CLI-private
  functions or invoking a shell subprocess. Preserve CLI options, output, exit
  codes and behavior when extracting these services. Characterize both data and
  lifecycle behavior before refactoring.
- Execute each initial data workload within one worker operation. Initialize
  networking there, before outbound work and business writes, and share the
  plan through all its clients and terminal alerts. This avoids serializing
  proxy credentials or rerunning preflight in multiple Dagster step processes.
- Resources resolve existing pipeline/manifest/warehouse/alerting config from
  protected server-side paths. Expose environment, workload selection and safe
  flags in job config; never expose API keys, DSNs, tokens, proxy credentials
  or token-cache contents in Dagster launch config or event metadata.

## Jobs, results and concurrency

| Job | Contract |
| --- | --- |
| `macro_pipeline_job` | Run the existing manifest-driven main pipeline through a shared execution service; FOMC refresh is on and ECB polling is off by default |
| `ecb_eligible_assets_job` | Run the shared ECB polling service and health assessment with Spec 013 authority/writer exclusion |
| `ecb_eligible_assets_health_job` | Read-only accepted-state checks; no source polling or ingestion writes |
| `warehouse_health_job` | Read-only all-source staleness, missing-data, persisted DQ and configured critical Gold relation checks; no source fetching or Gold rebuild |
| `gold_rebuild_job` | Rebuild Gold from persisted inputs, without source extraction; fail truthfully on exceptions or table-level failure results |
| `metadata_reconciliation_job` | Compare FRED manifest metadata with FRED's catalog and reuse all-source stored-data staleness checks; report drift without automatically changing manifests |

The main job accepts existing environment/manifest, series/source filtering,
full/incremental and Gold/calendar options through validated configuration.
Do not let users select arbitrary shell commands or uncontrolled file paths.
The separately invoked CLI keeps its present default ECB hook unless an
operator explicitly uses `--no-ecb-assets` during scheduler cutover.

### Full-run FOMC refresh

Set `refresh_fomc_calendar: true` by default in the full Dagster job and the
CLI `run` entry point, including routine incremental runs. This behavior is
independent of `--full`, which controls source history extraction. Retain
`--refresh-fomc-calendar` as an explicit enable switch and add
`--no-refresh-fomc-calendar` as its disable counterpart. Explicit CLI/job
settings take precedence over the shared full-run default. Lower-level library
callers may still explicitly disable refresh for offline/maintenance execution.
This is an approved change to the old opt-in CLI default; preserve other CLI
options and exit behavior during service extraction.

Reuse the existing `fomc_calendar` stage before Gold so newly added dates are
available to FOMC Gold builders in that same run. Retain the existing stage
eligibility rules: a dry run without a warehouse, disabled Gold, or a run with
no successful extraction does not mutate the calendar or claim a refresh.
Report skipped/not-run stages clearly. An explicit opt-out still performs the
offline calendar health check when the pre-Gold stage runs.

Refresh is additive only: never remove moved/vanished meetings automatically.
Use the configured calendar path (`FRED_FOMC_CONFIG_FILE` or current default),
validate candidate content and atomically replace the file. Deployment must
provide a writable calendar path that Gold also reads. Do not create commits,
open PRs, or rewrite a read-only release checkout implicitly. Local default
paths retain the existing uncommitted-file behavior.

Register the Fed-page origin in startup networking as advisory. The refresh
fetch uses the run-selected Requests transport, without a second route probe
or a Selenium escape path. If preflight/fetch/parse/write fails, retain the
existing calendar and record the refresh failure; continue the data work using
the existing calendar-health rules. A warning yields the existing `partial`
domain verdict, mapped to failed Dagster execution by this spec. Missing,
malformed or expired local calendars remain visible failures; fallback must
not imply valid FOMC coverage. Preserve current CLI exit semantics.

Stage metadata must distinguish refresh requested, attempted, updated,
successful/no-change, failed and deliberately skipped. A successful fetch with
no new meetings is not an error or a claim that the file changed.

### Maintenance job contracts

Use shared library services with `WarehouseFactory` and the configured backend,
including PostgreSQL, instead of invoking the existing CLI handlers. The
standalone Gold/reconciliation handlers currently select SQLite/Spark directly;
that behavior is not PostgreSQL parity evidence. Preserve their legacy modes
while adapting their execution logic, and test against isolated backends.

`warehouse_health_job` reuses stored-observation staleness logic without
requiring a FRED API key. Report missing data separately from stale data;
distinguish ingestion/capture age from observation-period age and honor the
existing per-series cadence rules. Read persisted latest-run DQ results and
check configured required Gold relations/columns/meaningful row expectations
with bounded queries, not full-history Python scans. Thresholds and the set
of required relations are validated settings; legitimate empty optional tables
are not blanket failures. A missing warehouse, missing required relation,
missing required evidence or unreadable state is an explicit failure/unknown
check result, never a green empty report. Use a consistent read snapshot where
supported, and record the inspected run/watermark. Publish checks/observations
without business writes or initialization DDL. Read-only access must be real,
including warehouse construction. Use existing coarse asset keys; defer
inventing individual table assets/lineage.

`gold_rebuild_job` calls the shared Gold builder once, with configurable
supported rebuild options and the same authority guard as the main job.
It never re-fetches source observations or release calendars. Run the offline
FOMC health check before FOMC-dependent Gold work; this maintenance job does
not inherit full-run refresh-on behavior. Any future opt-in remote refresh
must be explicit and use the same network/lifecycle contract. Publish a Gold
refresh event only after actual successful work, without claiming a fresh
source ingestion or an atomic all-table commit. Share result classification
with the main job so non-`ok` builder results cannot become success merely
because no exception escaped.

`metadata_reconciliation_job` reuses FRED metadata drift/lifecycle comparison
and stored-data staleness across all sources. FRED-only metadata comparison
must not be advertised as metadata reconciliation for every provider. Missing
FRED credentials fails this job explicitly; warehouse-only monitoring remains
available independently. Default to no persistence and no manifest changes.
Optionally persist through existing audit writers with an explicit setting;
do not introduce new warehouse schemas for this wrapper. Error-level drift
fails execution; warnings are recorded and routed through configured alert
policy. Preflight FRED once and respect existing rate limits. Reuse health-job
staleness logic rather than maintaining two definitions of stale data.

All three jobs return sanitized structured reports and share the configured
email profiles/policies. They are independently launchable. Health and
reconciliation can have independent stopped-by-default schedules, so a failed
main run cannot prevent monitoring. Gold-only is manual by default; operators
may opt into a schedule with the same overlap controls.

For the main job, compute a shared execution report from `RunStageTracker` and
`overall_verdict`; `EtlRun.status` alone is insufficient because Gold failures
can be swallowed. Report pipeline run id, Dagster run id, stage verdict,
source/series counts, sanitized network summary and delivery result.
Required-stage/preflight failure makes the Dagster job fail, including a Gold
failure with all series ingested. Define `partial` as failed execution in the
initial Dagster wrapper, retaining its distinct domain verdict in metadata;
this makes unattended partial loads visible. Normal CLI status/exit behavior
is preserved except for the requested FOMC refresh-on default described above.

Production main jobs require a configured authoritative warehouse and reject
backend mismatch or silent warehouse fallback before writes. Explicit local
development/dry runs may omit it, with observations instead of materializations.

Expose one logical `macro/pipeline` asset event after a successful required
stage sequence with an actual Gold refresh. A run with Gold deliberately
disabled, all work skipped, partial/failure verdict, or no authoritative
warehouse emits an observation/report, not a misleading Gold materialization.
Retain native per-series audit results even when the wrapper fails. Do not
claim every Gold table committed atomically or publish fake per-table lineage.
Existing standalone CLI runs do not need to write Dagster events.

For ECB retain Spec 013's `ecb/eligible_securities` asset: materialize each
accepted committed release; observe no-new-release/busy states. Quarantine and
failed signatures never materialize. A failure after earlier accepted releases
does not roll those releases back. The database remains the source of truth
after a crash between commit and Dagster event emission.

Use one shared per-destination concurrency pool for main-pipeline and Gold-only
writes, including reconciliation when audit persistence is enabled; separate
job-name limits would still allow these jobs to collide. Queue-limit ECB
polling to one as well. Dagster limits do not protect CLI/systemd writers.
Require the
ECB writer-session lock from Spec 013 for every ECB writer. For the main job,
initial coexistence requires operational exclusion: disable/drain existing
main timers/jobs and prohibit manual same-destination writes during a run.
Automatic mixed-entry-point main writer locking is a separate change; do not
claim a cross-process main lock already exists. Read-only health checks must
not mutate schemas during warehouse initialization.

Keep whole-job automatic retries disabled initially: existing HTTP retries
handle transient calls, and data jobs are not assumed safe to replay in full.

## Alerts, scheduling and operations

Use the shared business email layer for normal main/ECB/maintenance outcomes.
Record the
delivery outcome durably in sanitized orchestration metadata. A failure sensor
handles missing terminal reports such as initialization failure or worker crash,
using the same profiles/network policy. Its independent execution is a new
invocation and may preflight its own alert-only destinations.

Suppress a sensor duplicate when a terminal business notification was already
accepted. Persist a correlation/deduplication marker so sensor reevaluation
does not resend. Do not replay an `unknown` delivery outcome automatically.
The crash window between provider acceptance and marker persistence cannot
guarantee exactly-once delivery; document the possible duplicate and the
reconciliation procedure as Spec 013 requires.

Provide stopped ECB schedules matching the existing 17:45, 18:25 and 21:20
Europe/Berlin weekdays. Main scheduling is opt-in and configurable with cron,
IANA timezone and overlap policy; do not invent an active production cadence.
All schedules and sensors ship stopped. Account for DST, missed ticks and
stale queued runs using the original ECB operating contract.

Local development may use a private local instance. Production deployment
uses the private Linux/systemd shape in Spec 013, separate metadata storage,
one daemon and independent monitoring for daemon/host outage. Before cutover,
disable/drain old triggers and ensure main schedules cannot invoke ECB a second
time. Rehearse backup/restore with automation stopped. Roll back by stopping
and draining Dagster first, then restoring previous triggers. Preserve all
warehouse authority/verification gates and existing service files.

## Structure, commands and acceptance

Proposed modules: `orchestration/definitions.py`, `resources.py`, `macro_jobs.py`,
`maintenance_jobs.py`, ECB jobs/checks/schedules/notification modules from
Spec 013, and shared
execution reports outside the Dagster-only package. Tests use pytest plus
Dagster's in-process execution with injected sources, warehouse and transports.
Follow current dataclasses, lazy optional imports and explicit resource ownership.

Proposed commands after the extra and definitions exist (not implemented):

```bash
python -m pip install -e '.[postgres,dagster,dev]' \
  -c requirements-dagster.lock.txt
dagster dev -m fred_pipeline.orchestration.definitions --host 127.0.0.1
python -m pytest tests/test_dagster_*.py
```

| ID | Acceptance evidence |
| --- | --- |
| DAG-01 | Clean core-only and optional Python 3.12 installs, `pip check`, CLI/import smoke and side-effect-free definitions import pass |
| DAG-02 | Fixture main runs agree with CLI data/audit behavior; Gold/preflight/partial failures map truthfully; deliberate skips do not materialize Gold |
| DAG-03 | CLI and Dagster ECB accepted/no-op/quarantine/multi-release/crash cases meet Spec 013; independent PostgreSQL sessions prove every ECB writer lock path |
| DAG-04 | Exactly one startup plan per workload execution is used by all source clients and terminal alerts; separate runs and sensor executions reselect |
| DAG-05 | Normal outcome and crash-sensor notification ownership, persisted dedupe, ambiguous-delivery behavior and secret-free metadata are tested |
| DAG-06 | Stopped schedule defaults, DST, overlapping work and old-trigger cutover/rollback are proven; core CLI behavior remains compatible |
| DAG-07 | Isolated staging proves real proxy/direct networking, chosen mail receipt, metadata restore and independent daemon outage detection before production activation |
| DAG-08 | Warehouse-only health checks run without source credentials or source calls, use bounded consistent reads, detect stale/missing/failed-DQ/required-Gold problems, and make no writes including initialization DDL; enabled alerts may use their own advisory targets |
| DAG-09 | Gold-only rebuild agrees with direct builder behavior on supported backends, never fetches sources, reports table-level failures truthfully, and cannot overlap main writes to the same destination |
| DAG-10 | Metadata reconciliation preserves FRED-only drift semantics and all-source stored staleness, respects rate limits/network selection, defaults to no writes, and covers explicit audit persistence on PostgreSQL |
| DAG-11 | Full Dagster and CLI run defaults request FOMC refresh before Gold for incremental and forced-full modes; explicit opt-out and existing skipped-stage rules are tested; Gold sees new dates in the same run |
| DAG-12 | FOMC no-change/addition/fetch/parse/write/preflight failure fixtures prove stage reporting, atomic additive updates, retained local-calendar fallback, no Selenium bypass and existing verdict/CLI-exit behavior |

Characterize existing FOMC and reconciliation behavior before adapting it with:

```bash
python -m pytest tests/test_fomc_calendar_check.py tests/test_reconcile.py \
  tests/test_pipeline.py tests/test_cli_ecb_assets_default.py
```

Add focused maintenance-job, full-run default/opt-out and cross-job contention
tests during implementation. Use isolated PostgreSQL services for integration
tests, never production DSNs.
Retain Spec 013's production and performance evidence gates; this addendum does
not waive them. Final implementation slicing and CI changes follow spec review.

# Module: dagster-integration

Status: Draft for review. Parent: [Spec 016](README.md).
Depends on: [outbound-network](SPEC-outbound-network.md),
[alert-email](SPEC-alert-email.md).

## Objective and relationship to Spec 013

Provide optional Dagster OSS jobs for the main Bronze-to-Gold pipeline and ECB
eligible-assets polling. Preserve the CLI as a supported entry point and make
Dagster outcomes reflect required-stage failures as well as series outcomes.
The user confirmed this initial scope on 2026-11-04, with detailed per-table
assets deferred. The main-pipeline job is an explicit addition to
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
| `macro_pipeline_job` | Run the existing manifest-driven main pipeline through a shared execution service; ECB polling is off in this job by default |
| `ecb_eligible_assets_job` | Run the shared ECB polling service and health assessment with Spec 013 authority/writer exclusion |
| `ecb_eligible_assets_health_job` | Read-only accepted-state checks; no source polling or ingestion writes |

The main job accepts existing environment/manifest, series/source filtering,
full/incremental and Gold/calendar options through validated configuration.
Do not let users select arbitrary shell commands or uncontrolled file paths.
The separately invoked CLI keeps its present default ECB hook unless an
operator explicitly uses `--no-ecb-assets` during scheduler cutover.

For the main job, compute a shared execution report from `RunStageTracker` and
`overall_verdict`; `EtlRun.status` alone is insufficient because Gold failures
can be swallowed. Report pipeline run id, Dagster run id, stage verdict,
source/series counts, sanitized network summary and delivery result.
Required-stage/preflight failure makes the Dagster job fail, including a Gold
failure with all series ingested. Define `partial` as failed execution in the
initial Dagster wrapper, retaining its distinct domain verdict in metadata;
this makes unattended partial loads visible. Normal CLI status/exit behavior
is preserved unless separately approved.

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

Queue-limit the main workload to one per destination; queue-limit ECB polling
to one as well. Dagster limits do not protect CLI/systemd writers. Require the
ECB writer-session lock from Spec 013 for every ECB writer. For the main job,
initial coexistence requires operational exclusion: disable/drain existing
main timers/jobs and prohibit manual same-destination writes during a run.
Automatic mixed-entry-point main writer locking is a separate change; do not
claim a cross-process main lock already exists. Read-only health checks must
not mutate schemas during warehouse initialization.

Keep whole-job automatic retries disabled initially: existing HTTP retries
handle transient calls, and data jobs are not assumed safe to replay in full.

## Alerts, scheduling and operations

Use the shared business email layer for normal main/ECB outcomes. Record the
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
ECB jobs/checks/schedules/notification modules from Spec 013, and shared
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

Use isolated PostgreSQL services for integration tests, never production DSNs.
Retain Spec 013's production and performance evidence gates; this addendum does
not waive them. Final implementation slicing and CI changes follow spec review.

# Dagster Development Runtime

Last verified: 2026-11-13

Dagster is optional. Seven manual jobs now use existing library workloads with
independent validated configuration. Three ECB schedules are defined and ship
stopped. Production deployment, cutover, crash-alert deduplication, missed-tick
recovery and independent host monitoring remain outstanding. The complete
operating requirements are in [Spec 016](../../specs/spec016/SPEC-dagster-integration.md).

## Install the pinned Python 3.12 runtime

From the repository root, in a development virtual environment:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[postgres,dagster,dev]' \
  -c requirements-dagster.lock.txt
.venv/bin/python -m pip check
.venv/bin/python -m fred_pipeline --help
.venv/bin/dagster --version
.venv/bin/dagster-webserver --version
```

Reuse an existing Python 3.12 environment rather than replacing it. The
constraints record the full resolved package set for this extra combination;
installing the core package alone does not install Dagster. Deployments must
use their own protected configuration and credentials. The CLI checks above do
not open a warehouse or contact a data provider.

## Configure a development instance

Run from the repository root. Keep the existing pipeline/warehouse configuration
and source credentials in the worker environment; launch YAML contains no DSNs,
API keys, arbitrary paths or shell commands.

```bash
export DAGSTER_HOME="$PWD/.dagster"
mkdir -p "$DAGSTER_HOME"
cp resources/dagster/dagster.yaml "$DAGSTER_HOME/dagster.yaml"
.venv/bin/dagster job list -m fred_pipeline.orchestration.definitions -a defs
.venv/bin/dagster dev -m fred_pipeline.orchestration.definitions -a defs --host 127.0.0.1
```

The example instance sets each concurrency pool to one. Main, Gold rebuild,
metadata reconciliation and FOMC refresh share `warehouse_writer`; ECB polling
uses `ecb_writer`. This conservatively serializes those jobs across destinations.
Health jobs can run independently. These pools require the configured instance
and queued launcher; direct CLI/in-process executions must be serialized by the
operator. Existing main CLI/systemd writers also require operational exclusion.

The ECB PostgreSQL session lock additionally protects shared polling, direct
release ingestion, live archive promotion and warehouse initialization across
entry points. A Dagster contender reports `busy` without source access. ECB
Dagster execution opens an already initialized PostgreSQL schema without DDL;
provision it using the existing warehouse setup before polling. Local SQLite
remains a development option and retains its existing locking semantics.

## Jobs and launch files

Each YAML is the job's **flat launch configuration**: select the job in Launchpad
and paste its YAML, or pass it with `-c`. Defaults apply when fields are omitted.

| Job | Purpose | Example launch file |
| --- | --- | --- |
| `macro_pipeline_job` | Main Bronze/Silver/Gold run; FOMC refresh on, ECB separate | [dev](../../resources/dagster/jobs/macro_pipeline_job.dev.yaml), [PostgreSQL](../../resources/dagster/jobs/macro_pipeline_job.postgres.yaml), [local](../../resources/dagster/jobs/macro_pipeline_job.local.yaml) |
| `ecb_eligible_assets_job` | Signed ECB MID security-level eligible-assets polling | [PostgreSQL](../../resources/dagster/jobs/ecb_eligible_assets_job.postgres.yaml) |
| `ecb_eligible_assets_health_job` | Accepted-release, signature evidence, counts, history, coverage and source-no-op checks | [PostgreSQL](../../resources/dagster/jobs/ecb_eligible_assets_health_job.postgres.yaml) |
| `warehouse_health_job` | All-source stored staleness/missingness, capture age, latest-run DQ and required Gold evidence | [PostgreSQL](../../resources/dagster/jobs/warehouse_health_job.postgres.yaml) |
| `gold_rebuild_job` | Rebuild persisted inputs, with offline FOMC validation | [PostgreSQL](../../resources/dagster/jobs/gold_rebuild_job.postgres.yaml) |
| `metadata_reconciliation_job` | FRED catalog metadata drift plus all-source stored staleness; persistence opt-in | [PostgreSQL](../../resources/dagster/jobs/metadata_reconciliation_job.postgres.yaml) |
| `fomc_refresh_job` | Independently refresh and assess the FOMC calendar | [dev](../../resources/dagster/jobs/fomc_refresh_job.dev.yaml) |

```bash
# Same core workload as `python -m fred_pipeline run --env dev --no-ecb-assets`.
# The repository's dev warehouse configuration already selects PostgreSQL.
.venv/bin/dagster job execute -m fred_pipeline.orchestration.definitions -a defs \
  -j macro_pipeline_job -c resources/dagster/jobs/macro_pipeline_job.dev.yaml

# Require PostgreSQL explicitly, with no fallback to another backend.
.venv/bin/dagster job execute -m fred_pipeline.orchestration.definitions -a defs \
  -j macro_pipeline_job -c resources/dagster/jobs/macro_pipeline_job.postgres.yaml

.venv/bin/dagster job execute -m fred_pipeline.orchestration.definitions -a defs \
  -j ecb_eligible_assets_job -c resources/dagster/jobs/ecb_eligible_assets_job.postgres.yaml

.venv/bin/dagster job execute -m fred_pipeline.orchestration.definitions -a defs \
  -j fomc_refresh_job -c resources/dagster/jobs/fomc_refresh_job.dev.yaml
```

Main configuration supports series/source/excluded-source lists, incremental or
`full` extraction, due-date skipping, optional Gold, FOMC refresh, explicit dry
runs and bounded extraction/rate tuning. Production dry runs are rejected.
`backend: configured` resolves the environment's primary backend; explicit
`postgres` or `local` chooses that configured profile. Jobs never use fallback
backends. Production main overrides must match the configured primary backend;
production ECB requires its configured authoritative PostgreSQL backend.

Server-side configuration remains separate:

| Environment variable | Setting |
| --- | --- |
| `FRED_CONFIG_FILE` | Protected pipeline/network/source configuration |
| `FRED_WAREHOUSE_CONFIG` | Warehouse profiles; defaults to `config/warehouse.yml` |
| `FRED_DAGSTER_MANIFESTS` | Manifest file/directory; defaults to `manifests` |
| `FRED_FOMC_CONFIG_FILE` | Calendar shared by refresh and Gold; defaults to `config/fomc.yml` |
| `FRED_ALERTING_FILE` | Existing main/ECB business alert policies and profiles |
| Existing provider/DSN variables | Credentials resolved by existing pipeline/warehouse code |

For a writable deployed calendar, set `FRED_FOMC_CONFIG_FILE` to a protected
runtime copy. Refresh adds dates atomically, preserves configured dates missing
upstream, and creates no commits or PRs. `refresh: false` on the standalone job
performs only the offline check. Gold-only execution always uses an offline
check. Failed/partial refreshes fail the job and retain the existing calendar.

Jobs emit coarse asset observations and materialize only actual successful Gold
refreshes, committed ECB releases, or changed FOMC calendars. Accepted ECB
commits remain visible if a later release or health assessment fails. Partial
pipeline outcomes fail Dagster while preserving domain status. Whole-job retries
are disabled. Existing main/ECB notifications remain in their library paths;
maintenance outcomes are available in Dagster metadata. A crash failure sensor,
maintenance email policy integration and native UI asset-check definitions remain
production follow-up work.

Read-only health and default reconciliation open SQLite with `mode=ro` and
PostgreSQL in a read-only repeatable-read transaction. They perform no schema
setup or query-log writes. Warehouse health distinguishes observation-period
staleness from capture age, requires latest-run DQ evidence and checks required
Gold rows; absent state fails rather than yielding an empty healthy report.
Missing/unreadable relations appear as missing evidence. ECB health inspects
stored signature evidence without re-fetching artifacts or loading security
payloads; full cryptographic re-verification remains `ecb-assets verify`.

## Extend the job catalog

Add a schema to `orchestration/config.py`, a library workload method on
`JobRuntime`, and an explicit `JobSpec` row in `orchestration/definitions.py`.
Add a launch file under `resources/dagster/jobs/` and an in-process test with
isolated sources/storage. The common builder supplies config mapping, sanitized
reports, event emission and failure handling. Workload implementations stay
Dagster-independent and reuse `FredPipeline.run_from_manifest`, `execute_ecb_poll`
and warehouse builders. Definitions imports resolve no secrets or external state.
See [Dagster run configuration](https://docs.dagster.io/guides/operate/configuration/run-configuration)
and [concurrency pools](https://docs.dagster.io/guides/operate/managing-concurrency).

## Verification evidence

On 2026-11-05, clean Python 3.12 environments were created from a source copy,
without system site packages. The Linux check used Python 3.12.13 in an
isolated container; the local check used Python 3.12.0. The following passed:

- Core-only install, `pip check`, pipeline CLI help and imports, and an explicit
  check that the Dagster module was absent.
- A clean `.[postgres,dagster,dev]` install and another clean install with the
  recorded constraints; `pip check`, pipeline CLI help, package imports, and
  Dagster and webserver version commands passed. The Linux locked reinstall
  resolved exactly the versions in the constraints file; the constrained local
  reinstall also matched that set.
- Resolved versions: `dagster==1.13.26`, `dagster-webserver==1.13.26`, and
  `dagster-postgres==0.29.26`.

The 2026-11-05 record above covers dependency installation only. On 2026-11-13,
the manual job slice was additionally exercised with definition/import and
launch-file validation, in-process SQLite/source fixtures, a real multiprocess
offline FOMC CLI run and an isolated PostgreSQL 16 container. PostgreSQL checks
cover main ingestion, actual Gold rebuild, signed ECB fixture ingestion and
no-op event behavior, read-only repeatable-read monitoring, all live writer
boundaries, nested ownership, commit survival and process-death release.
The complete regression run passed **1,892 tests with 1 optional skip**; lint,
documentation and whitespace checks passed.
This includes 52 Dagster/runtime/ECB health and writer-lock tests. Expanded
PostgreSQL health-job checks cover healthy and missing evidence without source
credentials or query-log writes, plus recovery after a missing Gold relation.
They also guard the savepoint naming fix required by the PostgreSQL SQL adapter.

```bash
.venv/bin/python -m pytest tests/test_dagster_jobs.py tests/test_job_runtime.py \
  tests/test_ecb_health.py tests/test_ecb_writer_boundaries.py
# Set TEST_ECB_POSTGRES_DSN to an isolated test database with CREATEDB privileges.
# The job tests create/drop uniquely named databases; never select a business DB.
.venv/bin/python -m pytest tests/test_dagster_postgres.py tests/test_ecb_writer_lock.py
```

The dedicated CI job installs the pinned optional dependencies on Python 3.12
and uses a disposable PostgreSQL service. Production deployment, scheduler
activation, missed-tick recovery, monitoring and real notification delivery
still require their own evidence before cutover.

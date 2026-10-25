# Dagster Orchestration Handoff

Status: Specification and bootstrap instructions committed; T1 package metadata
research complete, runtime implementation and verification pending. No tracked
dependencies, jobs, services or schedules changed.

Last verified: 2026-10-25 (repository/specification consistency, package metadata
and document links; Dagster runtime and production gates remain unverified).

Branch: `spec012-dagster-orchestration`, created from `origin/main` at `5077ef8`.

Baseline documentation commit: `72703d5` — specification, plan, checklist,
handoffs and README installation commands. Inspect current Git status/log on
pickup; this hash is a checkpoint, not a claim about the latest branch head.

This is the canonical pickup document for Spec 012. Read the next-task section
below, then [Spec 012](../../specs/spec012/README.md),
[the implementation plan](../../tasks/plan.md) and
[the task checklist](../../tasks/todo.md).

## Next task: T1

**Complete T1 — Resolve the optional runtime.** Do this before starting T2.
The specification remains a draft; documentation commits and published package
compatibility are not implementation acceptance or production approval.

### Already established

- Branch and first-release scope: optional Dagster OSS integration for ECB
  forward polling, with PostgreSQL authoritative.
- PyPI metadata checked on 2026-10-25 identifies `dagster==1.13.26`,
  `dagster-webserver==1.13.26` and `dagster-postgres==0.29.26` as the matching
  candidate set. Both companion packages require `dagster==1.13.26`; all three
  declare Python `>=3.10,<3.15`. Python 3.12 is the selected runtime.
- The [README bootstrap instructions](../../README.md#install-the-optional-dagster-development-runtime)
  install those candidates explicitly with `.[postgres,dev]`. `.venv/` is ignored.
- Package metadata and installation instructions are verified facts; no clean
  install, transitive dependency lock, `pip check`, import smoke or core-only
  isolation result has been recorded. Do not infer those checks passed.

### Remaining T1 work

1. Inspect Git status and `pyproject.toml`; inspect any existing `.venv` before
   reusing it. Preserve existing environments and user changes. Read the repo's
   pinned workflow guidance in `AGENTS.md`. Use the candidate versions above;
   investigate a resolver conflict rather than silently selecting newer ones.
2. Add the optional `dagster` dependency group to `pyproject.toml`. Resolve the
   full dependency set on Python 3.12 and create the planned reproducible
   `requirements-dagster.lock.txt` constraints file, including transitive pins.
   Keep normal/core installs free of Dagster dependencies.
3. In separate clean temporary environments, install the core project and the
   project with `.[postgres,dagster,dev]` plus the constraints file. Verify
   dependency consistency, CLI help and imports. Use the README's current
   explicit-package install only for bootstrap; it is not the final lock proof.
4. Add a focused optional-dependency regression test and the initial
   `docs/deployment/dagster_runbook.md`. Record Python/package versions and actual
   commands/results there. Do not create jobs, send live mail, poll ECB or
   connect to production for T1.
5. Update README installation commands to use the new extra/constraints only
   once they exist and pass. Update this handoff, spec status, plan and checklist
   together. Mark T1 complete only with its evidence, then name T2 as next.

Expected T1 files: `pyproject.toml`, `requirements-dagster.lock.txt`,
`tests/test_dagster_optional_dependencies.py`, the initial Dagster runbook, and
the README install section. Update progress documents after verification.
These files, apart from `pyproject.toml` and README, are currently planned.

### T1 completion evidence

Record the following for **both** clean environments, using their own Python
executables (not whichever `python` happens to be active):

```bash
python --version
python -m pip check
python -m fred_pipeline --help
```

For core-only, verify `importlib.util.find_spec('dagster') is None` and that
the pipeline imports. For the Dagster environment, import `dagster`,
`dagster_webserver`, and `dagster_postgres`, print their installed versions via
`importlib.metadata`, and run `dagster --version` and
`dagster-webserver --version`. Reinstall from the constraints in a clean
environment to prove reproducibility. Run the focused optional-dependency test,
`python3 scripts/check_doc_staleness.py`, and `git diff --check`.

T1 does not require loading `fred_pipeline.orchestration.definitions`: that
module is planned for T4. AC-01's definitions-without-side-effects proof remains
open until T4 even after T1's runtime portion passes.

### After T1

T2 extracts the shared ECB execution service from `_cmd_ecb_assets()` and
`_poll_ecb_assets_for_run()` in `src/fred_pipeline/cli.py` while preserving
authority guards, JSON, defaults, exit codes and alerts. T3 then proves writer
exclusion before T4 introduces a Dagster writer. Do not skip directly from
package installation to live polling or scheduler activation.

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

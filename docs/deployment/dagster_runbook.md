# Dagster Development Runtime

Last verified: 2026-11-05

Dagster is optional. The repository currently provides the dependency foundation;
Dagster jobs, schedules, writer exclusion and production deployment remain
unimplemented. Installing the extra does not start a daemon, poll ECB, send
mail or activate a schedule. The expanded job scope and remaining requirements
are in [Spec 016](../../specs/spec016/SPEC-dagster-integration.md).

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

This is dependency evidence, not a Dagster job, PostgreSQL writer-lock,
definition import, scheduler, or production deployment proof. Those checks
remain required before enabling orchestration.

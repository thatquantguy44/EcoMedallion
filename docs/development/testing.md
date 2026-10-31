# Testing and baseline evidence

Spec 005 T-001 defines the canonical clean-environment bootstrap for the
offline test baseline. It deliberately does not add marker selection, coverage
thresholds, changed-code gates, or expected-skip enforcement; those are later
Spec 005 tasks.

## Clean bootstrap

Run from the repository root with Python 3.10, 3.11, or 3.12:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[dev]"
```

`requirements-dev.txt` remains a compatibility entry point and installs the
same editable `dev` extra. The canonical command above is preferred because
the package metadata is the single dependency declaration.

## Reproduce the offline baseline

The baseline excludes the independently provisioned Spark/Delta suite and
does not use provider credentials or public-network requests:

```bash
mkdir -p artifacts/test-baseline
.venv/bin/python -m pytest --strict-markers -m unit \
  --ignore=tests/test_spark_integration.py \
  --junitxml=artifacts/test-baseline/unit-junit.xml \
  --cov=fred_pipeline --cov-branch \
  --cov-report=xml:artifacts/test-baseline/unit-coverage.xml \
  --cov-report=json:artifacts/test-baseline/unit-coverage.json
```

The generated `artifacts/test-baseline/` directory is intentionally ignored.
It records the actual result for a run rather than claiming a passing baseline
in source control. CI uploads the corresponding JUnit and coverage artifacts
for each supported Python unit job, even when the test command fails.

## Initial recorded baseline

The clean command above was run on 2026-10-31 with Python 3.12. It collected
1,505 tests, with 4 failures, no skips, no errors, and 75.433% branch-enabled
coverage. This is a **recorded failing baseline**, not release evidence. The
four failures are the already-known stale `dim_series` source-shape assertions
and the missing `mmkt_rate_snapshot` Power BI catalog row. Expected-skip
registry enforcement, coverage ratchets, and repair of the failures are later
Spec 005 work.

## Backend suites

Spark/Delta and PostgreSQL are separate integration commands because each has
additional runtime requirements. Their evidence is not yet combined with this
baseline; coverage combination and required-job policy are Spec 005 T-003 and
T-013/T-014 work.

```bash
# Spark/Delta: install the pinned CI dependencies first.
.venv/bin/python -m pip install "pyspark==3.5.3" "delta-spark==3.2.0"
.venv/bin/python -m pytest --strict-markers -m spark

# PostgreSQL: use the project's isolated test service configuration.
.venv/bin/python -m pip install -e ".[postgres,dev]"
.venv/bin/python -m pytest --strict-markers -m postgres
```

## Tier and network policy

Pytest registers the Spec 005 markers in `pyproject.toml` and all CI commands
use `--strict-markers`. During incremental migration, collection assigns
`postgres` to `test_postgres_warehouse.py`, `spark` to
`test_spark_integration.py`, and `unit` to all other tests unless they are
explicitly marked `external`. New tests should declare their applicable marker
at the test or module boundary rather than relying on this compatibility rule.

The unit command also explicitly ignores the legacy Spark module because its
module-level optional-dependency skip happens before marker filtering. The
Spark job is the sole required collector for that module; its later Spec 005
migration will remove this compatibility exception.

The `unit` tier replaces `socket.socket` with a denial guard. Required tests
therefore fail immediately if they try to open a network connection. Only
`external`, `postgres`, and `spark` tests bypass that guard; their commands
remain separately provisioned and are never selected by the unit CI job.

Required tier jobs must not introduce `skip` or `xfail` as a way to hide a
failure. The remaining T-002 slice will add an owned expected-skip registry
(node id, reason, owner, and review date) and enforce it in CI. There are no
expected skips in the strict unit command today.

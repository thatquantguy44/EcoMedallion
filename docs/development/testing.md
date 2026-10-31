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
.venv/bin/python -m pytest --ignore=tests/test_spark_integration.py \
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
1,521 tests, with 4 failures, 16 skips, no errors, and 75.775% branch-enabled
coverage. This is a **recorded failing baseline**, not release evidence. The
four failures are the already-known stale `dim_series` source-shape assertions
and the missing `mmkt_rate_snapshot` Power BI catalog row. Marker/skip policy,
coverage ratchets, and repair of those failures are later Spec 005 work.

## Backend suites

Spark/Delta and PostgreSQL are separate integration commands because each has
additional runtime requirements. Their evidence is not yet combined with this
baseline; coverage combination and required-job policy are Spec 005 T-003 and
T-013/T-014 work.

```bash
# Spark/Delta: install the pinned CI dependencies first.
.venv/bin/python -m pip install "pyspark==3.5.3" "delta-spark==3.2.0"
.venv/bin/python -m pytest tests/test_spark_integration.py

# PostgreSQL: use the project's isolated test service configuration.
.venv/bin/python -m pip install -e ".[postgres,dev]"
.venv/bin/python -m pytest tests/test_postgres_warehouse.py
```

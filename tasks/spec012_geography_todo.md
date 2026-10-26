# Spec 012 PostgreSQL Geography Tasks

Status: **Draft — UUID-text `mapping_id` approved; await implementation-plan approval before coding.**

The Dagster work remains in `tasks/plan.md` and `tasks/todo.md`. This checklist
is intentionally separate for [Spec 012](../specs/spec012/README.md).

## Phase 1 — Contract and PostgreSQL foundation

- [ ] **G1 — Build the deterministic config-candidate contract** (M; no dependency)
  - Acceptance: the USA/CAN/AUS inflation and policy entries resolve once to
    the six confirmed manifest sources; all emitted rows are candidates; missing,
    duplicate, mismatched, or unselected entries fail closed.
  - Verify: `python -m pytest -q tests/test_series_geography.py` covers
    candidate construction, config evidence hashing, source resolution, and
    refusal paths without PostgreSQL or network access.
  - Likely files: `src/fred_pipeline/catalogs/series_geography.py`, a focused
    config helper, `tests/test_series_geography.py`.

- [ ] **G2 — Add PostgreSQL-only geography storage and approved-country view** (M; G1)
  - Acceptance: additive bootstrap creates `meta.series_geography` and
    `meta.v_approved_series_country`; source-aware validation occurs before
    mutation; one approved country per `(source, series_id)` is enforced;
    candidates/rejections/superseded rows cannot appear in the view.
  - Verify: disposable PostgreSQL integration tests prove idempotency,
    transaction rollback, approval/supersession, conflict refusal, and view
    semantics.
  - Likely files: `src/fred_pipeline/io/postgres_store.py`,
    `src/fred_pipeline/io/series_geography_store.py`, focused tests.

### Checkpoint A

- [ ] Focused unit and PostgreSQL integration tests, repository suite, and
  `ruff check src/fred_pipeline tests` pass.
- [ ] Review the physical table/view DDL and six generated candidate records
  before adding a mutating operator interface.

## Phase 2 — Operator review workflow

- [ ] **G3 — Add seed/report/review CLI commands** (M; G2)
  - Acceptance: commands require PostgreSQL, act on an exact evidence hash,
    and report candidate/review results truthfully without FRED polling or
    observation/Gold changes.
  - Verify: CLI tests use a disposable PostgreSQL database and the runbook
    includes an exact smoke command.
  - Likely files: `src/fred_pipeline/cli.py`, geography modules, CLI tests,
    dictionary/runbook.

### Checkpoint B

- [ ] Manually inspect the approved-only country query for all six records;
  confirm no unreviewed record is visible.

## Phase 3 — Separate FRED evidence increment

- [ ] **G4 — Add bounded FRED geography-tag evidence and drift reporting** (M; G3)
  - Acceptance: selected FRED series create provenance-backed candidates only;
    changed tags create drift evidence and cannot revoke an approval.
  - Verify: fixture tests cover response retention, crosswalk, unknown tags,
    and drift; optional live proof is documented separately.

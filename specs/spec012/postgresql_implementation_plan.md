# Spec 012 PostgreSQL Implementation Plan

Status: **G1/G2 implemented 2026-10-26 — explicit operator workflow (G3) is next.**

This plan delivers the first release of the reviewed series-geography contract
in PostgreSQL only. It does not add a SQLite or Delta implementation.

## Discovery completed

The initial six seed candidates from `config/global_series.yml` resolve exactly
once in the checked-in manifests and in the local PostgreSQL
`meta.fred_series` catalog:

| ISO3 | Product | Series ID | Registered source |
| --- | --- | --- | --- |
| USA | inflation | `CPIAUCSL` | `fred` |
| USA | policy rate | `FEDFUNDS` | `fred` |
| CAN | inflation | `CANCPIALLMINMEI` | `fred` |
| CAN | policy rate | `BIS:CA` | `bis` |
| AUS | inflation | `AUS:FP.CPI.TOTL.ZG` | `worldbank` |
| AUS | policy rate | `BIS:AU` | `bis` |

The local PostgreSQL parent table is `meta.fred_series`. It has all six rows,
is keyed by `series_id` only, and has no `source` column. The current metadata
sync projects manifest fields into that table and therefore cannot establish a
database foreign key for the proposed source-aware `(source, series_id)`
association.

## Proposed PostgreSQL boundary

- Add a PostgreSQL-only `meta.series_geography` child table and a read-only
  `meta.v_approved_series_country` view.
- Keep `meta.fred_series` unchanged. Validate a candidate's `(source,
  series_id)` against parsed manifests at write time; do not claim a database
  foreign key that the existing parent key cannot support.
- Add the DDL directly to `PostgresWarehouse` bootstrap/migration code. The
  PostgreSQL backend currently derives most tables from `LocalWarehouse`'s
  SQLite schema, so adding it there would inadvertently introduce the feature
  to SQLite as well.
- Seed only the USA, CAN, and AUS inflation/policy records as `candidate`
  rows. The complete global configuration is not automatically backfilled.
- No candidate is visible in the approved-country view until an explicit manual
  review action records approval. A config row is evidence, not approval.

## Approved row-identity decision

On 2026-10-26, the owner selected application-generated UUID text
`mapping_id` for each immutable geography record.

Each candidate/review version is append-only, and supersession links to the
prior `mapping_id`. The UUID is generated in application code rather than by a
database extension or an identity sequence, making command output and evidence
references stable and unambiguous across PostgreSQL environments.

`mapping_version` remains the version number per logical association. The
decision unblocks G2 (PostgreSQL persistence) and G3 (operator workflow).

## Delivery slices

### G1 — Pure candidate and review contract

Create immutable candidate/review domain models, ISO3 and status validation,
and deterministic extraction of the six selected config candidates. Parse
manifests to resolve each candidate's source and fail closed for a missing,
duplicate, or source-mismatched series.

- Acceptance: six candidates have the discovery report above; unselected
  global-config rows are not emitted; config evidence cannot produce
  `approved`.
- Verification: isolated unit tests cover source resolution, config hashing,
  candidate uniqueness, unknown ISO3, and collision/refusal behavior.
- Likely files: `src/fred_pipeline/catalogs/series_geography.py`, a focused
  config helper, and `tests/test_series_geography.py`.

Completed 2026-10-26. The implementation emits only the six selected
USA/CAN/AUS rows as deterministic `candidate` records, hashes their exact
config evidence, and refuses missing, changed, duplicate, or source-mismatched
records. It does not write to PostgreSQL.

### G2 — PostgreSQL persistence and approved-country contract

Add additive, restart-safe DDL in `PostgresWarehouse`: the child table,
integrity checks/indexes, and approved-only view. Add a focused repository that
writes candidates, records review/supersession transactionally, and reads the
view. It must validate the manifest-source evidence before any write.

- Acceptance: bootstrapping an existing PostgreSQL warehouse is additive;
  candidate/rejected/superseded rows never appear in the view; each
  `(source, series_id)` has at most one approved country; conflicting approval
  fails without changing the prior approved row.
- Verification: disposable-PostgreSQL integration tests prove idempotent seed,
  restart, rollback, source-aware refusal, approval, and supersession.
- Likely files: `src/fred_pipeline/io/postgres_store.py`,
  `src/fred_pipeline/io/series_geography_store.py`, the domain module, and
  focused unit/integration tests.

Completed 2026-10-26. PostgreSQL bootstrap now adds only
`meta.series_geography`, its approved-country partial-uniqueness index, and
`meta.v_approved_series_country`; SQLite and Delta are unchanged. The
repository validates both the manifest-backed source identity and the parent
`meta.fred_series` record before a write or review, seeds idempotently, and
reviews/supersedes transactionally.

### Checkpoint A

Run the focused unit tests, disposable PostgreSQL integration tests, repository
suite, and `ruff check src/fred_pipeline tests`. Review the actual table DDL,
approved-only view, and six candidate records before exposing any operator
command.

### G3 — Explicit operator workflow

Add narrow CLI commands to seed selected candidates, display a review report,
and approve/reject/supersede an exact evidence hash. The command must require a
PostgreSQL target and must not poll FRED or touch observations/Gold output.

- Acceptance: an operator can seed, inspect, and review the six candidates;
  stale or mismatched evidence hashes are refused; every mutating result is
  truthful and auditable.
- Verification: CLI tests against a disposable PostgreSQL database plus one
  documented smoke run.
- Likely files: `src/fred_pipeline/cli.py`, the repository/domain modules,
  `tests/test_cli_series_geography.py`, and the operator runbook/dictionary.

### G4 — Bounded FRED tag evidence (separate increment)

Add the selected-series `series/tags` client, retained sanitized response
provenance, versioned crosswalk, and drift report. It only creates candidates;
it never auto-approves a mapping. Non-FRED candidates remain config/manual
evidence only.

## Risks and controls

| Risk | Control |
| --- | --- |
| The parent metadata table lacks `source` | Validate source against parsed manifests at write time; preserve source in every child row and query predicate. |
| A schema addition leaks into SQLite through shared bootstrap DDL | Keep the new DDL and repository PostgreSQL-specific for this release. |
| Config is mistaken for an approval | Seed all records as candidates and require exact-hash human review. |
| Multiple country claims fan out consumers | Enforce one approved country per `(source, series_id)` and make the view approved-only. |
| A later tag refresh silently rewrites history | Store evidence hashes and create drift candidates; approval is append/supersede, never update-in-place. |

## Non-goals retained

This plan does not alter Silver observations, Gold outputs, `meta.fred_series`,
or the parent catalog key. It does not enable SQLite/Delta geography support,
automatic approval, an all-catalog FRED crawl, or a production migration.

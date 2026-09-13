# Spec 005: Full Validation Suite and Test Engineering System

Status: **proposed — ready for review**

Last verified: 2026-09-12

Primary owner: TBD

Target: the complete `fred_pipeline` Python package, its Bronze → Silver →
Gold data contracts, all supported storage backends, and the CI/release path.

Baseline repository commit: `32c7502f2f13509dfd67700611d57c712103f363`

External design reference: QuantSmith commit
[`2a12fbb`](https://github.com/thatquantguy44/QuantSmith/tree/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012),
inspected 2026-09-12.

---

## 1. Executive Summary

This repository already has a large and useful pytest suite. It does **not**
yet have a complete validation system: there is no acceptance-criterion
traceability gate, no global or changed-code coverage gate, no property-based
test layer, no mutation-quality signal, no CI job that proves the Postgres
backend against a real service, and no combined evidence report spanning the
unit, SQLite, Postgres, and Spark/Delta paths.

The purpose of this spec is to close that gap without turning coverage into a
vanity metric. The finished system must prove five different claims:

1. individual Python units implement their behavioral contracts;
2. source, storage, schema, and configuration boundaries conform to shared
   contracts;
3. equivalent inputs produce equivalent material outputs across the local,
   Postgres, and Spark/Delta implementations;
4. point-in-time and statistical outputs are deterministic, leakage-safe, and
   numerically defensible; and
5. CI can make an evidence-backed release decision with no unexpected skip,
   fabricated result, or hidden external dependency.

The operating model is adapted from QuantSmith's test-engineering agents:
test authoring, acceptance-criteria validation, and release approval remain
separate responsibilities. This repo is Python-only, so it adopts the useful
parts of that model—an orchestrator, a Python test engineer, a testing and
validation reviewer, and a quality guard—without importing the irrelevant
C++, JavaScript, or TypeScript agents.

The pytest suite and machine-readable CI artifacts remain the source of truth.
Agent prose never substitutes for an executed test result.

---

## 2. Problem and Measured Baseline

### 2.1 Repository size and current evidence

At the baseline commit, the repository contains:

| Measure | Observed value | How measured |
|---|---:|---|
| Production Python files | 127 | `find src/fred_pipeline -name '*.py'` |
| Production Python lines | 27,513 | raw `wc -l`, including comments/blanks |
| Test modules | 61 | `find tests -name 'test_*.py'` |
| Test lines | 15,953 | raw `wc -l`, including comments/blanks |
| Non-Spark collected cases | 1,084 | pytest collection with Spark file excluded |
| Spark/Delta collected cases | 30 | `tests/test_spark_integration.py` |
| Total discoverable cases | 1,114 | sum of the two intentionally separate collections |
| Non-Spark result | 1,081 passed, 3 skipped | declared dev dependencies + editable install |
| Branch-aware coverage | 77% | non-Spark suite, `coverage.py` branch mode |

The three skips are the Postgres tests: they use `pytest.importorskip` and
skip again when a local Postgres service is unavailable. That behavior is
appropriate for an ordinary developer laptop, but it is not release evidence.
CI currently has no Postgres service job, so this production backend can pass
the main workflow without being exercised.

The baseline was measured in a temporary virtual environment with:

```bash
pip install -r requirements-dev.txt
pip install -e .
pytest -q --ignore=tests/test_spark_integration.py \
  --cov=fred_pipeline --cov-branch \
  --cov-report=term-missing:skip-covered
```

This matters because `requirements-dev.txt` alone does not install NumPy;
`pip install -e .` supplies package dependencies declared in `pyproject.toml`.
The canonical setup and CI commands must preserve both steps.

### 2.2 What is already strong

The suite already provides valuable protection and should be evolved rather
than replaced:

- source clients predominantly use fake sessions or recorded payloads rather
  than live network calls;
- manifests, configuration loaders, quality rules, transforms, reconciliation,
  alerting, Gold view builders, and quantitative models have extensive
  example-based tests;
- local SQLite tests exercise persistence, idempotency, rollback, additive
  migration, query logging, and an end-to-end local run;
- Spark/Delta has a dedicated CI job and tests merge semantics, key Gold SQL,
  schema parity, and representative terminal-view population;
- Gold Polars parity tests already encode the right cross-implementation idea;
  and
- error paths are not ignored: the suite contains numerous explicit
  `pytest.raises` cases and failure-isolation checks.

### 2.3 Material gaps found in the baseline

Coverage is not the only measure, but the branch-aware report identifies real
contract gaps:

| Production surface | Baseline coverage | Why it matters | Priority |
|---|---:|---|---|
| `io/database_connection.py` | 0% | shared read/query contract for SQLite, Databricks, DuckDB, and Postgres | P0 |
| `writer/gold.py` | 0% in non-Spark report | 1,700+ lines of Spark Gold orchestration; separate Spark coverage is not merged | P0 |
| `io/warehouse_factory.py` | 15% | chooses dry-run/local/Databricks/Postgres behavior | P0 |
| `io/spark_io.py` | 15% | Delta merge and append boundary | P0 |
| `io/postgres_store.py` | 21% | production Postgres write path; three tests are optional skips | P0 |
| `cli.py` | 30% | public operational interface and exit-code contract | P1 |
| `io/warehouse.py` | 30% | shared protocol and Spark implementation | P0 |
| `data/silver.py` | 49% | source dispatch and normalized write boundary | P0 |
| `data/bronze.py` | 59% | raw payload/audit retention boundary | P1 |
| `catalogs/meta.py` | 62% | manifest-to-metadata synchronization | P1 |

The following suite-level gaps are also material:

- pytest markers do not classify unit, contract, integration, backend, parity,
  quant-validation, slow, or external smoke tests;
- no Hypothesis/property-based tests are present;
- no mutation-testing run checks whether assertions detect plausible defects;
- CI does not enforce coverage, changed-code coverage, or coverage regression;
- CI does not merge coverage from unit and Spark jobs;
- Postgres integration may be skipped everywhere;
- skips are not classified as expected versus unexpected;
- acceptance criteria do not map mechanically to test node IDs;
- no machine-readable test evidence envelope records commit, command,
  environment, seed, counts, skips, coverage, and AC mapping;
- a few tests read `date.today()`/current year directly, creating calendar-edge
  risk;
- test runs emit deprecation warnings from `datetime.utcnow()` and a Polars
  default that will change in 2.0; and
- there is no explicit release rubric separating blocking failures from
  advisory diagnostics.

### 2.4 The key distinction

`docs/validation/validation.md` describes **runtime data-quality rules** such
as non-empty batches, duplicate keys, parseable dates, numeric values, bounds,
freshness, and manifest policy. This spec is broader. It covers validation of:

- the code that implements those rules;
- data and schema contracts between pipeline layers;
- backend implementations;
- quantitative/statistical correctness;
- operational CLI and failure semantics; and
- the evidence used to approve a release.

Runtime DQ is one test domain inside the validation suite, not a synonym for
the suite.

---

## 3. Goals

### 3.1 Functional goals

| ID | Requirement | Priority |
|---|---|---|
| REQ-001 | Provide deterministic, offline unit tests for every critical pure-Python behavioral contract and failure path. | must |
| REQ-002 | Classify the suite into explicit pytest marker tiers with documented commands and ownership. | must |
| REQ-003 | Provide reusable contract tests for every `SourceClient`, `Warehouse`, and `DatabaseConnection` implementation. | must |
| REQ-004 | Prove Bronze → Silver → Gold behavior end to end on SQLite using a small, versioned, production-shaped fixture corpus. | must |
| REQ-005 | Run Postgres integration tests against a real ephemeral Postgres service in CI; the job must fail rather than skip when the service is expected. | must |
| REQ-006 | Run Spark/Delta integration tests against supported pinned versions and merge their coverage/evidence with the main suite. | must |
| REQ-007 | Prove material schema and value parity across SQLite, Postgres, Spark/Delta, and Polars implementations where they claim equivalent semantics. | must |
| REQ-008 | Add point-in-time and quant-validation tests for leakage, revision selection, deterministic fitting, numerical bounds, and statistical invariants. | must |
| REQ-009 | Add property-based tests where the implementation exposes a real invariant: idempotency, ordering, round trips, key uniqueness, bounded probabilities, and as-of safety. | must |
| REQ-010 | Add coverage ratchets and targeted mutation testing without making raw line coverage the definition of correctness. | must |
| REQ-011 | Map each acceptance criterion in this and future non-trivial specs to one or more executable pytest node IDs or explicit non-test verification steps. | must |
| REQ-012 | Produce machine-readable, auditable test evidence for each CI run and a human-readable release decision. | must |
| REQ-013 | Add repo-local testing-agent contracts adapted from QuantSmith for routing, Python test authorship, AC/quant validation, and quality gating. | should |
| REQ-014 | Prevent live network access and uncontrolled wall-clock/random/filesystem dependencies in PR tests. | must |
| REQ-015 | Make expected optional-dependency skips explicit and fail CI on unexpected skips in required jobs. | must |
| REQ-016 | Define a regression-test rule: every defect fix adds a test that fails on the pre-fix behavior. | must |

### 3.2 Non-functional requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-001 | Determinism | Two clean runs at the same commit, seed, and dependency lock produce the same pass/fail, counts, and material snapshots. |
| NFR-002 | PR feedback time | Required PR jobs complete in 12 minutes or less at p95; the offline unit tier completes in 2 minutes or less at p95. |
| NFR-003 | Python compatibility | Required unit/contract tiers pass on Python 3.10, 3.11, and 3.12. |
| NFR-004 | Coverage | Final branch-enabled global total is at least 85%; changed production code is at least 95%; no critical module is below its approved floor. |
| NFR-005 | Assertion quality | Targeted pure-core mutation score is at least 80%; every surviving mutant is fixed, justified, or recorded as follow-up. |
| NFR-006 | Isolation | PR tests require no public network, developer secrets, Databricks workspace, or persistent local service. |
| NFR-007 | Reproducibility | CI records Python/Java/Postgres/Spark/Delta versions, dependency inputs, test seed, and baseline commit. |
| NFR-008 | Honest reporting | No pass, coverage value, mutation score, or AC closure is stated unless produced by an attached command result. |
| NFR-009 | Maintainability | Fixtures express domain intent, duplicate setup is centralized, and test names identify behavior and expected outcome. |
| NFR-010 | Compatibility | Existing public runtime behavior and storage schemas remain unchanged unless a separately approved requirement authorizes a change. |
| NFR-011 | Security | Fixtures contain no credentials or restricted data; external smoke tests use least-privilege CI secrets and never run on untrusted forks. |
| NFR-012 | Test-to-production ratio | New test infrastructure does not require production-only dependencies in the core runtime install. |

### 3.3 Success measures

The project is successful when all acceptance criteria in §14 are met, not
when a particular number of tests has been written. Test count is diagnostic;
contract closure, mutation sensitivity, backend parity, and reproducible CI
evidence are outcomes.

---

## 4. Non-Goals

- Rewriting production modules merely to increase coverage.
- Requiring 100% line or branch coverage.
- Treating snapshots as a substitute for semantic assertions.
- Calling FRED, BLS, EIA, ECB, Treasury, World Bank, BIS, BEA, Census, SEC,
  Tiingo, Stooq, iShares, OECD, or any other third party during required PR
  tests.
- Load, soak, disaster-recovery, or full Databricks workspace certification;
  those need separate operational specs.
- Validating the economic meaning of all 2,000+ manifest series one by one.
- Adding non-Python language agents to this Python repository.
- Allowing an agent to approve its own tests or claim an unexecuted result.
- Replacing production monitoring, reconciliation, lineage, or DQ persistence.
- Moving all existing test files into a new directory tree in one disruptive
  commit. Classification and migration are incremental.
- Shipping DuckDB's missing write backend. Its existing read contract is in
  scope; the write implementation remains separate work.

---

## 5. QuantSmith-Informed Operating Model

### 5.1 Source material used

This design uses the separation of responsibilities documented in these
QuantSmith artifacts at the pinned reference commit:

- [`agents/test_engineering/README.md`](https://github.com/thatquantguy44/QuantSmith/blob/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/agents/test_engineering/README.md)
  — language-specific test authorship is distinct from AC validation and
  release approval;
- [`test_engineering_orchestrator`](https://github.com/thatquantguy44/QuantSmith/tree/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/agents/test_engineering/test_engineering_orchestrator)
  — detect the stack, route work, consolidate gaps, and hand off;
- [`python_test_engineer`](https://github.com/thatquantguy44/QuantSmith/tree/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/agents/test_engineering/python_test_engineer)
  — pytest fixtures, parametrization, boundary-aware fakes, property tests,
  determinism, and meaningful assertions;
- [`testing_validation`](https://github.com/thatquantguy44/QuantSmith/tree/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/agents/testing_validation)
  — map acceptance criteria to evidence and review quant integrity; and
- [`quality-guard-agent`](https://github.com/thatquantguy44/QuantSmith/tree/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/agents/quality-guard-agent)
  — make an explicit approve/reject decision based on contract and policy
  evidence.

QuantSmith's shared
[`test_engineering.md`](https://github.com/thatquantguy44/QuantSmith/blob/2a12fbb156c1d52aeeeddb2e5ad64e3a7b535012/instructions/test_engineering.md)
also establishes principles this spec adopts: deterministic tests, behavioral
assertions over coverage-chasing, boundary-appropriate fakes, mutation testing
as a stronger periodic signal, and honest reporting of untested paths.

The reference repository does not contain a license file at the pinned commit.
Implementation must therefore **not copy its prompt files verbatim**. It must
write original, repo-specific agent contracts that apply the public design
ideas above and retain this attribution/link.

### 5.2 Roles and non-overlap

```text
approved spec + code diff + current suite
                |
                v
 Test Engineering Orchestrator
   - inventories affected Python contracts
   - routes bounded test-authoring batches
   - consolidates overlaps and gaps
                |
                v
 Python Test Engineer
   - writes/reviews pytest, fixtures, parametrization, properties
   - runs the scoped tests and reports raw evidence
   - does not declare an AC closed or a release approved
                |
                v
 Testing & Validation Reviewer
   - maps AC-* to actual node IDs/results
   - checks PIT leakage, statistics, parity, and uncovered risks
   - declares AC status, but not release approval
                |
                v
 Quality Guard
   - checks all required jobs, policies, skips, evidence, and waivers
   - issues the final approve/reject decision
```

| Role | Owns | Must not own | Required output |
|---|---|---|---|
| Test Engineering Orchestrator | scope detection, routing, non-duplicative work plan, consolidated gaps | writing all tests itself; AC closure; release decision | affected surfaces, assignments, dependency order, handoff |
| Python Test Engineer | idiomatic tests, fixtures/fakes, properties, scoped execution | mocking the unit under test; unverifiable pass claims; release decision | code, command, result, assertions explained, remaining gaps |
| Testing & Validation Reviewer | AC mapping, edge/failure coverage, quant integrity, test-evidence review | silently accepting uncovered ACs; changing production behavior to force green | traceability report, AC pass/fail, validation-integrity review, risks |
| Quality Guard | blocking/advisory classification, policy checks, waiver expiry, approve/reject | test authorship; overriding failed evidence with prose | gate report with blockers, owners, remediation, decision |

### 5.3 Repo-local agent artifacts

Implementation should add original, tailored contracts under:

```text
agents/testing/
├── README.md
├── test_engineering_orchestrator/
│   ├── README.md
│   ├── prompt.md
│   ├── instructions.md
│   └── tasks.md
├── python_test_engineer/
│   ├── README.md
│   ├── prompt.md
│   ├── instructions.md
│   └── tasks.md
├── testing_validation/
│   ├── README.md
│   ├── prompt.md
│   ├── instructions.md
│   └── tasks.md
└── quality_guard/
    ├── README.md
    ├── prompt.md
    ├── instructions.md
    └── tasks.md
```

Each four-file contract must state inputs, outputs, checks, stop conditions,
scope boundaries, evidence rules, and the next handoff. The agents are a
repeatable collaboration aid; they are not a runtime dependency of the
pipeline or CI.

### 5.4 Required agent behavior for this repo

- inspect the approved spec, changed production files, existing tests, and CI
  configuration before proposing tests;
- reuse existing fixtures and test patterns where they express the same
  contract;
- prefer parametrization to near-duplicate tests;
- introduce Hypothesis only for explicit invariants, not for random-looking
  test volume;
- freeze/inject time and seed numeric work;
- fake third-party boundaries, not the function under test;
- keep production-code refactors separate and explain why testability requires
  them;
- name exact commands actually run and attach the result;
- identify expected and unexpected skips separately;
- never weaken an assertion, delete a regression test, or widen a tolerance
  merely to obtain green CI; and
- stop and hand off when the requested behavior is ambiguous or an AC cannot
  be tested without a product decision.

---

## 6. Test Architecture

### 6.1 Test tiers

| Marker | Boundary | External requirements | PR policy |
|---|---|---|---|
| `unit` | one function/class/module; pure or boundary faked | none | required on every PR |
| `contract` | one implementation against a reusable protocol/source contract | none unless paired with backend marker | required on every PR for offline contracts |
| `sqlite` | real temporary SQLite database | filesystem temp dir | required on every PR |
| `postgres` | real ephemeral Postgres | CI service container | required when Postgres job runs; zero skips allowed |
| `spark` | real local Spark + Delta | Java, PySpark, Delta | required in dedicated CI job; zero skips allowed |
| `parity` | same fixture/contract across two or more engines | depends on paired backend markers | required for affected backend changes |
| `quant` | PIT, leakage, numeric/statistical invariant | deterministic NumPy/Polars inputs | required on every PR when fast; slow cases nightly |
| `slow` | exceeds ordinary unit budget | declared local dependencies | nightly or explicit PR job |
| `external` | live third-party smoke test | secret/network | scheduled/manual only; never a required fork PR job |

Markers must be registered in `pyproject.toml`, and CI must use
`--strict-markers`. A test may carry more than one marker, such as
`contract + postgres` or `parity + spark`.

### 6.2 Logical layout

Existing files do not need a one-shot move. New tests should converge on this
layout, and legacy tests should be marked in place before optional later moves:

```text
tests/
├── unit/
│   ├── catalogs/
│   ├── data/
│   ├── governance/
│   ├── gold_config/
│   ├── ml/
│   ├── sources/
│   └── writer/
├── contracts/
│   ├── source_client_contract.py
│   ├── warehouse_contract.py
│   └── database_connection_contract.py
├── integration/
│   ├── sqlite/
│   ├── postgres/
│   └── spark/
├── parity/
├── validation/
│   ├── test_point_in_time_integrity.py
│   ├── test_quant_invariants.py
│   └── test_schema_contracts.py
├── regression/
├── fixtures/
│   ├── api/
│   ├── manifests/
│   ├── golden/
│   └── malformed/
├── conftest.py
└── traceability.yml
```

### 6.3 Reusable test-kit primitives

Add `tests/testkit/` helpers only where at least two domains reuse them:

- `FrozenClock` or injected `today` provider;
- deterministic RNG/seed fixture;
- network-denial fixture for offline tiers;
- production-shaped `SeriesSpec`, manifest, Bronze, and Silver row factories;
- response/session fakes with request history and exhaustion assertions;
- canonical fixture dataset builder;
- normalized row comparison with explicit type/date/float policies;
- schema comparison helpers;
- backend lifecycle/context manager;
- AC traceability parser/validator; and
- evidence-envelope writer.

Avoid a giant universal fixture. Small factories should make the economic and
temporal meaning of each test legible.

### 6.4 Fixture and golden-data policy

Required PR tests use only committed, synthetic or redistributable fixtures.
Every external-source fixture records:

- source and endpoint shape;
- retrieval or authored date;
- whether it is synthetic, transformed, or a short recorded response;
- license/provenance note where data is not synthetic;
- redaction status; and
- a stable checksum when exact bytes matter.

Golden outputs are reserved for stable schemas or compact, economically
meaningful result sets. Reviewers must be able to understand a golden diff.
Large opaque snapshots and auto-accepting snapshot updates are prohibited.

### 6.5 Assertion policy

Every test must assert at least one behavioral contract: returned values,
persisted state, emitted audit data, exception type/message, request shape,
schema, ordering, or invariant. “Did not raise” is insufficient when an
observable contract exists.

Floating-point assertions must choose and document one of:

- exact equality for deliberately exact arithmetic;
- absolute tolerance for near-zero/statistical boundaries;
- relative tolerance for scale-dependent values; or
- normalized decimal/string comparison for serialized values.

Tolerance widening requires a reason tied to the algorithm and expected
numeric stability.

---

## 7. Required Validation Domains

### 7.1 Source-client contract

Every registered source client must be parametrized through a shared contract
covering, where applicable:

- stable source name and series-ID handling;
- request URL, method, headers, auth placement, and timeout;
- pagination/chunking and deterministic ordering;
- rate limiting and retry/backoff with injected sleep;
- 4xx/5xx, malformed JSON/CSV/XML, empty response, and provider error payload;
- missing-value sentinel normalization;
- date/frequency normalization;
- vintage/revision fields;
- incremental-start semantics;
- no secrets in exception messages or logs; and
- no live socket access.

Provider-specific tests remain alongside the shared contract for shapes that
cannot honestly be generalized.

### 7.2 Bronze and Silver contracts

Tests must prove:

- raw payload fidelity and required ingestion metadata in Bronze;
- deterministic source dispatch in Silver normalization;
- natural-key uniqueness on
  `(source, series_id, observation_date, realtime_start)`;
- valid date canonicalization and numeric/missing parsing;
- no mutation of input payloads;
- duplicate behavior is explicit;
- replaying Bronze produces the same Silver rows at the same code/config
  version; and
- malformed source payloads fail with source/series context.

Property candidates include order independence of normalized sets,
idempotent normalization, and key uniqueness after valid deduplication.

### 7.3 Warehouse contract

One reusable suite must run against each supported write implementation:

- `sync_meta` inserts and updates without duplication;
- `restate_start` returns the correct boundary;
- Bronze append/read round trip;
- Silver merge is idempotent and revision-safe;
- `build_gold` is transactional at the backend's promised boundary;
- lifecycle, drift, staleness, release-calendar, run, and DQ persistence;
- `latest_observation_dates` honors source identity;
- context/close behavior is safe and repeatable; and
- errors roll back rather than leave partially rebuilt material outputs.

The protocol test should be implementation-parametrized. It must not mock the
backend implementation being certified.

### 7.4 DatabaseConnection contract

SQLite, DuckDB, Postgres, and Databricks adapters must share tests for:

- parameterized `query` and `execute`;
- stable list-of-dict row shape;
- streaming chunk boundaries, complete row delivery, and cursor cleanup;
- `table_names(schema=...)` semantics;
- commit/rollback behavior;
- context-manager and repeated-close behavior;
- provider placeholder adaptation without SQL corruption; and
- useful missing-driver/connection errors with no credential leakage.

Databricks may use an offline driver fake for unit contract coverage and a
separate scheduled workspace smoke test. The fake must live at the driver
boundary, not replace `DatabricksConnection` itself.

### 7.5 Backend and engine parity

A canonical miniature economy dataset must exercise:

- multiple sources with colliding raw series IDs;
- two vintages of a revised monthly series;
- a non-revised daily market series;
- missing values and a zero denominator;
- a release-calendar event;
- curve/spread inputs;
- recession flags;
- representative global/equity/company rows; and
- at least one row for each core Gold family.

For the same fixture and as-of date, compare:

1. relation name sets where the backends promise the same surface;
2. column names, logical types, nullability, and keys;
3. row counts and natural-key sets;
4. exact categorical/date/string outputs; and
5. numeric outputs under field-specific tolerances.

Intentional backend differences must be recorded in one versioned exception
registry with owner, reason, and expiry/review date. A comment hidden in one
test is not an approved parity exception.

### 7.6 Point-in-time and leakage validation

The suite must make the following invariants executable:

- an as-of query never uses an observation or revision whose availability
  date is after the requested as-of date;
- latest-observation logic chooses the correct newest revision deterministically;
- forward fill starts only after the first available value and never fills
  backward into unknown history;
- expanding/rolling transforms use only the permitted historical window;
- feature dates are aligned to the information-availability date, not merely
  the observation period;
- backfill resume does not change completed historical snapshots;
- replay at a pinned code/config version is deterministic; and
- incremental restatement yields the same material state as a full rebuild for
  the overlapping history.

At least one regression fixture must be constructed so a deliberately
look-ahead implementation would pass an ordinary value test but fail the PIT
invariant.

### 7.7 Quantitative/statistical validation

For PCA, Nelson-Siegel, recession, anomaly, inflation, factor attribution,
regime statistics, z-scores, and structural-break logic, test as applicable:

- deterministic output at a fixed seed/input/order;
- invariance to irrelevant row ordering;
- bounded probabilities and ordered confidence intervals;
- monotonic or conservation relationships implied by the formula;
- rank-deficient, constant, tiny, missing, and ill-conditioned samples;
- explicit minimum sample behavior;
- train/validation/test or as-of separation;
- no fitting of scalers/means/loadings on future evaluation rows;
- sign/label normalization where decompositions are non-unique;
- stable error behavior for non-convergence; and
- comparison with a small hand-calculated or independently implemented oracle.

Statistical tests must avoid fragile assertions on a single noisy estimate.
Prefer deterministic synthetic data with known parameters and tolerances tied
to sample size and algorithm behavior.

### 7.8 CLI and operational behavior

`cli.py` tests must cover the public parser and each command's high-level
contract without reaching live services:

- required/mutually exclusive arguments and defaults;
- configuration/environment/CLI precedence;
- source and series filtering;
- dry-run/local/backend selection;
- success, validation failure, partial series failure, quota limit, and
  unexpected exception exit codes;
- resource closure and persisted audit status;
- human-readable versus JSON output where supported; and
- no secret values in errors.

Use direct `main([...])` calls for fast behavior and a small subprocess suite
for packaging/entrypoint fidelity.

### 7.9 Runtime data-quality validation

Every rule documented in `docs/validation/validation.md` must have:

- pass, boundary, and fail examples;
- severity/profile behavior;
- missing and malformed input behavior;
- persisted audit representation; and
- a trace from documentation rule name to test node ID.

Manifest-wide tests must also validate schemas, duplicate IDs, config
references, licensing entries, and documented Gold/table contracts.

---

## 8. Property-Based and Mutation Testing

### 8.1 Hypothesis scope

Add Hypothesis to the test extra and use it first on high-value pure functions:

- manifest/config parsing and serialization boundaries;
- source key/date normalization;
- Bronze/Silver natural keys;
- quality-rule thresholds;
- rolling/expanding feature transforms;
- curve/spread operations and zero guards;
- probability/interval bounds;
- SQL-name/identifier conversion helpers; and
- idempotent merge/replay planning.

PR profile: deterministic database, deadline disabled only with justification,
approximately 100 examples per property. Nightly profile: larger example
budget (for example 1,000) with the failing seed/example retained as a
regression test.

### 8.2 Mutation scope

Run mutation testing periodically, not across the full 27,000-line package on
every PR. Initial targets:

- `validation/quality.py`;
- `data/transform.py` and `data/silver.py` pure paths;
- manifest/config validation;
- PIT selection and pure Gold feature builders;
- source base retry/rate-limit logic; and
- quantitative pure functions with deterministic fixtures.

The mutation report must list killed, survived, timeout, and incompetent
mutants. A score alone is insufficient. Surviving changes to comparisons,
date inequalities, merge keys, zero guards, severity, or revision selection
are release-significant gaps.

---

## 9. Coverage Policy

Coverage is a discovery and regression-control tool, not the proof of
correctness.

### 9.1 Ratchet

1. Commit the baseline branch-enabled coverage JSON/XML from a clean CI run.
2. Initially fail only on regression below the accepted baseline and on
   changed-code coverage below 90%.
3. Raise changed-code coverage to 95% after the first two implementation
   phases.
4. Raise the global branch-enabled total in small reviewed increments until it
   reaches at least 85%.
5. Require every P0 module in §2.3 to reach its module-specific approved floor,
   normally 85% or higher, with its critical branches explicitly tested.

Generated code, empty `__init__` files, and trivial compatibility re-exports
may be excluded only through a reviewed coverage configuration. A production
module cannot be excluded merely because it is difficult to test.

### 9.2 Combined coverage

Unit/SQLite, Postgres, and Spark jobs must emit parallel-mode coverage data or
XML/JSON artifacts that are combined before the global gate. This prevents
Spark-only code such as `writer/gold.py` from appearing permanently at 0% in
the main report while being exercised elsewhere.

---

## 10. Traceability and Evidence Contracts

### 10.1 Acceptance-criteria mapping

Add `tests/traceability.yml` with this logical shape:

```yaml
specs:
  SPEC005:
    acceptance_criteria:
      AC-001:
        tests:
          - tests/regression/test_test_bootstrap.py::test_clean_bootstrap_collects
      AC-002:
        tests:
          - tests/unit/test_test_policy.py::test_markers_are_registered
```

A validator must fail when:

- a referenced spec or AC does not exist;
- a must-have AC has no test or approved verification step;
- a referenced pytest node ID is not collected;
- a duplicate/unknown ID is used; or
- an expired waiver is the only evidence.

New feature specs should use their own stable prefix (for example
`SPEC006-AC-003`). Existing tests do not need invented historical ACs; they may
be associated with stable behavioral contract IDs as the suite is classified.

### 10.2 Test evidence envelope

Every required CI workflow uploads JSON with at least:

```json
{
  "schema_version": "1.0",
  "repository_commit": "<sha>",
  "job": "unit-py311",
  "command": "python -m pytest ...",
  "environment": {
    "python": "3.11.x",
    "platform": "linux",
    "java": null,
    "postgres": null,
    "spark": null,
    "delta": null
  },
  "seed": "<seed-or-not-applicable>",
  "started_at": "<UTC timestamp>",
  "duration_seconds": 0,
  "counts": {
    "collected": 0,
    "passed": 0,
    "failed": 0,
    "skipped_expected": 0,
    "skipped_unexpected": 0
  },
  "coverage_artifacts": [],
  "junit_artifact": "<path>",
  "traceability_artifact": "<path>",
  "result": "pass|fail"
}
```

CI timestamps naturally vary; reproducibility means the material result,
counts, seed, and snapshots match—not that envelope bytes are identical.

### 10.3 Release decision

The Quality Guard consumes the evidence envelopes and produces a short report:

- required jobs and exact results;
- AC/NFR status;
- expected and unexpected skips;
- coverage and mutation status;
- backend/parity status;
- blocking versus advisory findings;
- waiver owner and expiry; and
- explicit `APPROVE` or `REJECT`.

No confidence score may convert a hard failure into approval.

---

## 11. CI Design

### 11.1 Pull-request jobs

| Job | Environment | Command intent | Blocking |
|---|---|---|---|
| policy | Python 3.11 | lint test metadata, markers, traceability, fixture provenance, and forbidden live-network patterns | yes |
| unit matrix | Python 3.10/3.11/3.12 | `unit or contract or quant`, excluding backend/slow/external | yes |
| SQLite integration | Python 3.11 | real temporary DB, local E2E, replay/backfill/rollback | yes |
| Postgres integration | Python 3.11 + pinned service container | contract + integration + parity; fail on any skip | yes |
| Spark/Delta integration | Python 3.11 + Java 17 + pinned Spark/Delta | contract + integration + parity; fail on any skip | yes |
| evidence/coverage | Python 3.11 | combine coverage, validate AC mapping, publish JUnit/JSON/HTML summaries | yes |

Pin Postgres major version and the PySpark/Delta compatibility pair. A
scheduled dependency-update workflow may test newer versions before pins are
changed.

### 11.2 Scheduled/manual jobs

- larger Hypothesis profile;
- targeted mutation suite;
- randomized test order repeated with a recorded seed;
- optional live source smoke tests with minimal queries and strict quotas;
- optional Databricks connection/workspace smoke test; and
- dependency/version-forward compatibility.

Live jobs are operational signals, not replacements for deterministic fixture
tests. A third-party outage should not make an unrelated PR red.

### 11.3 Skip policy

- Unit jobs select markers explicitly rather than collecting integration tests
  and relying on import-time skip.
- Backend jobs treat unavailable required dependencies/services as failures.
- Expected optional skips require a registered reason code.
- CI reports the exact skip list; a new skip fails until reviewed.
- `xfail(strict=True)` is allowed only with issue/spec ID, owner, and expiry.
- Retries may diagnose flaky infrastructure but may not turn a flaky test green
  for release purposes.

### 11.4 Network and secret policy

Offline tiers fail on outbound socket use. Tests inject fake sessions/transports
and assert request construction. Scheduled external jobs:

- use environment-scoped, least-privilege secrets;
- do not run on forked pull requests;
- redact request headers and connection strings;
- cap calls/retries/time; and
- never write third-party live data into committed fixtures automatically.

---

## 12. Phased Implementation Plan

### Phase 0 — Reproducible baseline and test policy

Deliverables:

- canonical test bootstrap and commands;
- registered markers and strict-marker enforcement;
- baseline JUnit and branch-enabled coverage artifacts;
- offline network guard;
- expected-skip registry;
- traceability schema/validator skeleton; and
- warning inventory with owner and disposition.

Exit gate: the non-Spark baseline is reproducible from a clean environment;
suite selection is explicit; no baseline result is claimed without artifacts.

### Phase 1 — Critical unit and protocol coverage

Prioritize:

1. `io/database_connection.py`;
2. `io/warehouse_factory.py`;
3. `data/bronze.py` and `data/silver.py`;
4. `io/warehouse.py` helpers/dispatch;
5. CLI parser, backend selection, exit codes, and resource closure; and
6. `catalogs/meta.py`.

Add reusable `SourceClient`, `Warehouse`, and `DatabaseConnection` contract
suites. Use SQLite and driver-level fakes first; do not postpone pure contract
coverage until container jobs exist.

Exit gate: every P0 pure/boundary path has behavior and failure assertions;
changed-code coverage is at least 95%; no new unexpected skips.

### Phase 2 — Property and regression suite

Deliverables:

- Hypothesis profiles and first invariant set;
- regression directory and bug-fix policy;
- frozen clock/seed fixtures;
- PIT anti-leakage counterexamples;
- incremental-versus-full and replay idempotency properties; and
- warning fixes or time-bounded waivers.

Exit gate: properties run deterministically in PR CI and retain minimized
failures as examples/regressions.

### Phase 3 — Real Postgres and DatabaseConnection integration

Deliverables:

- pinned Postgres CI service;
- Postgres Warehouse contract suite with zero expected skips;
- Postgres and SQLite `DatabaseConnection` contract runs;
- transaction/rollback, placeholder, streaming, schema-listing, and close tests;
- SQLite ↔ Postgres schema/value parity on the canonical fixture; and
- separate driver-fake coverage for Databricks/DuckDB read adapters.

Exit gate: Postgres cannot be released on a skipped job; core local/Postgres
Gold outputs meet the parity contract.

### Phase 4 — Spark/Delta Gold coverage and parity

Deliverables:

- marker-based Spark suite with dependency failures treated as failures;
- coverage emitted from the Spark process and combined globally;
- Warehouse contract on Spark where practical;
- transaction/merge/schema tests;
- representative coverage of every Gold builder family; and
- SQLite/Postgres/Spark parity report with explicit approved exceptions.

Exit gate: `writer/gold.py` and `io/spark_io.py` meet approved floors; every
core Gold family is populated and semantically checked on the canonical data.

### Phase 5 — Quant/statistical validation

Deliverables:

- model-specific invariant matrices;
- hand-calculated or independent small oracles;
- rank-deficient/constant/missing/tiny-sample cases;
- seed/order invariance;
- fit-window and availability-date leakage tests;
- documented numerical tolerances; and
- targeted mutation run over pure quant and PIT logic.

Exit gate: no critical date inequality, revision selector, zero guard,
probability bound, or training-window rule has a surviving unreviewed mutant.

### Phase 6 — Agent workflow and release guard

Deliverables:

- original repo-specific four-file agent contracts from §5.3;
- one end-to-end worked test-authoring handoff;
- evidence-envelope generator;
- AC traceability gate;
- combined Quality Guard report; and
- developer/reviewer runbook.

Exit gate: a reviewer can reproduce the decision from machine evidence without
trusting agent narrative; the final gate emits an explicit approve/reject.

### Phase 7 — Ratchet and closeout

Deliverables:

- branch-enabled global total ≥85%;
- changed-code coverage ≥95%;
- targeted mutation score ≥80%;
- p95 timing within NFR-002 or a documented optimization plan;
- zero unexplained flakes across repeated clean runs;
- all SPEC005 ACs closed; and
- final handoff with remaining non-blocking backlog.

---

## 13. Task Backlog and Traceability

| Task | Work | Covers | Depends on |
|---|---|---|---|
| T-001 | Add canonical test extras/bootstrap and document clean setup. | REQ-001, REQ-002, NFR-003, NFR-006 | — |
| T-002 | Register markers, strict selection, skip/xfail policy, and network guard. | REQ-002, REQ-014, REQ-015 | T-001 |
| T-003 | Add coverage config, baseline artifacts, changed-code ratchet, and coverage combination. | REQ-010, REQ-012, NFR-004 | T-001 |
| T-004 | Implement traceability schema, validator, and initial SPEC005 mapping. | REQ-011, REQ-012 | T-001 |
| T-005 | Build `SourceClient` contract tests and migrate all registered sources. | REQ-003, REQ-014 | T-002 |
| T-006 | Build `DatabaseConnection` contract tests for SQLite/DuckDB plus driver-faked Databricks/Postgres unit paths. | REQ-003 | T-002 |
| T-007 | Build `Warehouse` contract tests and cover factory/dispatch behavior. | REQ-003 | T-002 |
| T-008 | Close Bronze/Silver/meta P0/P1 unit and failure-path gaps. | REQ-001, REQ-004 | T-002 |
| T-009 | Close CLI parser/exit/resource/secret-handling gaps. | REQ-001, REQ-014 | T-002 |
| T-010 | Create canonical miniature economy fixtures and provenance records. | REQ-004, REQ-007, NFR-011 | T-002 |
| T-011 | Add PIT, replay, incremental/full, DQ, and regression invariants. | REQ-008, REQ-016 | T-010 |
| T-012 | Add Hypothesis profiles and prioritized property tests. | REQ-009 | T-008, T-011 |
| T-013 | Add real Postgres CI service, contract, rollback, streaming, and parity jobs. | REQ-005, REQ-007 | T-006, T-007, T-010 |
| T-014 | Refactor Spark test selection, emit coverage, and complete Gold-family/parity coverage. | REQ-006, REQ-007 | T-003, T-007, T-010 |
| T-015 | Add quant/model validation matrix and deterministic oracles. | REQ-008 | T-010, T-012 |
| T-016 | Add targeted mutation workflow and triage policy. | REQ-010 | T-011, T-012, T-015 |
| T-017 | Write original QuantSmith-informed agent contracts and worked handoff. | REQ-013, NFR-008 | T-004 |
| T-018 | Generate evidence envelopes and the blocking Quality Guard report. | REQ-012, REQ-015, NFR-007, NFR-008 | T-003, T-004, T-013, T-014 |
| T-019 | Measure p95 runtime, remove flakes/warnings, raise ratchets, verify compatibility/maintainability, and publish closeout. | REQ-001–REQ-016; NFR-001–NFR-012 | T-001–T-018 |

### Definition of done for every implementation task

- behavior is tied to at least one requirement/AC;
- tests assert a real contract and fail against an intentionally broken
  implementation or equivalent demonstrated regression;
- required commands were actually run and evidence retained;
- no live network or secret dependency entered required PR tests;
- new skips/xfails have reason, owner, issue/spec ID, and expiry;
- relevant docs and traceability mapping are updated; and
- remaining gaps are stated plainly.

---

## 14. Acceptance Criteria

| ID | Given / When / Then | Covers | Evidence type |
|---|---|---|---|
| AC-001 | Given a clean supported Python environment, when the documented bootstrap and offline suite commands run, then collection succeeds and required tiers pass without public network or secrets. | REQ-001, REQ-014; NFR-003, NFR-006 | CI + network-denial regression |
| AC-002 | Given the pytest configuration, when an unknown marker is used or a test is selected by tier, then strict marker validation fails unknown values and each registered tier selects the documented set. | REQ-002 | policy tests + collection report |
| AC-003 | Given every registered source, when it runs through the shared source contract with fake transport responses, then request, retry, error, normalization, and secret-redaction rules pass. | REQ-003, REQ-014 | parametrized contract suite |
| AC-004 | Given each supported `DatabaseConnection`, when the reusable contract runs, then query, stream, execute, table discovery, transaction, context, and close semantics pass or carry an explicit scheduled-smoke classification. | REQ-003 | contract + integration results |
| AC-005 | Given Local, Postgres, and Spark Warehouse implementations, when the reusable Warehouse contract runs in their required jobs, then all promised protocol behaviors pass with zero unexpected skips. | REQ-003, REQ-015 | backend contract results |
| AC-006 | Given the canonical fixture corpus, when a local pipeline run, replay, incremental restatement, and full rebuild complete, then Bronze/Silver/Gold/audit outputs satisfy the documented key, idempotency, and rollback contracts. | REQ-004, REQ-016 | SQLite E2E + regression tests |
| AC-007 | Given the Postgres CI job, when the service or driver is unavailable, then the job fails; when available, all Postgres contract/integration tests pass without skip. | REQ-005, REQ-015 | GitHub Actions job artifact |
| AC-008 | Given the Spark/Delta CI job, when Java/PySpark/Delta is unavailable, then the job fails; when available, the 30 existing cases plus new contract/coverage cases pass without skip. | REQ-006, REQ-015 | GitHub Actions job artifact |
| AC-009 | Given identical canonical inputs, when material outputs are built by equivalent SQLite, Postgres, Spark/Delta, and Polars paths, then schema/key/value comparison passes subject only to versioned, unexpired exceptions. | REQ-007 | parity report |
| AC-010 | Given a fixture containing late revisions and future-available values, when any as-of feature or snapshot is computed, then no row uses information unavailable at that as-of timestamp. | REQ-008 | PIT anti-leakage regression |
| AC-011 | Given incremental and full processing of the same overlap, when final material states are normalized, then natural keys and values match. | REQ-008, REQ-009 | property + integration test |
| AC-012 | Given each quantitative model/transform and deterministic synthetic inputs, when order, seed, boundary, missing, and ill-conditioned cases run, then documented invariants and numeric tolerances hold with no future-data fit. | REQ-008 | quant validation matrix |
| AC-013 | Given the prioritized invariant list, when the PR Hypothesis profile runs twice with the same database/seed, then both runs produce the same result and any minimized failure is reproducible. | REQ-009; NFR-001 | property test artifacts |
| AC-014 | Given branch-enabled combined coverage from all required jobs, when the final gate runs, then global total is at least 85%, changed code at least 95%, and each critical module meets its approved floor. | REQ-010; NFR-004 | combined coverage JSON/XML |
| AC-015 | Given the targeted pure-core mutation set, when the scheduled mutation job runs, then score is at least 80% and no unreviewed survivor changes a critical temporal/key/severity/numeric rule. | REQ-010; NFR-005 | mutation report + triage |
| AC-016 | Given `tests/traceability.yml`, when its validator runs, then every SPEC005 AC resolves to a collected node ID or approved verification step and unknown/orphan/expired entries fail. | REQ-011 | traceability gate |
| AC-017 | Given required CI jobs, when they complete, then each emits a schema-valid evidence envelope containing environment, seed, counts, skips, artifacts, and result. | REQ-012; NFR-007, NFR-008 | JSON schema validation |
| AC-018 | Given the four repo-local testing roles, when their files are inspected, then each has the required four-file contract, explicit non-overlap, evidence rules, stop conditions, and named handoff, with QuantSmith attribution but no verbatim unlicensed copy. | REQ-013; NFR-008 | structural/document review |
| AC-019 | Given a source change without a changed/new regression test or approved test-not-needed record, when the policy gate runs, then it blocks the release. | REQ-016 | policy-gate regression |
| AC-020 | Given a required job with a new skip, non-strict xfail, or retry-only pass, when the Quality Guard evaluates it, then it rejects until the finding is resolved or explicitly waived with owner and expiry. | REQ-015 | synthetic evidence/gate tests |
| AC-021 | Given two clean runs at the same commit and recorded seed, when material evidence is compared, then test results, counts, and canonical snapshots match. | NFR-001, NFR-007 | repeat-run comparison |
| AC-022 | Given at least 20 representative PR workflow runs after rollout, when duration metrics are evaluated, then required jobs meet the p95 limits in NFR-002 or the spec remains open with a named remediation plan. | NFR-002 | CI timing report |
| AC-023 | Given an implementation PR, when the Quality Guard consumes all required evidence, then it emits an explicit approve/reject decision and cannot approve with a failed hard gate. | REQ-012; NFR-008 | quality-guard contract tests |
| AC-024 | Given committed test fixtures and artifacts, when secret/provenance/size policy checks run, then no credential or unapproved/restricted payload is present. | NFR-011 | secret scan + fixture manifest validation |
| AC-025 | Given the completed test suite, when maintainability review runs, then shared setup used by two or more domains is centralized, test names state behavior, and no giant fixture or opaque snapshot requires unrelated context to diagnose a failure. | NFR-009 | test-structure policy + reviewer report |
| AC-026 | Given the baseline public behavior and schemas, when the validation-suite implementation is compared with the baseline, then no runtime behavior or storage schema changed without a separately approved requirement and regression evidence. | NFR-010 | compatibility/parity report |
| AC-027 | Given a core runtime installation without test extras, when `fred_pipeline` is imported and its non-optional entrypoints are inspected, then no test-only package is required or imported. | NFR-012 | clean-install smoke test |

---

## 15. Release Gate

### 15.1 Blocking conditions

- collection error or failed required test;
- required backend job skipped or unavailable;
- unexpected skip or non-strict/expired xfail;
- missing must-have AC evidence;
- PIT/leakage, natural-key, transaction, or backend-parity failure;
- changed-code/global/module coverage below the active ratchet;
- critical surviving mutant without approved disposition;
- nondeterministic repeat result;
- live-network access from an offline tier;
- secret/prohibited fixture content;
- schema-invalid evidence envelope; or
- missing explicit Quality Guard decision.

### 15.2 Advisory conditions during rollout

- coverage above the active ratchet but below the final 85% target;
- mutation gaps outside the targeted critical set;
- deprecation warnings with owner and unexpired deadline;
- scheduled live-source smoke failure caused by confirmed provider outage; or
- documented parity differences not covered by a claimed shared contract.

An advisory item must still have an owner and next review date. It cannot stay
advisory indefinitely by omission.

### 15.3 Waivers

A waiver records ID, failed gate, scope, rationale, owner, approving reviewer,
creation date, expiry date, and remediation link. Waivers cannot exempt
credential exposure, fabricated evidence, or proven PIT leakage.

---

## 16. Risks and Mitigations

| ID | Risk | Impact | Mitigation |
|---|---|---|---|
| RISK-001 | Coverage becomes the objective and agents add vacuous assertions. | High numbers, weak regression protection. | Behavioral assertion rule, changed-code review, targeted mutation testing, independent Testing & Validation review. |
| RISK-002 | Tests over-mock storage or source logic. | Suite passes while real integration is broken. | Fake only external boundaries; reusable contracts run against real SQLite/Postgres/Spark implementations. |
| RISK-003 | Golden snapshots become opaque and are auto-updated. | Real semantic regressions are approved as noise. | Small reviewable goldens, semantic comparators, provenance, no automatic acceptance. |
| RISK-004 | Live APIs make CI flaky or consume quotas. | Unreliable releases and provider incidents. | Offline network denial; live tests scheduled/manual with strict quotas and secrets policy. |
| RISK-005 | Wall clock, ordering, or random numerical paths cause flakes. | Intermittent failures and lost trust. | Inject clocks, seed RNG, deterministic sort, repeat-run gate, record failing seed. |
| RISK-006 | Postgres/Spark jobs dominate CI duration. | Slow feedback and skipped local testing. | Parallel jobs, compact fixture, session-scoped backend, p95 SLO, scheduled heavy profiles. |
| RISK-007 | Backend parity assertions erase intentional engine differences. | Brittle tests or forced lowest-common-denominator behavior. | Contract-level parity plus versioned exception registry with owner/expiry. |
| RISK-008 | Floating-point tolerances hide defects or are too brittle. | False green or false red quant validation. | Field/algorithm-specific tolerance rationale and independent small oracles. |
| RISK-009 | Agent that wrote tests also declares success. | Confirmation bias and fabricated closure. | QuantSmith-informed role separation; machine evidence; independent reviewer and guard. |
| RISK-010 | Large test refactor causes merge conflict and loses coverage. | Delivery delay or silent regression. | Mark in place first; migrate files incrementally; compare node IDs/counts before/after. |
| RISK-011 | Optional-dependency skips conceal missing release evidence. | Untested production backend ships. | Explicit job selection and zero-skip rule for required backend jobs. |
| RISK-012 | Third-party fixtures violate terms or contain secrets. | Legal/security exposure. | Synthetic-first fixtures, provenance manifest, checksums, redaction and secret scan. |
| RISK-013 | Mutation testing consumes excessive compute. | CI cost/latency. | Target only critical pure modules, run scheduled, cache baselines, time-box expansion. |
| RISK-014 | QuantSmith material is copied despite absent license. | Attribution/licensing ambiguity. | Use design ideas only, write original repo-specific contracts, pin and link the reference. |

---

## 17. Rollout, Ownership, and Maintenance

### 17.1 Recommended owners

| Surface | Accountable owner |
|---|---|
| test policy, markers, evidence tooling | platform/test owner |
| source contracts and fixtures | data ingestion owner |
| Warehouse/DatabaseConnection contracts | storage owner |
| Spark/Postgres CI | platform owner |
| PIT and Gold parity | quant data owner |
| model/statistical invariants | model owner + independent quant reviewer |
| agent contracts and traceability | engineering lead |
| final release decision | reviewer other than primary test author |

Names may be assigned later, but no phase exits while a blocking surface has
only `TBD` ownership.

### 17.2 Rollback

Test infrastructure changes must be independently reversible:

- marker/layout changes do not modify production behavior;
- coverage thresholds are ratcheted in configuration commits;
- new CI jobs can be disabled by reverting their isolated workflow commit;
- fixture format changes include migration/compatibility notes; and
- production refactors made for testability remain separate from test-policy
  changes.

Rollback never means deleting a regression test to restore green. If the test
is correct and production regressed, the release remains blocked.

### 17.3 Living maintenance rules

- review dependency/backend pins at least quarterly;
- review waivers and expected skips on every release;
- convert every escaped production defect into a regression fixture;
- re-run targeted mutation analysis after material pure-core changes;
- keep `docs/validation/validation.md` rule names mapped to tests;
- re-baseline timing only when runner class or job topology changes;
- do not lower thresholds without a written risk decision; and
- update this spec when the promised production contract changes.

---

## 18. Recommended First Implementation Slice

Start with a small slice that improves both safety and the foundation for all
later work:

1. add and register markers without moving files;
2. create the offline network guard and explicit skip policy;
3. add branch-enabled coverage artifacts and preserve the measured 77%
   baseline as a non-regression ratchet;
4. build `DatabaseConnection` contract tests for SQLite first, covering the
   current 0% module;
5. add WarehouseFactory unit tests for local, dry-run, invalid, Postgres, and
   Databricks dispatch using boundary fakes;
6. introduce `tests/traceability.yml` and map AC-001, AC-002, AC-004, AC-014,
   and AC-016; and
7. run the Testing & Validation reviewer against that slice before the Quality
   Guard makes the first decision.

This slice is local, deterministic, and fast. It proves the new operating model
before the heavier Postgres/Spark and quantitative phases are added.

---

## 19. Open Decisions Requiring Owner Sign-Off

1. **Coverage floors:** approve 85% branch-enabled global, 95% changed code,
   and per-critical-module floors, or choose different values with rationale.
2. **CI SLO:** approve 12-minute p95 required PR workflow and 2-minute offline
   tier targets.
3. **Postgres version:** choose and pin the production-compatible major version
   for CI.
4. **Spark/Delta matrix:** decide whether one pinned pair is sufficient per PR
   or whether a second compatibility pair belongs in scheduled CI.
5. **Databricks smoke test:** decide whether a real workspace smoke is needed
   before release or remains scheduled/manual operational evidence.
6. **Mutation runner:** choose `mutmut` or an equivalent maintained Python tool
   after a short proof against `validation/quality.py`.
7. **Agent artifact location:** approve `agents/testing/` or select the repo's
   future standard before Phase 6. The role contracts themselves are required
   only if REQ-013 remains `should` and is accepted.
8. **Coverage exclusions:** approve the exact list of trivial re-export and
   package files, never a broad directory exemption.

Until these are decided, implementation may complete Phase 0 and the first
implementation slice using the proposed defaults, but final thresholds and
release closure remain subject to owner approval.

# Spec 012: Series Geography Metadata and Reviewed Country Map

Status: **Owner-reviewed — ready for implementation planning and task breakdown.**
Last verified: 2026-10-25 (repository code/config inspection).
Primary owner: Joshua Lutkemuller
Target: a source-backed, reviewable geography association for existing economic series, initially supporting United States, Australia, and Canada without changing observation values or inventing country labels.

## 1. Objective

Answer "which geography does this series measure?" using existing series identity and explicit evidence. FRED geography tags may suggest an answer, but are not themselves an approved country assignment. The same contract must work for non-FRED series such as World Bank and BIS. A consumer must be able to distinguish an approved country map from an ambiguous, regional, global, missing, or unreviewed candidate.

This is a metadata feature, not a second series catalog. The existing series catalog remains the parent series inventory. The proposed mapping attaches geography to a series; it does not add a country column to every observation, rewrite the observation store, or infer a country from a series ID string.

### Owner decision recorded

On 2026-10-25, the owner approved a child series-geography association rather
than a universal `country_iso3` column on the existing series catalog. The
association retains `country_iso3` when `geo_type=country` and leaves it null
for non-country or ambiguous geography scopes.

The owner also approved manual first-release review for all mappings, including
the six entries seeded from `config/global_series.yml`. They are candidates
until an operator approves the exact evidence record; configuration evidence
does not auto-approve a country map.

The owner further approved SQLite, PostgreSQL and Delta parity as a first-release
completion requirement. An unavailable or skipped backend remains unverified;
the approved-country contract is not complete until all three persistence paths
meet the same candidate, review and approved-map behavior.

### Initial user stories

1. An operator can inspect geography candidates and their sources for a cataloged series.
2. An operator can approve, reject, or supersede a candidate with an audit trail.
3. An approved country-specific series can be found by ISO 3166-1 alpha-3 code without joining to repeated observation rows.
4. A consumer asking for US/AU/CA coverage sees only approved mappings and an explicit "unmapped/ambiguous" state otherwise.

## 2. Current state and evidence

- The existing series catalog contains `series_id`, title and manifest `tags`, but no canonical geography field. `src/fred_pipeline/catalogs/meta.py` builds it from manifests; its `tags` value must not be reinterpreted as the response from FRED's tags API.
- The observation store is keyed logically by `(source, series_id, observation_date, realtime_start)`; one series may have many dates and vintages. Geography belongs at series scope, not observation scope.
- `config/global_series.yml` already maps country/ISO3 to a selected inflation and policy series. `src/fred_pipeline/writer/global_views.py` uses that config to build the two global Gold products. This is a curated product map, not a general FRED tag bridge.
- The current config names six US/AU/CA inflation and policy series. Before migration, implementation must verify that every configured series resolves exactly once in the existing catalog, its source agrees with registration, and its derived country/date product does not create a fanout. These are acceptance checks, not assumptions.
- FRED's [series/tags endpoint](https://fred.stlouisfed.org/docs/api/fred/series_tags.html) returns tags including `group_id=geo` (geography) and `group_id=geot` (geography type). Its [observations endpoint](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) returns dates, values and real-time fields, not country per observation. FRED's [tag guidance](https://fred.stlouisfed.org/help/data/finding-data/freds-tags) describes geography tags as discovery metadata.

Repository evidence: [global config](../../config/global_series.yml), [config loader](../../src/fred_pipeline/gold_config/global_config.py), [global Gold builder](../../src/fred_pipeline/writer/global_views.py), [meta sync](../../src/fred_pipeline/catalogs/meta.py), [FRED client](../../src/fred_pipeline/sources/fred.py), and [warehouse dictionary](../../docs/dictionary/data_dictionary.md).

## 3. Scope and non-goals

### In scope

- A versioned, auditable series-to-geography association using the existing series catalog.
- FRED `series/tags` retrieval for selected FRED series, with raw response provenance and bounded refresh behavior.
- Deterministic candidate extraction from recognized FRED `geo` and `geot` tags, and from reviewed repository config.
- Explicit human approval before a candidate is queryable as a country map.
- Backfill for the six existing US/AU/CA global inflation/policy series; subsequent expansion can use the same contract.
- SQLite, PostgreSQL and Spark/Delta persistence parity, matching the repository's supported backends.

### Out of scope

- Adding country to observation store, retroactively rewriting historical observations, or changing the existing Gold calculations.
- Treating every FRED `geo` tag as one country. State, metro, district, regional, supranational and global scopes must remain distinct.
- Automatically assigning geography to the whole catalog based on title, prefix, publisher location, source country or an LLM.
- A new SFMeridian query planner or automatic agent execution authorization. That consumer has its own semantic review gates.
- Re-keying the existing series catalog. A source/series collision found during validation must fail closed and be raised as a separate catalog-identity migration decision.

## 4. Proposed data contract

Add a child series-geography association, not a duplicate series table. Choose its physical name according to each backend's established convention during planning; do not put private deployment identifiers in this public spec. One row represents one candidate or reviewed association between an existing series and one geographic scope.

| Field | Contract |
| --- | --- |
| `source`, `series_id` | Source-aware series identity. `series_id` must exist in the existing catalog; `source` must agree with its manifest/source registration. |
| `geo_type` | Controlled enum: `country`, `subnational`, `region`, `supranational`, `global`, `unknown`. |
| `geo_code` | Canonical code within `geo_type`. Country uses ISO 3166-1 alpha-3 (`USA`, `AUS`, `CAN` initially). Non-country codes carry a documented namespace, never a fake ISO3. |
| `country_iso3` | Populated only for `geo_type=country`; otherwise null. |
| `status` | `candidate`, `approved`, `rejected`, or `superseded`. Only approved rows are queryable country mappings. |
| `evidence_kind` | `global_config`, `fred_geo_tag`, or `manual_review`. |
| `evidence_ref`, `evidence_hash` | Stable reference and digest of the config revision or retained FRED response; no API key or secret in either. |
| `observed_at_utc`, `reviewed_at_utc`, `reviewed_by` | Discovery and human-review timestamps/actor. A null review timestamp is required for unreviewed candidates. |
| `mapping_version`, `created_at_utc`, `superseded_at_utc` | Immutable version/audit fields; do not silently overwrite a prior approval. |

Uniqueness and checks:

- One current record per `(source, series_id, geo_type, geo_code, evidence_kind, evidence_hash)` for idempotent replays.
- At most one **approved country** per `(source, series_id)` in the first release. Multiple country candidates, tag/config conflicts, or a country alongside a conflicting global/region claim require manual resolution and do not produce an approved country row.
- A series may carry multiple non-country candidate scopes; do not collapse them into a country.
- Validate ISO3 against a pinned, versioned code list and document treatment of historical/aggregate codes. Do not use country-name string matching as the durable key.
- Approved rows cannot have empty provenance, missing reviewer, or a reference to a nonexistent cataloged series.
- Source is part of all lookup/join predicates even though the current catalog is keyed by `series_id` alone. Do not pretend `series_id` is globally source-unique in Silver.

Expose a read-only approved-country query contract (view or repository method) returning `source, series_id, country_iso3, evidence_kind, evidence_ref, mapping_version, reviewed_at_utc`. It must yield at most one row per `(source, series_id)` and must never expose candidates as approved rows.

## 5. Ingestion and review behavior

1. Seed the six existing US/AU/CA global-series entries from the pinned `config/global_series.yml` as candidates. Resolve each entry's actual source through the registered manifest/Silver source; do not infer FRED from the table name. Compare against the existing catalog and fail if a series is absent or source identity conflicts.
2. Add a bounded FRED client method for `GET /fred/series/tags?series_id=...&file_type=json`. Use existing API key, timeout, retry/rate-limit and audit conventions. Refresh only explicitly selected FRED series or due series; no unconditional full-catalog tag crawl during normal ingestion. World Bank and BIS entries do not call this endpoint.
3. Retain a sanitized response snapshot or content-addressed raw payload with `series_id`, retrieval time, endpoint, response hash and run ID. Do not log or persist the API key. Preserve the exact FRED tag name and group in evidence so a later mapping decision is reproducible.
4. Apply a deterministic, versioned crosswalk from known FRED `geo` tag names to canonical geography codes and use `geot` only as supporting geography-type evidence. Unknown tags remain candidates with `geo_type=unknown`; titles and free-text notes are never silently promoted.
5. Produce a review report showing each candidate, its evidence, collisions, config/tag disagreement, current approval and proposed change. Approval is an explicit operator action over an exact candidate/evidence hash, not a flag set by ingestion.
6. On approval, supersede prior versions transactionally. Refreshing FRED tags must not revoke or replace an approval automatically; a changed tag creates a drift finding requiring review. Rejection is recorded, not deleted.
7. The map's `observed_at_utc` is metadata retrieval time, not proof that FRED carried the same tag at an earlier date. Do not make point-in-time tag claims without captured historical evidence. Observation as-of/vintage selection remains the existing Silver/Gold contract.

## 6. Implementation boundaries and migration

- Use the existing Python package layout: FRED HTTP method under `src/fred_pipeline/sources/`, candidate/crosswalk and review logic under `src/fred_pipeline/catalogs/`, backend persistence under `src/fred_pipeline/io/`, DDL under `sql/`, tests under `tests/`, and dictionary/runbook updates under `docs/`.
- Add the new table and approved-country read contract in an additive migration. Existing existing series catalog, observation store, global Gold output and manifest format remain unchanged. Existing deployments must bootstrap an empty table safely and be restart/idempotency safe.
- Start with deterministic config candidates and review logic. Add FRED tag ingestion as a separate increment. Enable the approved-country consumer contract only after backend parity and collision tests pass.
- If the actual PostgreSQL, SQLite and Delta catalog schemas disagree with this draft, update the spec and migration contract before coding. Do not edit vendored `agent-skills`.

## 7. Commands, style and verification

Development setup: `python -m pip install -e '.[dev,postgres]'`.
Focused tests to add and run: `python -m pytest -q tests/test_series_geography.py` and backend integration tests for SQLite/PostgreSQL/Spark where available.
Repository suite: `python -m pytest -q`.
Lint: `ruff check src/fred_pipeline tests` (using the repository's configured rule set).
Runtime smoke: run an explicit geography candidate refresh against a small selected FRED fixture/live series set, then check the approved-country contract against a disposable database. The implementation PR must document the exact new CLI invocation; this draft does not claim a command already exists.

Follow the repository's pure-core plus backend-adapter style. For example, a candidate resolver returns a proposed value and reason, not an implicit approval:

```python
candidate = resolve_geo_tag(tag_name="usa", group_id="geo")
assert candidate.geo_type == "country"
assert candidate.country_iso3 == "USA"
assert candidate.status == "candidate"
```

Unit tests cover tag normalization, unknown/ambiguous tags, config precedence, review-state transitions, collisions and evidence hashes without network access. Integration tests cover idempotent persistence, migration from an existing database, source-aware joins, and one-row approved-country behavior on all supported backends. Live FRED tests are optional proof, not a substitute for deterministic fixtures.

Boundaries: always preserve raw evidence and run tests before promotion; ask the owner before changing the existing catalog key or automatically approving tags; never commit credentials, alter Silver observations, silently choose among multiple geographies, or edit vendored files.

## 8. Acceptance criteria

| ID | Criterion |
| --- | --- |
| AC-001 | The six configured US/AU/CA inflation and policy series remain in the existing series catalog; no duplicate catalog is created. Their candidate mappings identify correct source and ISO3. |
| AC-002 | FRED `geo`/`geot` responses are fetched only for selected/due FRED series, retained with verifiable provenance and no key exposure; non-FRED series do not call FRED. |
| AC-003 | No candidate becomes an approved country map without an exact-evidence human review action. Unknown, multi-country and conflicting tags fail closed. |
| AC-004 | The approved-country contract yields at most one row per `(source, series_id)`, with ISO3 and provenance; it excludes unreviewed/rejected/superseded candidates. |
| AC-005 | Existing Silver observation row counts/values/vintages and global Gold output are unchanged by the migration and refresh. No join fans out observation rows. |
| AC-006 | SQLite, PostgreSQL and Delta fixtures produce equivalent candidate/review/approved results; a skipped live backend is reported as unverified, not passed. |
| AC-007 | Re-run is idempotent; changed source tags create drift evidence without silently changing approvals; rollback leaves the prior approved map readable. |
| AC-008 | Data dictionary and operator runbook describe refresh, review, provenance, drift, limitations and the exact query contract. |
| AC-009 | Focused tests, repository suite and lint pass; a disposable-database smoke demonstrates US/AU/CA lookup and one ambiguous/refusal case. |

## 9. Approved decisions and next step

The owner has approved the child-association data model, manual first-release
review, and SQLite/PostgreSQL/Delta parity requirement. The next step is a
separate implementation plan and task breakdown before coding. No implementation,
production migration, or consumer query authorization is implied by this review.

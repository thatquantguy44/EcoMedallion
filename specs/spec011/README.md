# Spec 011: Point-in-Time ECB Eligible Marketable Securities

Status: **Phase 1 implemented for forward ingestion; Phase 2 archive bootstrap
and Phase 3 Spark runtime/production scheduling remain open.**

Last verified: 2026-10-23 (2026-10-22 MID release message, signed full/update
XML and CSV artifacts, dictionary, and live SQLite/PostgreSQL ingestions)

Primary owner: TBD

Target: a point-in-time-safe history of the ECB's List of eligible marketable
assets in SQLite, PostgreSQL, and Delta, with a current view and an auditable
answer to "which securities did the ECB list as eligible at this cutoff?"

Depends on: `specs/spec004` (warehouse parity), `specs/spec006` (licensing
gate), and the existing Bronze/Silver/Gold and run-audit conventions.

---

## 1. Goal

Ingest every published ECB eligible-assets release, preserve its source files,
and build a compact security history that can reconstruct the list exactly as
published at any accepted release cutoff.

The product must support three questions without looking at data published
after the requested cutoff:

1. Was security `X` listed as eligible at timestamp `T`, and under which
   attributes and haircuts?
2. What was the complete eligible universe at `T`?
3. What was added, removed, or changed between two accepted ECB releases?

This is a different shape from the pipeline's observation-series model. A
security is present in a daily list, its attributes may change, and it may later
disappear or reappear. The implementation therefore gets dedicated tables and
an SCD2-style interval model; it must not encode eligibility as a numeric FRED
observation or overwrite yesterday's row with today's state.

## 2. Authoritative sources and verified facts

### 2.1 Source endpoints

| Purpose | Endpoint |
| --- | --- |
| Human download/archive page | `https://www.ecb.europa.eu/mopo/coll/assets/html/list-MID.en.html` |
| MID RSS discovery feed | `https://mid.ecb.europa.eu/rss/mid.xml` |
| Latest full XML fallback | `https://mid.ecb.europa.eu/app/ea/ea.xml.gz` |
| Latest update XML fallback | `https://mid.ecb.europa.eu/app/ea/ea_update.xml.gz` |
| Latest dictionary fallback | `https://mid.ecb.europa.eu/app/ea/ea_dictionary.xml.gz` |
| Annual archives | `https://www.ecb.europa.eu/paym/coll/assets/shared/data/archive/YYYY.zip` |
| User guide | `https://www.ecb.europa.eu/mopo/coll/assets/guide/html/index.en.html` |
| MID integration guide | `https://www.ecb.europa.eu/press/shared/pdf/MID-Catalogue-and-Integration-Guide.en.pdf` |

Use HTTPS even though the ECB user guide still prints the RSS URL with
`http://`.

The operational trigger is the RSS item whose title is exactly **List of
eligible marketable assets**. The item link points to a signed MID release
message. Do not ingest an item based on title alone: the linked message must
also contain:

```text
releaseType  = EligibleAssetsList
releaseTitle = List of eligible marketable assets
```

The release message, not a guessed filename, is the source of the attachment
URLs.

### 2.2 Live evidence captured while writing this spec

The 2026-10-22 RSS entry was published at `17:56:07 +0200`. Its linked release
message exposed five attachments:

- `ea_csv_261022.csv.gz`
- `ea_xml_261022.xml.gz`
- `ea_csv_update_261022.csv.gz`
- `ea_xml_update_261022.xml.gz`
- `ea_dictionary_261022.xml`

The full XML contained **31,331** assets. The update XML contained **354**
records: **88 new (`N`)**, **102 deleted (`D`)**, and **164 updated (`U`)**.
These counts are evidence for the checked-in fixture and sizing estimates, not
permanent production thresholds.

The current full XML fields are:

```text
isinCode, otherRegNumber, haircutCategory, type, referenceMarket,
denomination, issuanceDate, maturityDate, issuerCsd, couponDefinition,
couponRate, issuerName, issuerGroup, issuerResidence, guarantorName,
guarantorGroup, guarantorResidence, nonOwnUseHaircut, ownUseHaircut,
potentiallyOwnUsableCoveredBond, climateFactor
```

The update file has the same fields plus `change`, whose documented values are
`N`, `D`, and `U`.

### 2.3 Cadence, history, and source caveats

- The ECB says the list is refreshed Monday through Friday, excluding TARGET
  closing days. Its public page says 18:15 CET; the live release above appeared
  earlier. Poll the feed rather than scheduling one exact download time.
- The public archive goes back to April 2010 and explicitly warns that some
  snapshots are missing and historical formats differ as reporting rules
  changed.
- The stable `/app/ea/` URLs expose only the latest files. They are a recovery
  path, not a historical store.
- The user guide says CSV is UTF-16LE and XML is UTF-16. The live 2026-10-22
  XML declared UTF-8. Parsers must honor a BOM/XML declaration and detect the
  CSV encoding; they must not hardcode the prose description.
- `CLIMATE_FACTOR` was added in June 2026. Older absence means "not present in
  that source schema", not `N`.
- MID release messages carry a signature for each attachment. A transport-level
  TLS success and a locally computed hash are not substitutes for signature
  verification.
- The public list is not the complete universe of all collateral that may be
  eligible in every circumstance. The ECB notes that some eligible assets are
  not included. Table and UI labels must say "listed as eligible by the ECB",
  not make a broader legal assertion.

## 3. Point-in-time contract

### 3.1 What `as_of` means

For this dataset, point in time means publication time, not issuance date,
maturity date, file retrieval time, or the date embedded in a filename.

Given an `as_of_timestamp`, select the latest **accepted** release whose
`publication_at_utc <= as_of_timestamp`, then return the eligible intervals
open at that release. A release is accepted only after its artifacts,
signature, schema, identifiers, full/update reconciliation, and database
transaction pass.

The release XML timestamps have no UTC offset while RSS does. Resolve them in
the `Europe/Frankfurt` time zone and retain all three values:

- source-local timestamp and zone;
- normalized UTC timestamp;
- retrieval timestamp in UTC.

Never attach UTC directly to the source's naive timestamp. Daylight-saving
transitions make that wrong by one or two hours.

Intervals are half-open:

```text
valid_from <= as_of_timestamp < valid_to
```

`valid_to IS NULL` means no later accepted release has superseded that version.
A same-day correction is ordered by publication timestamp, so a `DATE` alone
is not a sufficient version key.

For a date-only consumer, the documented helper contract is "latest accepted
release published by 23:59:59.999999 Europe/Frankfurt on that date." The helper
must still return the actual selected release timestamp and coverage status.

### 3.2 Security identity

Create one deterministic `asset_key`:

```text
ISIN:<trimmed uppercase isinCode>
OTHER:<trimmed otherRegNumber>
```

Prefer ISIN when present. Use `OTHER:` only when ISIN is empty. Reject and
quarantine a row when both are empty. Reject duplicate `asset_key` values
within one full snapshot. Preserve both original identifier fields even when
one is not part of the key.

An identifier change is a removal of the old key and an addition of the new
key; do not infer that two identifiers refer to the same security from issuer,
maturity, or coupon similarity.

### 3.3 Eligibility and attribute versions

Each full snapshot is authoritative for membership at that release. Comparing
two consecutive accepted full snapshots produces:

- `N`: absent before, present now;
- `D`: present before, absent now;
- `U`: present in both, but the canonical attribute hash changed;
- unchanged: present in both with the same canonical hash.

`N` and `U` open an eligible version. `D` closes the eligible version and
opens an ineligible tombstone version so the removal has explicit provenance.
A later reappearance opens a new eligible version; it does not rewrite the old
interval.

The ECB update attachment is an independent reconciliation input. On a normal
consecutive daily load, its `N`/`D`/`U` keys must match the diff computed from
the two full snapshots. A mismatch stores all raw evidence but does not publish
the release to Silver/Gold until it is resolved. For the first snapshot after
an archive gap, compute the net diff and mark it `derived_full_diff`; do not
pretend the changes occurred on one of the missing dates.

### 3.4 Knowledge time versus effective security dates

`issuance_date` and `maturity_date` are security attributes. They never control
when a row becomes visible to a point-in-time query. A security with an old
issuance date first seen in today's release is not visible yesterday.

The same rule applies to corrected attributes: a corrected haircut or issuer
value becomes known at the correcting release's publication time. Historical
queries retain the previously published value.

## 4. Data model

Physical names follow each backend's convention: schema-qualified names on
PostgreSQL/Delta and underscore-prefixed equivalents on SQLite.

### 4.1 `bronze.ecb_eligible_asset_release`

One immutable row per discovered MID release message.

| Column | Contract |
| --- | --- |
| `release_id` | Deterministic SHA-256 of canonical release URL plus publication timestamp |
| `release_url` | RSS item URL; unique |
| `release_type`, `release_title` | Values parsed from the linked message |
| `release_at_local`, `publication_at_local`, `publication_at_utc` | Source chronology |
| `rss_pub_at`, `rss_item_hash` | Feed provenance and deduplication |
| `discovered_at`, `retrieved_at` | UTC pipeline timestamps |
| `status` | `discovered`, `downloaded`, `validated`, `accepted`, `quarantined`, `failed` |
| `run_id`, `error_detail` | Audit linkage and terminal failure reason |

The unique keys are `release_url` and `release_id`. Re-reading RSS is a no-op.

### 4.2 `bronze.ecb_eligible_asset_artifact`

One row per release attachment.

| Column | Contract |
| --- | --- |
| `release_id`, `artifact_kind` | `full_xml`, `full_csv`, `update_xml`, `update_csv`, or `dictionary_xml` |
| `file_name`, `source_url`, `content_type`, `encoding` | Source metadata |
| `compressed_bytes`, `uncompressed_bytes`, `sha256` | Integrity and sizing |
| `signature_raw`, `signature_status`, `signer_subject`, `verified_at` | MID signature evidence |
| `archive_path` | Immutable file in the configured Bronze raw archive |
| `retrieved_at`, `run_id` | Pipeline lineage |

Store the exact compressed bytes before parsing. Never overwrite an existing
artifact at the same path. If the source URL later returns different bytes,
store a new content-addressed object and quarantine the release.

### 4.3 `silver.ecb_eligible_asset_release`

The accepted release/coverage ledger. It records row counts, `N`/`U`/`D`
counts, parser/schema version, source column-set hash, prior accepted release,
whether that prior release is consecutive, reconciliation status, dictionary
hash, and the transaction that published the release.

This table is the only source from which Gold may choose a cutoff. Bronze rows
that are downloaded but not accepted are intentionally invisible to consumers.

### 4.4 `silver.ecb_eligible_asset_version`

Grain: one row per `(asset_key, valid_from_release_id)`.

Required columns:

```text
asset_key, isin_code, other_reg_number, is_eligible,
haircut_category, asset_type, reference_market, denomination,
issuance_date, maturity_date, issuer_csd, coupon_definition, coupon_rate,
issuer_name, issuer_group, issuer_residence,
guarantor_name, guarantor_group, guarantor_residence,
non_own_use_haircut, own_use_haircut,
potentially_own_usable_covered_bond, climate_factor,
source_fields_json, source_schema_version, row_hash,
change_type, change_origin,
valid_from_release_id, valid_from_utc,
valid_to_release_id, valid_to_utc,
ingested_at, run_id
```

`source_fields_json` preserves source fields that are not yet promoted to typed
columns. It does not authorize silently publishing an unknown operational
schema: a newly observed field pauses acceptance until its meaning and hashing
rule are reviewed. For historical registered schemas, fields that did not yet
exist are `NULL` and the schema version explains why.

Constraints:

- `valid_to_utc > valid_from_utc` when `valid_to_utc` is not null;
- no overlapping intervals for one `asset_key`;
- at most one open interval per `asset_key`;
- `row_hash` covers every normalized source attribute plus `is_eligible`;
- source codes remain codes; dictionary labels are joined, not substituted.

### 4.5 `silver.ecb_eligible_asset_change`

Grain: one row per `(release_id, asset_key)` changed by that release. Retain
the ECB-declared change code, computed change code, prior/new row hashes, and
reconciliation outcome. This is the audit-friendly delta table; it is not the
authority for membership when the full snapshot disagrees.

### 4.6 Gold consumer objects

| Object | Purpose |
| --- | --- |
| `gold.ecb_eligible_security_history` | Typed eligible/ineligible intervals with release provenance and dictionary labels |
| `gold.ecb_eligible_security_current` | Eligible rows from the latest accepted release only |
| `gold.ecb_eligible_security_change` | Reviewable additions, removals, and attribute changes |
| `gold.ecb_eligible_security_coverage` | Accepted/missing/quarantined releases, gaps, schema versions, and freshness |

The history and release-ledger objects must support this portable query shape:

```sql
WITH selected_release AS (
    SELECT release_id, publication_at_utc, coverage_status
    FROM silver.ecb_eligible_asset_release
    WHERE status = 'accepted'
      AND publication_at_utc <= :as_of_timestamp
    ORDER BY publication_at_utc DESC
    LIMIT 1
)
SELECT
    h.*,
    r.release_id AS selected_release_id,
    r.publication_at_utc AS selected_release_publication_at_utc,
    r.coverage_status
FROM gold.ecb_eligible_security_history h
CROSS JOIN selected_release r
WHERE h.valid_from_utc <= r.publication_at_utc
  AND (h.valid_to_utc IS NULL OR r.publication_at_utc < h.valid_to_utc)
  AND h.is_eligible = TRUE;
```

The query result includes `selected_release_id`,
`selected_release_publication_at_utc`, and coverage/freshness status so a
consumer can distinguish an exact recent state from a carried-forward state
across a known archive gap.

## 5. Ingestion design

```text
MID RSS
  -> exact title filter
  -> fetch and validate linked release message
  -> download all five linked artifacts
  -> retain compressed bytes + hashes + signature evidence in Bronze
  -> parse full/update/dictionary with schema registry
  -> validate identifiers, types, duplicates, counts, and dates
  -> diff full snapshot against prior accepted full snapshot
  -> reconcile computed changes with update attachment
  -> transactionally close/open Silver intervals and write changes
  -> mark release accepted
  -> refresh Gold views/tables and audit/freshness state
```

### 5.1 Polling and recovery

- Poll RSS every 15 minutes. Filter locally; the feed contains many unrelated
  MID releases.
- Deduplicate before downloading through the release URL/ID ledger.
- Use conditional HTTP headers when the server supports them, but correctness
  must not depend on `ETag` or `Last-Modified` being present.
- Retry transient failures with the shared bounded exponential backoff and
  jitter. A partial release stays non-accepted and resumes by missing artifact
  hash, not by duplicating completed work.
- Run a daily reconciliation after the expected publication window. If RSS was
  missed, inspect the stable latest XML endpoints and the download page. The
  downloaded payload date/hash must be matched to a release before publication.
- Alert after an expected update day has no accepted release. TARGET closing
  days must come from an explicit calendar/config; absent that calendar, use a
  two-business-day warning rather than generating holiday false positives.

### 5.2 Parsing and normalization

- Prefer XML for forward ingestion because it is structured and linked from
  the signed release. Retain CSV too and sample-check XML/CSV key/count parity.
- Use streaming parsing; the current uncompressed full XML is about 27 MB and
  should not require building a whole DOM.
- Trim surrounding whitespace, preserve Unicode names, normalize identifiers
  and codes to uppercase, parse `DD/MM/YYYY HH:MM:SS` dates explicitly, and use
  decimal parsing before writing database numerics.
- Empty elements become `NULL`, never zero or an empty identifier.
- Do not translate dictionary codes into the version row. Load dictionary
  entries separately by `(dictionary_hash, domain, code)` and expose labels in
  Gold so historical codes retain the dictionary version published with their
  release.
- A new field, missing required field, duplicate key, invalid identifier, or
  unparseable non-empty numeric/date value fails acceptance for that release.

### 5.3 Transaction and idempotency

The following occur in one warehouse transaction (or one staged atomic Delta
publish): write accepted-release facts, write computed changes, close affected
open versions, insert new versions/tombstones, and advance the latest accepted
release pointer. A crash before commit leaves the previous release current. A
retry produces the same rows and hashes.

Do not mark a release accepted before its version rows commit. Do not advance
freshness merely because RSS was reachable or a file downloaded.

Out-of-order historical backfills are rebuilt in chronological order into
staging tables and atomically swapped/merged after parity checks. They must not
patch interval endpoints one archive file at a time in production.

## 6. Historical bootstrap

1. Enumerate archive ZIPs from 2010 through the current year from the official
   download page; do not assume every weekday/file exists.
2. Inventory filenames, compressed hashes, column sets, encodings, row counts,
   and missing dates before loading data.
3. Define a reviewed schema registry for each observed historical column set.
   Preserve unmapped source fields and document renamed/added/removed fields.
4. Load full snapshots in publication-date order. When an exact publication
   timestamp is unavailable, use a distinct `timestamp_precision = date` flag
   and a deterministic end-of-day Europe/Frankfurt timestamp; never imply the
   time was published by the ECB.
5. Diff consecutive available snapshots. Mark a gap whenever the calendar says
   an expected release is absent; changes across a gap have unknown exact event
   time.
6. Reconcile the final historical state to a known live full snapshot before
   enabling scheduled forward ingestion.

The compact version table is the production history. A row-per-security-per-day
expanded snapshot table is explicitly rejected as the primary model: at the
current ~31k rows and roughly 4k working-day snapshots it would duplicate on
the order of 100 million mostly unchanged rows. Exact full source snapshots
remain recoverable from Bronze.

## 7. Quality, observability, and security

Per release, persist and alert on:

- artifact download/signature/hash status;
- full, update, and dictionary record counts;
- duplicate/missing identifier count;
- `N`/`U`/`D` counts from both source and computed diff;
- XML/CSV sampled parity;
- new/missing columns and schema hash;
- current eligible count and change percentage versus prior release;
- release publication lag and pipeline acceptance lag;
- prior-release gap and coverage status;
- interval overlap/open-version invariant failures.

Hard failures quarantine a release without advancing Gold. A large but valid
day-over-day change is not automatically discarded; it requires an alert and a
reviewable override tied to the run/release. Thresholds are calibrated from the
archive rather than invented from the one live day in §2.2.

Signature validation must follow the ECB MID integration guide and use a
pinned/trust-store-backed certificate-chain policy with certificate-expiry and
revocation handling documented. Phase 0 must prove the exact signed-content
verification procedure against an official sample and a live release. Until
then, the pipeline may archive a release but may not call its signature
`verified`.

`config/data_licensing.yml` currently marks `ecb` as provisional. A named human
must review the applicable ECB copyright/reuse terms and record `reviewed_by`
before this dataset is redistributed or used for a commercial compliance
decision. Attribution to the ECB is retained in Gold metadata and downstream
exports.

## 8. CLI and operator contract

Phase 1 exposes:

```text
python -m fred_pipeline ecb-assets poll
python -m fred_pipeline ecb-assets ingest-release --release-url <release-url>
python -m fred_pipeline ecb-assets verify --release-id <id>
python -m fred_pipeline ecb-assets status
python -m fred_pipeline ecb-assets as-of --as-of <ISO-8601 UTC timestamp>
```

`poll` and `ingest-release` are safe to repeat. `verify` currently re-runs the
stored attachment signatures and signed MD5 comparison without mutating
accepted data. `status` emits JSON with the latest accepted release,
quarantine reasons, and current row count. Phase 2 adds archive `backfill` with
a dry-run inventory and resumable content-hash checkpoints; Phase 3 expands
`verify` to replay parsing, reconciliation, and interval invariants.

## 9. Phased implementation plan

### Implementation checkpoint — 2026-10-23

Delivered in Phase 1:

- exact-title RSS discovery and strict five-attachment release manifests;
- trust-store-backed PKCS#7 verification plus the ECB signed-MD5 comparison;
- immutable Bronze payloads and release/signature evidence;
- strict current-schema XML and UTF-16 CSV parsing with full-format parity;
- full-snapshot-authoritative N/U/D reconciliation and fail-closed quarantine;
- transactionally published SQLite/PostgreSQL SCD2 release, version, change,
  dictionary, current, history, change, and coverage objects;
- idempotent poll/manual ingestion, signature re-verification, status, and
  release-aligned as-of queries;
- default polling after `fred_pipeline run` on SQLite/PostgreSQL, with a
  `--no-ecb-assets` operator escape hatch and combined non-zero failure status;
  and
- Delta Bronze/Silver DDL and Gold view definitions, without a Spark writer.

The 2026-10-22 live release was ingested independently into SQLite and an
isolated PostgreSQL 16 database: all five signatures verified, XML/CSV parity
held, the full list contained 31,331 securities, and dictionary-enriched as-of
queries returned the same security state. A two-release fixture also proves
SQLite/PostgreSQL parity across update, deletion, new-security, interval, and
current-view behavior. Historical archive inventory/backfill, Spark runtime
parity, production scheduling/alerts, and the named licensing review remain
explicit open work.

The combined `fred_pipeline run` path was also live-verified against the
isolated PostgreSQL database: one ECB SDMX series persisted 7,182 observations
through 2026-10-22, the automatic MID poll completed successfully, and the
existing 31,331-security current view remained intact.

### Phase 0 — source-contract and security proof

1. Check in small, license-compliant fixtures for RSS, release XML, full/update
   XML, dictionary XML, and at least two historical CSV schemas. Record hashes
   and the live-probe report; do not commit issuer data beyond what tests need.
2. Prove MID signature verification against the official procedure and a live
   release. Add negative tests for changed bytes, wrong signature, expired/
   untrusted signer, and a release/attachment URL mismatch.
3. Inventory at least the 2010, 2025, and 2026 archive ZIPs to identify real
   schema and encoding variants, gaps, and filename/timestamp limits.
4. Complete the existing ECB licensing review for the new dataset's intended
   use.

Exit: source facts in this spec are either fixture-backed or corrected; no
database implementation starts on guessed historical schemas.

### Phase 1 — forward ingestion on SQLite and PostgreSQL

Build the RSS/release/artifact client, schema registry, normalizer, Bronze
retention, release ledger, version/change tables, Gold objects, CLI, and audit
records. Ingest two consecutive fixture releases covering `N`, `U`, `D`, no-op,
delete/re-add, and same-day republication. Prove SQLite/PostgreSQL row parity.

Exit: scheduled ingestion can follow new releases without history bootstrap,
and a cutoff before the second fixture release cannot see its changes.

### Phase 2 — archive bootstrap and gap semantics

Implement annual ZIP inventory, historical parsers, staging rebuild, coverage
ledger, schema migrations, and reconciliation to a live full snapshot. Backfill
from April 2010 through the forward-ingestion boundary, with every gap and
date-precision timestamp explicit.

Exit: the entire available archive is queryable; a report lists accepted,
missing, and quarantined dates and proves the final state equals the live full
snapshot at the same cutoff.

### Phase 3 — Delta parity and production operations

Add Delta DDL/MERGE behavior, partitioning/clustering chosen from measurements,
Spark parity tests, scheduled RSS polling, alerts, runbook, replay, backup, and
restore. Benchmark archive rebuild and common point-in-time queries on all
three backends before choosing optimization layout.

Exit: SQLite, PostgreSQL, and Delta return identical ordered rows and hashes for
the same fixture cutoffs, and a failed/quarantined release leaves the prior
Gold state live.

## 10. Acceptance criteria

| ID | Criterion |
| --- | --- |
| AC-001 | RSS processing selects only the exact eligible-assets title, then verifies `EligibleAssetsList` and the release title in the linked message |
| AC-002 | Every accepted release retains immutable full/update/dictionary artifacts, SHA-256 hashes, and successfully verified MID signature evidence |
| AC-003 | Reprocessing the same RSS item/release changes no row counts, intervals, hashes, or current-state result |
| AC-004 | Full-snapshot diffs and source update codes reconcile for consecutive releases; mismatch quarantines rather than partially publishing |
| AC-005 | Fixture cutoffs immediately before and at a release boundary prove no future membership or attribute value leaks backward |
| AC-006 | Tests cover new, update, delete, re-add, no-op, same-day correction, empty IDs, duplicate IDs, bad types, and interrupted transaction recovery |
| AC-007 | Historical schemas before and after at least one real field addition load with correct null/schema semantics; unknown forward fields fail closed |
| AC-008 | Archive gaps are visible in coverage output; carried-forward state always exposes the selected source release and gap/freshness status |
| AC-009 | A full historical staging rebuild produces non-overlapping half-open intervals and reconciles its boundary state to the corresponding ECB full snapshot |
| AC-010 | SQLite, PostgreSQL, and Delta return identical sorted row sets and canonical hashes for current, single-security-history, full-universe-as-of, and between-release change queries |
| AC-011 | A tampered artifact, invalid signature, schema drift, or release/update mismatch is present in Bronze/audit but cannot advance Silver/Gold |
| AC-012 | Missing RSS delivery can be recovered from the stable latest endpoints/download page without inventing a publication timestamp or duplicating a release |
| AC-013 | The ECB licensing entry is verified by a named reviewer before external redistribution; source attribution survives Gold/export paths |
| AC-014 | The live runbook demonstrates alerting on late, quarantined, and failed releases and recovery without hand-editing database rows |

## 11. Non-goals

- Determining legal collateral eligibility beyond what the public ECB list
  states.
- Ingesting non-marketable assets that the ECB says are outside this list.
- Pricing securities, computing market value, or applying portfolio-specific
  collateral rules.
- Treating the daily haircut as a security price or return series.
- Inferring identifier continuity when an ISIN/registration number changes.
- Replacing the ECB dictionary's published labels with an internally invented
  taxonomy.
- Using the online search form or SOAP subset query as the primary archive.
- Keeping only daily update files. Full snapshots are required for recovery and
  reconciliation.

## 12. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| RSS is finite and an outage misses an item | Idempotent release ledger, daily latest-file/download-page reconciliation, and annual archive repair |
| A schema change silently drops a risk-relevant field | Column-set hash, raw field preservation, schema registry, and fail-closed forward drift |
| Filename date is mistaken for knowledge time | Publication timestamp is the PIT boundary; filename date is metadata only |
| DST shifts the cutoff | Parse source-local time in `Europe/Frankfurt`, preserve RSS offset, store UTC and local values |
| Update file is incomplete or applied out of order | Full snapshot is membership authority; update is reconciled, never blindly chained |
| Historical gaps create false event dates | Coverage ledger and `derived_full_diff`; no exact-day claim across gaps |
| SCD2 intervals are corrupted by a late backfill | Chronological staging rebuild plus atomic publish and overlap checks |
| Digital-signature code gives false confidence | Official sample/live proof, negative tests, trust-chain policy, and no `verified` status before proof |
| The list is presented as a complete legal determination | Consumer naming and metadata say "listed as eligible" and retain the ECB caveat |
| Full daily expansion becomes unnecessarily large | Compact change intervals in Silver/Gold; exact full files retained in Bronze |

## 13. Decisions made by this spec

1. **RSS is the trigger; the linked release message is the manifest.** Static
   latest URLs are fallback/recovery only.
2. **Full snapshots are authoritative.** Daily update files accelerate review
   and provide a reconciliation check, but never define state alone.
3. **Publication timestamp is knowledge time.** Security dates do not backdate
   availability.
4. **History is compact SCD2 plus immutable raw snapshots.** No primary
   row-per-security-per-calendar-day table.
5. **Schema and signature failures fail closed.** Bronze evidence is kept;
   consumer state does not advance.
6. **The implementation is a dedicated ECB-assets path.** It shares transport,
   audit, storage, and backend utilities with the series pipeline, but it does
   not force a security master into `silver.fred_observation`.

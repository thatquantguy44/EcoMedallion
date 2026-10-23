# ECB Eligible Assets — Forward-Ingestion Runbook

This runbook operates the Phase 1 implementation from
[Spec 011](../../specs/spec011/README.md). It maintains a release-aligned,
point-in-time history of securities appearing on the ECB's List of eligible
marketable assets. It does not independently determine legal collateral
eligibility.

Phase 1 supports SQLite and PostgreSQL forward ingestion. Phase 2 can inventory
and normalize registered historical archive schemas, but the atomic staging
backfill is not implemented yet. The Spark/Delta writer also remains a later
phase; the checked-in Delta DDL is a schema contract, not a production writer.

## Prerequisites

- The runtime can reach `https://mid.ecb.europa.eu` over TLS.
- `openssl` is installed and its trust store is current. An attachment is not
  accepted unless OpenSSL verifies the PKCS#7 chain/signature and the embedded
  ECB digest matches the downloaded bytes.
- The selected SQLite file or PostgreSQL database is on durable storage and is
  included in the normal backup policy.
- A named reviewer has completed the ECB entry in
  `config/data_licensing.yml` before external redistribution or use in a
  commercial compliance decision.

## Initial forward load

Use a dedicated database in a test environment first:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets poll \
  --local --db-path /path/to/fred_local.db --env test
```

The finite RSS feed is processed oldest-first. On an empty eligible-assets
store, the first accepted full list is marked `starts_here`; this is not a
claim of historical coverage. Re-running the command is idempotent.

To ingest a specific RSS-linked release message:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets ingest-release \
  --release-url https://mid.ecb.europa.eu/rel/RELEASE.xml \
  --local --db-path /path/to/fred_local.db
```

The forward command rejects a release whose publication timestamp is not
later than the current accepted release. Historical data must use the planned
chronological archive rebuild; do not force an old release into the live
tables.

## Routine polling

`python -m fred_pipeline run --env dev` polls eligible-assets releases by
default after the observation pipeline finishes on SQLite or PostgreSQL. The
observation writes are already committed if ECB polling fails, but the combined
command exits non-zero and reports `ecb_assets.status=failed` so operations do
not mistake a partial refresh for complete success. Use `--no-ecb-assets` only
for a deliberate observation-only run; `--dry-run` skips ECB because it has no
durable warehouse.

The standalone `ecb-assets poll` command remains available for targeted retry
and for a separately scheduled check after the normal ECB publication window.
The ECB states that the list is normally published on weekdays except TARGET
closing days. If scheduled separately, use the `Europe/Frankfurt` timezone and
run a later retry so a delayed release can still be discovered.

Spark/Delta currently has schema DDL but no ECB writer. A Databricks-backed
`run` reports ECB polling as skipped rather than failing the observation run;
Phase 3 replaces that skip with the Delta writer.

The repository does not ship a separate enabled ECB schedule: deployment
owners must choose any additional retry cadence and alert destination.

## Historical archive inventory and schema review

Inventorying is read-only and does not initialize or modify a warehouse:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets inventory \
  --from-year 2010 --summary-only \
  --output /path/to/ecb_archive_inventory.json
```

The command checks every observed column set against
`config/ecb_eligible_asset_schemas.yml`. It exits non-zero if a header is
unknown, its registered validity range drifts, or the registry itself is
invalid. The JSON output retains `schema_registry_validation` even on failure
so an operator can review the new source schema before changing mappings.
Use `--schema-registry PATH` only to test a separately reviewed candidate.

The checked-in historical parser normalizes registered snapshots and preserves
every original source field in `source_fields_json`. It does not publish rows
to PostgreSQL yet; production history must wait for the chronological staging
rebuild and atomic parity checks required by Spec 011.

## Health and point-in-time queries

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets status \
  --local --db-path /path/to/fred_local.db

PYTHONPATH=src python -m fred_pipeline ecb-assets as-of \
  --as-of 2026-10-22T15:56:07+00:00 \
  --asset-key ISIN:AT0000172747 \
  --local --db-path /path/to/fred_local.db
```

An as-of timestamp selects the latest accepted release at or before the
cutoff, then evaluates half-open security intervals at that release. Every row
includes the selected release and its coverage status.

Alert when:

- no release has been accepted by the deployment's expected publication SLA;
- `quarantined` is non-empty;
- the latest `coverage_status` is not `complete` after the initial load;
- the job exits non-zero; or
- current eligible count or release-change counts move beyond thresholds
  calibrated from historical releases.

## Verification and quarantine recovery

Re-run signature verification against the immutable Bronze payloads:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets verify \
  --release-id RELEASE_ID \
  --local --db-path /path/to/fred_local.db
```

Normal recovery is to fix the underlying trust-store, transport, parser, or
source-data issue and repeat the same `poll` or `ingest-release` command. A
quarantined release does not advance Silver/Gold. Never hand-edit intervals or
mark a signature as verified.

`--allow-gap` is an explicit operator override for a verified full snapshot
whose update attachment cannot reconcile. It publishes the authoritative full
diff with `coverage_status=gapped` and
`reconciliation_status=derived_full_diff`. Record the incident and approver;
do not use the flag merely to bypass a parser, signature, or XML/CSV mismatch.

## Backup and restore check

Back up the Bronze release/artifact tables together with Silver. After a
restore, confirm:

1. `ecb-assets status` identifies the expected latest release and current
   count;
2. `ecb-assets verify --release-id ...` validates all five stored artifacts;
3. an as-of query before and at a known release boundary returns the expected
   membership; and
4. repeating `poll` produces only duplicate/no-op results.

The annual archive is not a substitute for the database backup: Bronze also
contains the exact signed payloads, hashes, release URL, signer evidence, and
retrieval audit used to accept each release.

# ECB Eligible Assets — Forward-Ingestion Runbook

This runbook operates the Phase 1 implementation from
[Spec 011](../../specs/spec011/README.md). It maintains a release-aligned,
point-in-time history of securities appearing on the ECB's List of eligible
marketable assets. It does not independently determine legal collateral
eligibility.

Phase 1 supports SQLite and PostgreSQL forward ingestion. Historical archive
bootstrap and the Spark/Delta writer remain later phases; the checked-in Delta
DDL is a schema contract, not a production writer.

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

Schedule `ecb-assets poll` on ECB publication days after the normal release
window, with the scheduler timezone set to `Europe/Frankfurt`. The ECB states
that the list is normally published on weekdays except TARGET closing days.
Run a later retry so a delayed release can still be discovered. Keep this job
independent from the observation-series pipeline so a quarantined ECB release
does not block unrelated sources.

The repository does not ship an enabled schedule: deployment owners must
choose the runtime, durable backend, secret/config injection, retry policy,
and alert destination before enabling one.

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

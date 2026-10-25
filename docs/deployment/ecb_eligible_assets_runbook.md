# ECB Eligible Assets — Forward-Ingestion Runbook

This runbook operates the Phase 1-3 implementation from
[Spec 011](../../specs/spec011/README.md). It maintains a release-aligned,
point-in-time history of securities appearing on the ECB's List of eligible
marketable assets. It does not independently determine legal collateral
eligibility.

Phase 1 supports SQLite and PostgreSQL forward ingestion. Phase 2 inventories,
normalizes, stages, reconciles, and atomically promotes the historical archive.
Phase 3 provides run-partitioned Delta archive staging, catalog-transaction
promotion, and current/history/as-of/change query contracts. Local Spark/Delta
fixtures prove those contracts; Databricks Runtime 18+ execution remains a
deployment acceptance step. The normal Spark-backed observation run still does
not poll forward MID releases, so the production schedule below targets the
PostgreSQL service backend.

## Prerequisites

- The runtime can reach `https://mid.ecb.europa.eu` over TLS.
- `openssl` is installed and its trust store is current. An attachment is not
  accepted unless OpenSSL verifies the PKCS#7 chain/signature and the embedded
  ECB digest matches the downloaded bytes.
- The selected SQLite file or PostgreSQL database is on durable storage and is
  included in the normal backup policy.
- A reviewer has completed the ECB entry in
  `config/data_licensing.yml` before external redistribution or use in a
  commercial compliance decision.

## Current production checkpoint

Review status: **Reviewed**. No individual owner is recorded.

The code, PostgreSQL-authoritative configuration contract, weekday systemd
resources, deterministic file alert drill, and explicit live alert drill are
implemented. Live Brevo SMTP delivery was proven on 2026-10-24: all four
deterministic alert scenarios were accepted, the drill produced a passing
manifest, and the recipient confirmed mailbox receipt. The setup SMTP key was
then rotated.

A production-profile poll against the local PostgreSQL `macro_medallion`
service on 2026-10-25 found the 2026-10-23 release, retained and verified all
five signed artifacts, then quarantined the release because its update contained
three keys absent from the computed full-snapshot diff. The real failure route
delivered an alert. Silver/Gold correctly remained at the accepted 2026-10-22
release and 31,331 current securities; no observation-series row carried the
ECB run ID. This proved the authoritative PostgreSQL path fails closed and uses
separate objects. Review then established that the three rows were ECB-declared
`U` records with identical prior-full, current-full, and update canonical
hashes. The narrow `source_noop` rule was implemented, and a normal repoll
accepted the release without `--allow-gap`: coverage is complete, Gold has
31,340 current securities, all three no-ops are audited, and none created a new
history interval. An immediate repoll was idempotent.

The next operator must install the protected configuration and repeat the
successful manual poll on the production Linux host. The timer must remain
disabled until that host-level gate passes. Do not commit the local alert YAML,
SMTP key, sender/recipient addresses, or PostgreSQL DSN.

### Authorize and prove live delivery — complete

1. Run from the production Linux host with a stable outbound IP. Authorize that
   IP in Brevo and verify the configured sender identity.
2. Rotate the SMTP key shared during initial setup. Install the replacement as
   `FRED_SMTP_PASSWORD` in the mode-`0600` service environment file; the YAML
   must contain only `password_env: FRED_SMTP_PASSWORD`.
3. Load the production environment and run the live drill shown under
   **Prove ECB alert classification, routing, and rendering**. Do not weaken IP
   controls merely to make a workstation proof pass.
4. Retain `manifest.json`, provider acceptance/message IDs when available, and
   recipient confirmation that all four clearly marked synthetic messages
   arrived. Provider acceptance alone is not mailbox-delivery proof.
5. The 2026-10-24 proof satisfied this gate. Proceed to one manual PostgreSQL
   poll and database verification before timer activation.

The focused handoff at
[`docs/handoffs/ecb_eligible_assets.md`](../handoffs/ecb_eligible_assets.md)
contains the complete resume order and completion evidence.

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
default after the observation pipeline finishes when the selected SQLite or
PostgreSQL backend equals `ecb_assets_authoritative_backend`. The observation
writes are already committed if ECB polling fails, but the combined command
exits non-zero and reports `ecb_assets.status=failed` so operations do not
mistake a partial refresh for complete success. If the observation warehouse is
not authoritative for ECB, the combined run reports an explicit skip and the
dedicated poller owns ingestion. Use `--no-ecb-assets` only for a deliberate
observation-only run; `--dry-run` skips ECB because it has no durable warehouse.

### Authoritative destination

PostgreSQL is the current authoritative production destination. It is selected
independently from the observation-series warehouse so Databricks can remain
the production observation backend without receiving duplicate ECB writes:

```yaml
default:
  primary_backend: postgres
  ecb_assets_authoritative_backend: postgres
```

The setting lives in the warehouse profile and can be overridden at deployment
time with `FRED_ECB_ASSETS_AUTHORITATIVE_BACKEND`. Valid values are `local`,
`postgres`, `databricks`, and `disabled`. Dedicated `poll`, `ingest-release`,
and `backfill-promote` writes fail closed if warehouse fallback selects a
different backend. `databricks` reserves the configuration contract, but
forward MID polling there is not yet implemented; selecting it for `poll` or
`ingest-release` produces a clear error before any ECB request. `local` is for
development only, and `disabled` turns authoritative writes off explicitly.

To change the production authority later, update both the poller's
`primary_backend` and `ecb_assets_authoritative_backend`, provision the target,
run backend parity and restore drills, then cut the scheduler over. Never run
two authoritative pollers during a cutover.

The standalone `ecb-assets poll` command is the production scheduler entry
point. The repository ships a systemd service/timer template under
`resources/ecb-eligible-assets.*`. It runs three idempotent weekday checks in
Frankfurt local time, using the canonical IANA zone `Europe/Berlin`:

| Local time | Purpose |
| --- | --- |
| 17:45 | Discover an early release, matching the timing observed in live evidence |
| 18:25 | Retry after the ECB's stated 18:15 publication time |
| 21:20 | Retry after the configured 180-minute grace period and alert if late |

The explicit timezone follows CET/CEST automatically. The timer still fires on
a TARGET closing day, but `is_target_operating_day()` makes the health check
expect the preceding operating date, so the run is an idempotent no-op without
a false missing-release alert. systemd will not overlap the oneshot service if
one poll is still active when the next trigger arrives.

### Install the PostgreSQL production timer

The checked-in `config/warehouse.yml` selects Databricks for production
observation series while explicitly declaring PostgreSQL authoritative for ECB.
Do not use it unchanged for this host-side poller because its primary backend
still initializes Databricks. Install the provided PostgreSQL service profile
and an environment file whose DSN comes from a protected environment variable:

```bash
sudo install -d -m 0750 /etc/fred-pipeline
sudo install -m 0640 resources/ecb-assets-warehouse.yml.example \
  /etc/fred-pipeline/warehouse.yml
sudo install -m 0600 resources/ecb-assets.env.example \
  /etc/fred-pipeline/ecb-assets.env
sudo install -m 0644 resources/ecb-eligible-assets.service \
  /etc/systemd/system/ecb-eligible-assets.service
sudo install -m 0644 resources/ecb-eligible-assets.timer \
  /etc/systemd/system/ecb-eligible-assets.timer
```

Edit `/etc/fred-pipeline/ecb-assets.env` and replace the DSN placeholder. Copy
the application and alerting configs to the paths named there. In the alerting
config, set `enabled: true`, retain `policy: failure`, restrict `environments`
to `[prod]`, configure real recipients/transport, and keep
`ecb_assets.expected_release_time_local: "18:15"` with
`grace_period_minutes: 180` unless operations approves a different SLA.
Create the unprivileged `fred-pipeline` user, install the repository and virtual
environment at `/opt/fred-bronze-to-gold-pipeline`, and grant that user only the
required database and configuration access.

### Alert transport choices

The alert layer supports generic SMTP, Outlook/Microsoft 365 SMTP, and Outlook
through Microsoft Graph without changing pipeline code.

For a no-cost, low-volume production setup, the recommended starting point is
the Brevo SMTP template:

```bash
sudo install -m 0640 resources/ecb-alerting.brevo-smtp.yml.example \
  /etc/fred-pipeline/alerting.yml
```

Brevo's free plan currently includes 300 sends per day. Configure a verified
sender/domain, replace the SMTP login and recipient placeholders, and store the
Brevo SMTP key only in `FRED_SMTP_PASSWORD`. The relay is
`smtp-relay.brevo.com:587` with STARTTLS. Gmail SMTP is another free option for
very small installations (`smtp.gmail.com:587`), but it requires 2-Step
Verification and an app password and is less suitable for a service identity.

For an existing Outlook/Microsoft 365 tenant, prefer Microsoft Graph:

```bash
sudo install -m 0640 resources/ecb-alerting.outlook-graph.yml.example \
  /etc/fred-pipeline/alerting.yml
```

Replace the tenant/client IDs, sender, and recipients, grant the Entra
application `Mail.Send` application permission with admin consent, and store
only the client secret in `FRED_GRAPH_CLIENT_SECRET`. The existing
`outlook_smtp` transport remains available for a tenant that explicitly permits
SMTP AUTH, but it uses username/password authentication; Microsoft recommends
OAuth/Graph and is retiring Basic authentication for SMTP AUTH. Do not choose
Outlook SMTP for a new unattended deployment.

Any other relay uses `kind: smtp` with its host, port, username, and a
`password_env` reference. No secret belongs in YAML. Whichever transport is
selected, run the safe file drill first and the explicit live drill second.

Validate the exact next trigger times, then run one manual production poll
before enabling the timer:

```bash
systemd-analyze calendar \
  'Mon..Fri *-*-* 17:45:00 Europe/Berlin' \
  'Mon..Fri *-*-* 18:25:00 Europe/Berlin' \
  'Mon..Fri *-*-* 21:20:00 Europe/Berlin'
sudo systemctl daemon-reload
sudo systemctl start ecb-eligible-assets.service
sudo systemctl status ecb-eligible-assets.service
sudo systemctl enable --now ecb-eligible-assets.timer
systemctl list-timers ecb-eligible-assets.timer
```

Inspect scheduled-run output with:

```bash
journalctl -u ecb-eligible-assets.service --since today
```

Keep the timer disabled until the manual poll succeeds and alert delivery has
been proven. A Databricks-backed `fred_pipeline run` continues to report
forward ECB polling as skipped; the implemented Delta path currently covers
historical staging/promotion and query parity, not this forward scheduler.

### Prove ECB alert classification, routing, and rendering

Run the safe drill before configuring a live mailbox:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets alert-drill \
  --output /tmp/ecb-alert-drill --env prod
```

The command does not open a warehouse, call the ECB, or contact an email
service. It forces the configured partial/failure routes through the file
transport for four deterministic cases: late release, quarantined release,
signature failure, and ingestion failure. A unique output directory contains
four HTML/text pairs and `manifest.json`; a missing delivery, wrong verdict, or
missing rendition makes the command exit non-zero. Placeholder routes are used
only when the loaded alerting config has no recipient for that verdict.

This proves application-side classification, routing, rendering, and transport
invocation. Production acceptance additionally requires configuring a real
SMTP or Microsoft Graph transport. After confirming the recipients know that
four synthetic messages are coming, run:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets alert-drill \
  --env prod --live-alert-delivery \
  --output /var/lib/fred-pipeline/ecb-alert-drill
```

Live mode refuses disabled alerting, console/file transports, `policy: never`,
environment mismatches, or missing partial/failure routes. Its manifest proves
that the real transport accepted each message; record recipient confirmation
and provider message IDs in the deployment record to prove mailbox receipt.
Do not enable the timer based only on transport acceptance or console logs.

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
every original source field in `source_fields_json`.

Build or resume an isolated staging history with:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets backfill-stage \
  --from-year 2010 --to-year 2026 \
  --archive-dir data/ecb \
  --backfill-run-id ecb-history-2010-2026 \
  --env dev
```

`--archive-dir` is optional; when present, completed-year ZIPs and current-year
daily files are cached there and `/data/` is ignored by Git. Every accepted
snapshot commits independently, so repeating the same command resumes from its
last staged release. If reviewed inputs or the requested start change, use a
new run ID. `--restart` deletes only that run's staging rows and rebuilds it.

A malformed snapshot after the first accepted full state is persisted as
`quarantined` and does not advance security state. Processing continues with
the next valid authoritative full snapshot, whose release is marked `gapped`
and non-consecutive. A malformed initial snapshot still fails closed because
there is no prior state to carry. Never resolve a conflicting duplicate by
arbitrarily selecting one row; review the quarantine and require exact final
boundary parity before promotion.

The command writes only these new objects:

- `silver.ecb_eligible_asset_backfill_run`;
- `silver.ecb_eligible_asset_release_stage`;
- `silver.ecb_eligible_asset_version_stage`; and
- `silver.ecb_eligible_asset_change_stage`.

A successful command returns `status: staged` after interval and final-state
hash checks. It still does not publish rows to the live ECB tables or Gold.
After reviewing every quarantine and proving exact parity to the signed live
full snapshot at the same cutoff, promote with the exact quarantine count:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets backfill-promote \
  --backfill-run-id ecb-history-2010-2026 \
  --acknowledge-quarantines 8 \
  --env dev
```

Promotion repeats the parity and interval checks, maps the final archive
transition onto the matching signed live release ID and exact publication
timestamp, and replaces the bootstrap-only Silver versions/changes in one
transaction. A missing or incorrect acknowledgement fails before mutation. Any
post-merge invariant failure rolls back the transaction. Repeating a successful
promotion is an idempotent no-op.

When the configured warehouse exposes Spark, the same `backfill-stage` and
`backfill-promote` commands select the Delta staging adapter and promoter.
Delta promotion requires Databricks Runtime 18.0+ Unity Catalog managed tables
because it uses one catalog `BEGIN ATOMIC` transaction; unsupported runtimes
fail closed before replacing live history. Run the fixture parity suite and a
non-production catalog promotion before enabling a production Delta workflow.

The development PostgreSQL proof run `ecb-history-2010-2026` completed through
`2026-10-22`: 4,248 accepted snapshots, nine missing TARGET dates, eight
duplicate-key quarantines, 1,964,303 compact versions/changes, and 31,331 final
assets. Final staged/live key and row hashes matched exactly in both directions;
duplicate-open and invalid-interval counts were zero. Promotion produced 4,265
live coverage rows and 1,964,303 live compact versions/changes. The signed
boundary now records 88 new, 152 updated, and 102 deleted securities; Gold
current remains 31,331.

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

The ECB can declare a `U` row even when no published field changed. The normal
poll accepts that row without an override only when the key exists in both
adjacent full snapshots and all three canonical hashes (prior full, current
full, and update) are identical. The release records
`reconciliation_status=matched_with_source_noops`; the change audit records
`change_origin=source_noop` with a null computed change type, and no new history
interval is created. Any other extra update row remains a reconciliation
failure.

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

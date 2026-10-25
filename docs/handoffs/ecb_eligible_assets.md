# ECB Eligible Assets Production Handoff

Review status: **Reviewed**

Primary owner: Not recorded, by request

Last verified: 2026-10-25

This is the active pickup point for
[Spec 011](../../specs/spec011/README.md). PostgreSQL is the authoritative
production destination for forward ECB eligible-assets ingestion. That choice
remains configurable through `ecb_assets_authoritative_backend`; it is not a
hard-coded dependency.

## Current state

The repository provides:

- forward RSS/release ingestion with immutable Bronze evidence, accepted
  Silver history, and Gold current/history/as-of/change query contracts;
- the 2010-forward historical archive inventory, reviewed schema registry,
  resumable staging, reconciliation, quarantine acknowledgement, and atomic
  PostgreSQL promotion;
- bounded PostgreSQL Gold construction that avoids materializing the complete
  Silver observation history in application memory;
- a dedicated PostgreSQL systemd service and weekday timer using Frankfurt
  business rules and the canonical `Europe/Berlin` IANA time zone;
- ECB-specific late, quarantine, signature-failure, and ingestion-failure
  alerts through shared SMTP/Microsoft Graph transports; and
- deterministic file and explicit live delivery drills.

Live alert delivery is complete. On 2026-10-24 Brevo accepted all four
deterministic scenarios, the drill exited zero with a passing manifest, and the
recipient confirmed mailbox receipt. The SMTP key exposed during initial setup
was subsequently rotated. The production timer is not active from this macOS
workspace, which has no systemd service manager or production PostgreSQL DSN.

On 2026-10-25 the dedicated production warehouse profile was exercised against
the repository's healthy PostgreSQL `macro_medallion` service. The poll found
the signed 2026-10-23 release but quarantined it because the update declared
three ISIN changes not present in the computed full-snapshot diff. The behavior
was correct and fail-closed:

- all five artifacts were stored and their ECB signatures re-verified;
- the real failure alert was delivered;
- all nine dedicated Bronze/Silver/Gold ECB relations were present;
- the release stayed in Bronze as `quarantined`;
- Silver/Gold stayed on 2026-10-22 with 31,331 current securities; and
- the ECB run ID appeared in zero `silver.fred_observation` rows.

This is a production-path and isolation proof on local PostgreSQL, not evidence
that an external production host or its timer has been activated. The successful
manual poll gate remains open because no new release was promoted.

## Pick up here

Alert identity and live delivery gates are complete. The PostgreSQL path was
proven to fail closed; continue at the reconciliation incident below, repeat a
successful manual poll, then activate the timer. Do not activate it early.

### 1. Establish the production alert identity — complete

- Use the production Linux host and determine its stable outbound IP.
- Authorize that IP in Brevo and verify the configured sender identity.
- Rotate the SMTP key shared during initial setup, then install only the new
  value in the protected service environment as `FRED_SMTP_PASSWORD`.
- Install the tracked Brevo example as `/etc/fred-pipeline/alerting.yml`, fill
  in real sender/routes there, and keep the credential itself out of YAML.

### 2. Prove real delivery — complete

With the production environment loaded:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets alert-drill \
  --env prod --live-alert-delivery \
  --output /var/lib/fred-pipeline/ecb-alert-drill
```

The completed gate required:

- the command exits zero;
- `manifest.json` records four accepted scenarios;
- the provider acceptance or message IDs are retained when available; and
- the recipient confirms all four synthetic messages arrived.

The 2026-10-24 run satisfied all four conditions. If a future run returns
`525`, review Brevo's authorized-IP configuration; if it rejects the sender,
review sender/domain verification. Do not replace the live drill with the file
drill.

### 3. Resolve the reconciliation incident — pick up here

Release `2026-10-23T15:56:12Z` is quarantined because its update has three
extra keys relative to the authoritative full-snapshot diff:

```text
ISIN:EU000A4E1NP4
ISIN:EU000A4E2KT0
ISIN:XS3420368998
```

Compare the signed update rows with the prior accepted and new full snapshots.
If the ECB republishes corrected data, repeat the normal poll. If review
concludes that the signed full snapshot should be accepted despite the update
mismatch, record that decision and use the existing explicit `--allow-gap`
operator path. Do not modify intervals or release status by hand.

### 4. Complete the authoritative PostgreSQL write proof

Install `resources/ecb-assets-warehouse.yml.example` and
`resources/ecb-assets.env.example` as described in the
[ECB runbook](../deployment/ecb_eligible_assets_runbook.md). Put the real DSN
only in the protected environment, then run:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets poll --env prod
PYTHONPATH=src python -m fred_pipeline ecb-assets status --env prod
```

Record the run ID and verify that the poll is successful or an idempotent
no-op, the latest accepted release/current count are sensible, the release row
is terminal, and the writes are confined to the dedicated new ECB tables/views.
At minimum, inspect the release ledger, version/change history, current view,
and coverage status named in Spec 011. Do not count an automatic warehouse
fallback as a successful proof.

### 5. Activate and observe the schedule

Validate the calendar expressions, manually start the service once, and enable
the timer using the commands in the runbook. Capture:

- `systemctl status ecb-eligible-assets.service`;
- `systemctl list-timers ecb-eligible-assets.timer`;
- the relevant `journalctl` output; and
- PostgreSQL evidence for the resulting run.

Observe at least one weekday scheduled cycle. A no-new-release result is valid
if the run is successful and idempotent. Exercise or observe a real failure
route and confirm the production recipient receives it.

### 6. Finish the remaining Phase 3 proof

After PostgreSQL production activation, run the replay/backup/restore drill and
the Databricks Runtime 18+ catalog-transaction cases. Benchmark common
point-in-time queries before choosing a Delta clustering layout. These do not
block the PostgreSQL scheduler, but they remain open Spec 011 work.

## Safety and repository state

- Never commit `.env`, `config/*.local.yaml`, SMTP keys, sender/recipient
  addresses, provider message contents, or database DSNs.
- Keep PostgreSQL authoritative until a separately proven cutover changes both
  the poller backend and `ecb_assets_authoritative_backend`.
- Never operate two authoritative pollers during a cutover.
- A quarantined release must not advance Silver/Gold; recover by fixing the
  cause and replaying, not by editing intervals manually.

The operational command details and recovery procedures remain authoritative
in the [ECB runbook](../deployment/ecb_eligible_assets_runbook.md); this file
records sequencing and the next natural resume point.

# ECB Eligible Assets Production Handoff

Review status: **Reviewed**

Primary owner: Not recorded, by request

Last verified: 2026-10-24

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

The local, gitignored Brevo profile was validated on 2026-10-24. It loaded an
enabled production-only SMTP transport, a protected environment credential,
and both partial/failure recipient routes. The live drill reached Brevo but
failed before sending its first message:

```text
525 5.7.1 Unauthorized IP address
```

No provider accepted a test message, no live manifest was produced, and inbox
delivery is not yet proven. The production timer is not active from this macOS
workspace, which has no systemd service manager or production PostgreSQL DSN.

## Pick up here

Complete these gates in order. Do not activate the timer early.

### 1. Establish the production alert identity

- Use the production Linux host and determine its stable outbound IP.
- Authorize that IP in Brevo and verify the configured sender identity.
- Rotate the SMTP key shared during initial setup, then install only the new
  value in the protected service environment as `FRED_SMTP_PASSWORD`.
- Install the tracked Brevo example as `/etc/fred-pipeline/alerting.yml`, fill
  in real sender/routes there, and keep the credential itself out of YAML.

### 2. Prove real delivery

With the production environment loaded:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets alert-drill \
  --env prod --live-alert-delivery \
  --output /var/lib/fred-pipeline/ecb-alert-drill
```

The gate passes only when:

- the command exits zero;
- `manifest.json` records four accepted scenarios;
- the provider acceptance or message IDs are retained when available; and
- the recipient confirms all four synthetic messages arrived.

If Brevo again returns `525`, fix its authorized-IP configuration. If it
rejects the sender, complete sender/domain verification. Do not replace the
live drill with the file drill; the latter is already proven.

### 3. Prove the authoritative PostgreSQL write

Install `resources/ecb-assets-warehouse.yml.example` and
`resources/ecb-assets.env.example` as described in the
[ECB runbook](../deployment/ecb_eligible_assets_runbook.md). Put the real DSN
only in the protected environment, then run:

```bash
PYTHONPATH=src python -m fred_pipeline ecb-assets poll --env prod
PYTHONPATH=src python -m fred_pipeline ecb-assets status --env prod
```

Record the run ID and verify that the poll is successful or an idempotent
no-op, the latest accepted release/current count are sensible, the audit row is
terminal, and the writes are confined to the dedicated new ECB tables/views.
At minimum, inspect the release ledger, version/change history, current view,
and coverage status named in Spec 011. Do not count an automatic warehouse
fallback as a successful proof.

### 4. Activate and observe the schedule

Validate the calendar expressions, manually start the service once, and enable
the timer using the commands in the runbook. Capture:

- `systemctl status ecb-eligible-assets.service`;
- `systemctl list-timers ecb-eligible-assets.timer`;
- the relevant `journalctl` output; and
- PostgreSQL evidence for the resulting run.

Observe at least one weekday scheduled cycle. A no-new-release result is valid
if the run is successful and idempotent. Exercise or observe a real failure
route and confirm the production recipient receives it.

### 5. Finish the remaining Phase 3 proof

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

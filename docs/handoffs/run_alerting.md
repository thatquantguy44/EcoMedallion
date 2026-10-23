# Run Alerting & Stage Tracking — Spec & Operations Guide

**Status:** implemented and tested; shipped **disabled** (opt-in)
**Owner:** pipeline / operations
**Config:** [`config/alerting.yml`](../../config/alerting.yml)
**Code:** `src/fred_pipeline/governance/{stages,alerting_config,email_report,email_transport,alerting}.py`
**Tests:** `tests/test_alerting.py` (43)
**Related:** [`powerbi_report_build_plan.md`](powerbi_report_build_plan.md) §Report 14

---

## 1. The problem this solves

`FredPipeline.run()` rebuilds the Gold layer like this:

```python
try:
    self.warehouse.build_gold()
except Exception:
    log.exception("Gold refresh failed for run %s", run.run_id)
```

The failure is logged and the run continues. By that point `run.finalize()` has
already set `RunStatus` from **series outcomes alone** — how many series
extracted cleanly. So a run whose Gold build blew up:

- reports `RunStatus.SUCCEEDED`,
- writes a clean row to `audit.etl_run`,
- fires a webhook saying `✅ SUCCEEDED`,
- and leaves **every Power BI report serving yesterday's tables**.

Nothing in the audit trail contradicts it, because the audit trail is answering
a different question. `RunStatus` is not wrong — "did the series ingest" is a
legitimate thing to record, and the Pipeline Health report depends on it. It
just is not the same question as "did this run do its job".

Stage tracking answers the second question, and the summary email is how a
human hears the answer.

## 2. Design

Five modules, each doing one thing, following the repo's existing conventions
(pure logic in `src`, config-driven YAML with a validating loader, I/O imported
lazily, transports injectable so tests never touch a network).

| Module | Responsibility |
|---|---|
| `governance/stages.py` | Record what each phase did. Pure, dependency-free. |
| `governance/alerting_config.py` | Load and validate `config/alerting.yml`. |
| `governance/email_report.py` | Render a summary to subject + text + HTML. Pure. |
| `governance/email_transport.py` | Actually send it (Outlook SMTP, Graph, console, file). |
| `governance/alerting.py` | Tie the four together; decide whether to send. |

The existing Slack webhook in `governance/notify.py` is **unchanged** and still
fires independently. This is an additional channel with a richer payload, not a
replacement — a team already reading the webhook loses nothing.

### 2.1 ECB eligible-assets alerts

The eligible-assets poll runs after the observation pipeline, so it has a
dedicated health assessment in `governance/ecb_alerting.py`. It reuses this
file's master switch, policy, environments, routes, and transport, while adding
an `[ECB]` subject tag and ECB-specific details. Both the default `run` command
and standalone `ecb-assets poll`, `ingest-release`, and `verify` commands invoke
it.

The `ecb_assets` block controls the expected 18:15 Europe/Berlin publication
time, grace period, large-change threshold, and gap/quarantine alerts. TARGET
weekends and closing days are excluded from freshness expectations. A late,
gapped, or unusually large release is `partial`; a failed/quarantined release,
invalid signature, or missing accepted baseline is `failure`.

### 2.2 Stages

`EXPECTED_STAGES` declares the phases `run()` actually has:

| Stage | Required | What it covers |
|---|---|---|
| `plan` | ✅ | Resolve each series' load window against the warehouse |
| `extract` | ✅ | Fetch from source APIs; write Bronze, Silver, DQ |
| `gold` | ✅ | Rebuild the Gold analytical layer |
| `release_calendar` | ❌ | Re-fetch the forward economic release calendar |
| `persist` | ✅ | Write run and series audit rows |

Stages are **declared, not discovered**. A phase that never ran comes back as
`NOT_RUN` rather than simply being absent from the report — the failure where a
whole phase is skipped by a bad flag otherwise looks identical to success if you
only read the stages that reported.

`swallow=True` on a stage records a failure without re-raising. `gold` and
`release_calendar` use it, preserving the existing behaviour that a Gold failure
does not discard a run whose ingestion succeeded — but the failure is now
*recorded* rather than only logged.

### 2.3 The verdict

`overall_verdict(records, series_failed, series_total)` returns
`(verdict, reason)`:

| Verdict | When |
|---|---|
| `failure` | a **required** stage failed or never ran, or every series failed |
| `partial` | some series failed, or an optional stage failed, or a stage warned |
| `success` | everything completed |

The rule that matters: **a required stage failing is a failure even when every
series succeeded.** That is the Gold case above.

## 3. What an alert looks like

```
SUBJECT: [FRED][prod] FAILURE — required stage(s) failed: gold (2820/2820 series ok)

❌ FAILURE — required stage(s) failed: gold

run       : 9f3c1a2b
environment: prod
started   : 2026-08-13 06:00:00 UTC
duration  : 12m 34s
triggered : databricks-job

STAGES
------
 ✓ plan               succeeded  1.2s
      series_planned: 2820
 ✓ extract            succeeded  11m 40s
      series_succeeded: 2820
      series_failed: 0
      sources: bea, bis, bls, fred, tiingo
 ✗ gold               failed     41.0s
      ERROR RuntimeError: Delta commit conflict on gold.dim_series
 – release_calendar   skipped    n/a
      reason: gold layer not built
 ✓ persist            succeeded  0.9s
      series_rows: 2820
```

The subject is designed to be **actionable without opening the mail**. Note
`2820/2820 series ok` sitting next to `FAILURE` — that juxtaposition is the
whole point: ingestion was perfect and the run still failed.

An HTML part is sent alongside the text. It is table-based with inline styles
because Outlook's rendering engine ignores most modern CSS, and a summary that
arrives unreadable is a summary nobody reads.

## 4. Configuration

Everything lives in [`config/alerting.yml`](../../config/alerting.yml).
Resolution: explicit path → `$FRED_ALERTING_FILE` → `config/alerting.yml`.

A **missing** file disables alerting quietly — it is opt-in, and a pipeline with
no alerting file should run normally. A **malformed** file raises: silently not
alerting because of a YAML typo is the exact failure this exists to prevent.

### 4.1 Two rules the loader enforces

**No secrets in the file.** `password_env` / `client_secret_env` name an
*environment variable*; they never hold a value. A config that looks like it
inlines a credential is **rejected**, because this file is in git and a rejected
config is a far better outcome than a leaked mailbox password. `${VAR}`
interpolation is also rejected rather than silently ignored — a config that
looks like it works but doesn't is worse than one that fails.

**Recipients are validated.** Every address is checked on load. A typo'd address
means an alert that silently goes nowhere while you believe you are covered,
which is worse than having no alerting at all.

### 4.2 Transports

| `kind` | Use it when |
|---|---|
| `outlook_smtp` | Microsoft 365 / Outlook over SMTP+STARTTLS (`smtp.office365.com:587`) |
| `smtp` | Any other relay; internal relays often need no auth at all |
| `microsoft_graph` | **Tenants with SMTP AUTH disabled** — increasingly the default |
| `console` | Default. Logs the message instead of sending. |
| `file` | Writes `.html`/`.txt` to a directory — good for CI and for eyeballing the HTML |

`console` is the default deliberately: alerting should be **visible before it is
configured**. A pipeline with no mail setup still emits the summary where an
operator can read it, rather than doing nothing and appearing healthy.

> **Microsoft 365 note.** Many tenants disable SMTP AUTH by default. If
> `outlook_smtp` fails with an authentication/policy error, either have it
> enabled for the sending mailbox or switch to `microsoft_graph`. The SMTP
> transport's error message says exactly this, because the raw `smtplib` error
> does not.

### 4.3 Routes

```yaml
routes:
  failure:
    to: [data-oncall@example.com]
    cc: [analytics-leads@example.com]
  partial:
    to: [data-oncall@example.com]
```

Keys are `success` / `partial` / `failure` / `default`. `bcc` is supported and is
passed as an **envelope** recipient, never a header — a `Bcc:` header would show
every recipient the bcc list.

## 5. Turning it on

1. Pick a transport and fill in `config/alerting.yml`.
2. Export the secret it names, e.g.
   `export FRED_SMTP_PASSWORD='...'`.
3. Dry-run it without sending anything real:
   ```yaml
   enabled: true
   policy: always
   transport: {kind: file, output_dir: ./alerts}
   ```
   Run the pipeline, then open `alerts/*.html` in a browser. This is the
   recommended first step — it exercises rendering and routing with zero risk of
   mailing the wrong people.
4. Switch `kind` to `outlook_smtp` or `microsoft_graph`, keep `policy: always`
   for one run to confirm delivery, then drop to `policy: failure`.
5. Consider `environments: [prod]` once dev runs stop being interesting.

## 6. Guarantees

**Alerting never fails a run.** Every path in `send_run_alert` is caught and
logged. A pipeline that ingested 2,820 series correctly must not be reported as
failed because a mail server was briefly unreachable.

**A run that dies still alerts.** A hard extract failure sends the alert *before*
propagating, so the run that crashed is heard about, not just the ones that
finished.

**A bad outcome is never silent.** When the policy suppresses an email for a
non-success verdict, the summary is still logged at WARNING.

## 7. Testing

43 tests in `tests/test_alerting.py`, none of which touch a network or a
mailbox: transports are injected, and the SMTP path runs against a fake
`smtplib.SMTP`.

Covered: stage recording, swallow semantics, `NOT_RUN` detection, every verdict
branch, config validation (inline secrets, `${}` interpolation, malformed
addresses, unknown keys, missing transport fields), policy and environment
gating, subject/text/HTML rendering including HTML escaping, SMTP STARTTLS +
login + envelope-vs-header bcc, the auth-failure message, the file transport,
and an **end-to-end test through the real `FredPipeline.run()`** asserting that
a broken `build_gold` with zero failed series produces a `FAILURE` alert naming
the `gold` stage.

### Not covered

**No message has ever been sent to a real mailbox.** The SMTP and Graph
transports are verified against fakes only — the authoring environment has no
outbound mail. The logic is tested; the first real send is still the proof.
Use the `file` transport dry-run in §5 before pointing this at a distribution
list.

## 8. Possible extensions

Deliberately not built, in rough order of value:

- **Per-source stage detail.** `extract` currently records aggregate counts; a
  per-source breakdown would show "Tiingo quota exhausted" without opening the
  audit tables.
- **Threshold alerts.** Alert when DQ pass rate drops below N%, or when stale
  series exceed N — currently only run outcomes trigger mail.
- **Digest mode.** One daily summary instead of one email per run, for
  environments that run hourly.
- **Teams/Slack cards.** The renderer is pure and already produces structured
  records; a card payload is another formatter, not a new subsystem.

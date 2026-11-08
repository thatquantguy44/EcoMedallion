# Spec 016: Dagster, configurable email, and outbound network selection

Status: **Pipeline/ECB routing, core email additions, CLI FOMC refresh and optional Dagster runtime implemented; Dagster jobs and remaining networking are draft.**

Date: 2026-11-04

Review updates, 2026-11-04: the user confirmed **main pipeline and ECB jobs**,
then accepted the recommended additions: **warehouse health checks, Gold-only
rebuild, and metadata reconciliation**. The full pipeline must also enable
**FOMC refresh by default**. Detailed per-table assets remain deferred. These
confirm the workload scope; the remaining specification is still under review.

Branch: `feature/dagster-alerts-network-spec`, created from the locally available
`origin/main` at `8f88ff7` and published to the matching remote branch.

## Objective

Let operators run the pipeline through Dagster, choose an alert email provider,
and run behind a proxy or with direct internet access. In automatic mode, test
the applicable connections at startup, choose the working routes, and reuse
those choices throughout that pipeline invocation. Avoid trying both routes on
every source request.

The implemented slices cover managed startup route selection for the main
pipeline's source clients, FOMC refresh, and ECB eligible-assets polling in the
main hook and standalone poll, plus default-on CLI FOMC refresh. Named email
profiles, SMTP TLS/auth and proxy options, Outlook OAuth, Graph attachments,
delivery outcomes and email diagnostics are also implemented. Graph text/HTML
parity for messages with BCC, other outbound consumers and Dagster jobs remain work; live mailbox
acceptance and receipt have not been verified.
The optional Dagster dependency set is locked and clean-install verified; it
does not yet provide jobs or activate schedules.

## Capability map

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| outbound-network | Validated settings, startup probes, immutable run route plan, configured HTTP sessions and SMTP connections | Existing configuration and injectable transports |
| alert-email | Provider profiles, SMTP TLS/auth options, personal Outlook.com OAuth, existing Microsoft 365 Graph, diagnostics | outbound-network |
| dagster-integration | Optional runtime; main, ECB, warehouse health, Gold-only and metadata reconciliation jobs; default-on full-run FOMC refresh; truthful outcomes, shared alerts, stopped schedules and cutover | outbound-network, alert-email, existing Spec 013 ECB execution/lock contracts |

Proposed build order: outbound-network → alert-email → dagster-integration.
Review these boundaries together with the module specifications:

- [Outbound networking](SPEC-outbound-network.md)
- [Email delivery](SPEC-alert-email.md)
- [Dagster integration](SPEC-dagster-integration.md)

The workload decisions recorded above are confirmed. Remaining module
contracts/defaults are proposals for review, not an implementation checklist.

## Existing behavior inspected

| Evidence in this checkout | Consequence for the design |
| --- | --- |
| `config.py`: explicit arguments > environment > YAML > defaults; optional flat or environment-specific YAML | Extend this resolver; keep existing installations working |
| `sources/base.py`: injectable `requests.Session`, rate limiting and bounded retries | Inject a session configured from the run plan; preserve source-specific behavior |
| `pipeline.py`: cached source clients and per-source extraction thread pools | Reset owned clients between runs and share immutable route decisions across workers |
| `ecb_eligible_assets.py`: `ECBMIDClient(HTTPSource)` | MID polling and artifacts use the same network contract |
| `ecb_archive_inventory.py`: separate streaming Requests session | Wire its downloads explicitly; source-base changes alone are insufficient |
| `catalogs/fomc_calendar.py`, `governance/notify.py`, `governance/email_transport.py`: direct Requests calls | Include calendar refresh, webhook, Graph token and Graph send paths |
| Alerting already supports `smtp`, `outlook_smtp`, `microsoft_graph`, `console`, `file` | Extend the existing renderer, routes and transports instead of creating a second alert system |
| Current SMTP uses STARTTLS/password login; Graph uses tenant client credentials | Add implicit TLS and OAuth; personal Outlook.com is not covered by the current Graph flow |
| Main-run FOMC refresh adds dates before Gold and retains the local file on fetch/parse/write failure | CLI `run` now enables it by default, with an explicit opt-out and existing additive/fallback behavior |
| Standalone Gold/reconciliation commands select SQLite/Spark directly; the main run uses `WarehouseFactory` | Adapt shared maintenance services to the configured authoritative backend, including PostgreSQL; do not shell-wrap the current CLI and assume parity |
| [Spec 013](../spec013/README.md) and [its handoff](../../docs/handoffs/dagster_orchestration.md) specify the writer safety work | Reuse those contracts; the optional runtime is installed separately from core |

For this initiative, this branch and Spec 016 are the pickup point. The user's
confirmed job scope supersedes the earlier ECB-only draft. The runtime's T1
dependency checks are complete; shared execution and writer exclusion remain.

## Proposed defaults for review

1. Keep Dagster optional and the CLI usable without it. The confirmed initial
   scope includes the main Bronze-to-Gold run, ECB polling/health, warehouse
   health, Gold-only rebuild and metadata reconciliation. Defer granular
   per-table orchestration. This expands Spec 013's ECB-only first release.
2. Preserve today's network behavior when new settings are absent (`mode: env`).
   Offer `auto` for startup selection, `direct` and `proxy` for strict operation.
   Automatic mode defaults to proxy first when a proxy is configured, then direct.
3. Select per destination origin, once per invocation. Honor explicit bypass
   destinations. Freeze the plan; a connection failure later in the run uses
   ordinary retries on that route and is reconsidered at the next run's startup.
4. Preserve legacy email YAML. Add named profiles so users can choose generic
   SMTP, personal Outlook.com, or Microsoft 365 without changing code. One
   provider handles each alert; automatic provider failover is outside this scope.
5. Include HTTP CONNECT support for SMTP where the proxy permits the mail port.
   Otherwise the operator can use Graph over HTTPS or an accessible SMTP relay.
   An HTTP proxy setting alone does not configure Python's SMTP connection.
6. Enable FOMC refresh before Gold in the full Dagster job and CLI `run`, for
   both routine incremental runs and forced full extraction. Add an explicit
   opt-out and retain local-calendar fallback. This requested default change
   is an exception to otherwise preserving CLI behavior.

## Commands and verification

Existing offline characterization commands (use the project's environment):

```bash
.venv/bin/python -m pytest tests/test_config.py tests/test_sources_base.py \
  tests/test_pipeline.py tests/test_alerting.py tests/test_ecb_alerting.py \
  tests/test_cli_ecb_assets_default.py
.venv/bin/python scripts/check_doc_staleness.py
git diff --check
```

Module specs define additional commands and acceptance tests for the remaining
work. Email diagnostics are available; the general network diagnostic command
and Dagster runtime remain unimplemented.

### Draft verification record

On 2026-11-04, the existing characterization command above passed **130 tests**
in the repository's `.venv`. The documentation staleness check passed. Local
links and Markdown fence/whitespace structure in the new spec files passed;
the README additions also resolve. A broader README link scan identified two
pre-existing missing Power BI handoff targets; this change does not alter them.
`git diff --check` passed. At that point, these results characterized existing
code and validated documentation only.

After the scope/FOMC update, the focused FOMC, reconciliation, pipeline and CLI
characterization command in the Dagster module passed **98 tests**. Spec links,
Markdown structure, the documentation staleness check and `git diff --check`
also passed. At that point, the refresh-on default and additional jobs remained
specified behavior awaiting implementation.

The first FRED-network slice adds managed per-run startup route selection while
leaving `env` as the default. The focused pipeline/config/alert/CLI suite passed
144 tests; the final network-specific suite passed 29 tests, including local
proxy/origin and CLI coverage of the routing paths.
The full suite reported 1,554 passed, one skipped, and four failures. The same
four failures reproduce against a clean archive of the pre-slice branch: three
schema-parity tests search for an outdated Spark-writer text pattern, and the
Power BI catalog test reports the existing missing `mmkt_rate_snapshot` entry.
The full run preceded the last focused test additions; no full-suite pass is
claimed. Managed routing for other consumers and preflight-failure alerting
remained open after that slice.

The next slice implements per-origin managed routing for all built-in
main-pipeline source clients and FOMC refresh, and makes FOMC refresh the CLI
`run` default with `--no-refresh-fomc-calendar` as an opt-out. Focused routing,
calendar, pipeline and CLI tests passed (250 tests before the final additional
coverage). The complete suite then reported **1,592 passed, one skipped, and
the same four baseline failures** documented above. After simplifying the
internal run-session lifecycle, 142 affected tests passed. Tests include mixed
actual local direct/proxy connections and a managed FOMC refresh with injected
Fed content; no live Fed fetch or production data operation was performed.

The ECB polling slice extends the selected route to the main-run ECB hook and
standalone poll, with a separate same-origin session boundary for MID links.
The affected networking, ECB, CLI and pipeline tests passed, including local
direct/proxy connections, signature-failure quarantine and ECB preflight
failure isolation. No live ECB request or production data write was performed.
The authoritative writer lock remains planned and was not added by this slice.
The full project-environment suite again reported the same four baseline
failures described above, with no new failures.

The email additions were tested with local SMTP/HTTP CONNECT servers, local
STARTTLS and implicit TLS with certificate verification, fake Graph calls and
an injected identity endpoint with MSAL 1.39.0. Tests also cover private cache
locking, account binding, route reuse, and preview/unknown delivery outcomes.
No live mailbox was authorized or sent mail; provider acceptance and actual
recipient receipt remain deployment checks. Graph HTML/text parity with BCC is open.

The Graph MIME slice verifies both rendered alternatives and binary attachments
by decoding submitted MIME for app-only and delegated authentication. Messages
with BCC retain structured Graph recipients and an HTML-only body. Failure tests
cover accepted, rejected, and ambiguous outcomes without replay. These offline
tests do not verify provider-side delivery or recipient privacy.

## Conventions and boundaries

Follow the existing Python 3.10+ core, dataclasses, YAML validation, injectable
I/O, pytest and Ruff conventions. Use Python 3.12 for the optional Dagster
runtime as proposed in Spec 013. For example, new value objects follow this
existing style (illustrative interface, not implemented code):

```python
@dataclass(frozen=True)
class RouteDecision:
    target_id: str
    route: str
    reason: str
```

- Always: characterize old behavior, validate new settings, redact credentials,
  retain TLS validation, close owned resources, and use existing tests/runbooks
  as the compatibility authority.
- Review before implementing: this scope, dependency choices, defaults and
  provider authentication design. Record exact dependency pins after resolution.
- Review separately before activating: production host settings, scheduler
  ownership, chosen sender/recipients, and real delivery/cutover evidence.
- Never: edit vendored guidance, commit secrets/token caches, silently change
  warehouse authority, infer success from a swallowed Gold failure, or activate
  a second writer/scheduler while the old one is active.

## Review decisions

| Decision | Proposal | Impact if changed |
| --- | --- | --- |
| First Dagster scope | **Confirmed: main, ECB polling/health, warehouse health, Gold-only and metadata reconciliation jobs; per-table assets deferred** | Expands Spec 013's ECB-only first release |
| Full-run FOMC refresh | **Confirmed: enabled by default** in Dagster and CLI `run`, with an explicit opt-out | Adds a Fed-page request and possible local calendar additions before Gold; retain existing failure fallback |
| Network rollout | Opt-in `auto`, existing `env` default | Defaulting all installs to `auto` adds startup traffic and changes routing immediately |
| Network preference | Proxy first, then direct; strict modes available | Direct-first is selectable for networks where direct access is usual |
| Mid-run network changes | Fail/retry on selected route; reselect next run | Transparent route switching needs a separate recovery and replay contract |
| Personal Outlook.com | Delegated Graph recommended; SMTP OAuth also supported | Both need one-time interactive consent and later reauthorization if consent expires |
| SMTP proxy scope | HTTP CONNECT through an `http://` proxy | SOCKS, PAC/WPAD and integrated proxy authentication remain deferred |

After review, reconcile Spec 013's scope and create implementation slices with
explicit acceptance IDs. No implementation or deployment is approved by this
document's existence.

# Spec 016: Dagster, configurable email, and outbound network selection

Status: **Draft for user review. Specification only; implementation has not started.**

Date: 2026-11-04

Review update, 2026-11-04: the user confirmed **main pipeline and ECB jobs** as
the initial Dagster scope, with detailed per-table assets deferred. This confirms
that scope decision only; the remaining specification is still under review.

Branch: `feature/dagster-alerts-network-spec`, created from the locally available
`origin/main` at `8f88ff7`. No remote fetch or push was performed.

## Objective

Let operators run the pipeline through Dagster, choose an alert email provider,
and run behind a proxy or with direct internet access. In automatic mode, test
the applicable connections at startup, choose the working routes, and reuse
those choices throughout that pipeline invocation. Avoid trying both routes on
every source request.

The immediate deliverable is this reviewable specification. Implementation
starts after the user reviews the scope and proposed defaults below.

## Capability map

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| outbound-network | Validated settings, startup probes, immutable run route plan, configured HTTP sessions and SMTP connections | Existing configuration and injectable transports |
| alert-email | Provider profiles, SMTP TLS/auth options, personal Outlook.com OAuth, existing Microsoft 365 Graph, diagnostics | outbound-network |
| dagster-integration | Optional runtime, main-pipeline and ECB jobs, truthful outcomes, shared alerts, stopped schedules and cutover | outbound-network, alert-email, existing Spec 013 ECB execution/lock contracts |

Proposed build order: outbound-network → alert-email → dagster-integration.
Review these boundaries together with the module specifications:

- [Outbound networking](SPEC-outbound-network.md)
- [Email delivery](SPEC-alert-email.md)
- [Dagster integration](SPEC-dagster-integration.md)

This map and all three module specs are proposals, not approved decisions or
an implementation task checklist.

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
| [Spec 013](../spec013/README.md) and [its handoff](../../docs/handoffs/dagster_orchestration.md) are draft-only | Reuse the detailed ECB safety and operational contracts; no Dagster runtime is assumed installed |

For this initiative, this branch and Spec 016 are the review/pickup point.
The old draft's instruction to continue on `spec012-dagster-orchestration`
applies to that older workstream. Do not switch branches or execute its T1
merely because the older handoff says to. Do not overwrite its plan/checklist
or mark its gates complete. Reconcile those documents when this spec is approved.

## Proposed defaults for review

1. Keep Dagster optional and the CLI usable without it. Initially expose the
   main Bronze-to-Gold run and ECB polling as coarse jobs; defer granular
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

## Commands and verification

Existing offline characterization commands (use the project's environment):

```bash
.venv/bin/python -m pytest tests/test_config.py tests/test_sources_base.py \
  tests/test_pipeline.py tests/test_alerting.py tests/test_ecb_alerting.py \
  tests/test_cli_ecb_assets_default.py
.venv/bin/python scripts/check_doc_staleness.py
git diff --check
```

Module specs define additional commands to implement and their acceptance
tests. Proposed commands, files and dependency extras do not exist yet.
No current command should be advertised as supporting the new settings.

### Draft verification record

On 2026-11-04, the existing characterization command above passed **130 tests**
in the repository's `.venv`. The documentation staleness check passed. Local
links and Markdown fence/whitespace structure in the new spec files passed;
the README additions also resolve. A broader README link scan identified two
pre-existing missing Power BI handoff targets; this change does not alter them.
`git diff --check` passed. These results characterize the existing code and
validate documentation only; the new capabilities have not been implemented
or runtime-tested.

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
| First Dagster scope | **Confirmed by user: main pipeline plus ECB jobs; per-table assets deferred** | Expands Spec 013's ECB-only first release |
| Network rollout | Opt-in `auto`, existing `env` default | Defaulting all installs to `auto` adds startup traffic and changes routing immediately |
| Network preference | Proxy first, then direct; strict modes available | Direct-first is selectable for networks where direct access is usual |
| Mid-run network changes | Fail/retry on selected route; reselect next run | Transparent route switching needs a separate recovery and replay contract |
| Personal Outlook.com | Delegated Graph recommended; SMTP OAuth also supported | Both need one-time interactive consent and later reauthorization if consent expires |
| SMTP proxy scope | HTTP CONNECT through an `http://` proxy | SOCKS, PAC/WPAD and integrated proxy authentication remain deferred |

After review, reconcile Spec 013's scope and create implementation slices with
explicit acceptance IDs. No implementation or deployment is approved by this
document's existence.

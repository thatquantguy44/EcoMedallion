# Module: alert-email

Status: Core email additions implemented; Graph text/HTML parity with BCC and deployment verification remain open. Parent: [Spec 016](README.md).
Depends on: [outbound-network](SPEC-outbound-network.md).

Implemented so far: legacy or named profile configuration, per-verdict sender
and transport selection for run and ECB alerts, validated SMTP STARTTLS,
implicit TLS or unauthenticated relay, SMTP partial-refusal reporting and
Graph attachments, delegated personal Outlook OAuth, managed Graph/SMTP
networking, structured delivery outcomes and diagnostics. Graph submits
multipart plain-text/HTML messages when there are no BCC recipients; messages
with BCC retain structured recipients and an HTML-only body. Graph BCC
text/HTML parity, real mailbox acceptance/receipt evidence, and Dagster
integration remain open.

Microsoft's [sendMail API](https://learn.microsoft.com/en-us/graph/api/user-sendmail?view=graph-rest-1.0)
accepts either JSON or a base64-encoded MIME body. The implementation retains
JSON's explicit `bccRecipients` rather than assuming raw MIME BCC delivery and
privacy; this leaves multipart BCC support unverified. A provider acceptance
response or an offline MIME parse alone cannot prove actual recipient privacy.

## Objective and existing contract

Make delivery selectable through validated configuration while preserving the
existing stage verdict, rendering, policy, environment gates, recipients and
ECB health assessment. Keep email delivery failures nonfatal to data runs.
Keep `console` and `file` for development and current webhook delivery independent.

Existing `outlook_smtp` and `microsoft_graph` configurations remain valid.
Do not describe the legacy `outlook_smtp` password-login alias as personal
Outlook.com OAuth support. The existing Graph implementation is a tenant
client-credentials transport, not a delegated personal-account transport.

## Provider and authentication matrix

| Profile / transport | TLS and authentication | Intended use |
| --- | --- | --- |
| `smtp` | Explicit `starttls`, `implicit_tls`, or `none`; `none`, `password`, or `oauth2` auth | Internal relay or SMTP providers such as Brevo, SES and other provider-approved relays |
| `outlook_smtp` (legacy alias) | Existing Microsoft 365 host/settings; retain legacy password behavior | Existing deployments only; availability depends on tenant/mailbox policy |
| `outlook_com_smtp` (new preset) | `smtp-mail.outlook.com:587`, required STARTTLS and delegated XOAUTH2 | Personal Outlook.com / Hotmail / Live sender |
| `microsoft_graph` | HTTPS; `client_credentials` (legacy default) or `delegated` | Organizational app-only sender, or signed-in personal/work sender |
| `console`, `file` | No mailbox connection | Preview and local diagnostics |

Microsoft documents personal Outlook.com SMTP as port 587 with STARTTLS and
OAuth2. Use that preset rather than promising basic password authentication.
[Outlook.com settings](https://support.microsoft.com/en-us/outlook/pop-imap-and-smtp-settings-for-outlook-com)
and [SMTP OAuth authentication](https://learn.microsoft.com/en-us/exchange/client-developer/legacy-protocols/how-to-authenticate-an-imap-pop-smtp-application-by-using-oauth).

For personal accounts, recommend delegated Graph over HTTPS, particularly when
outbound SMTP is blocked. Graph sendMail supports delegated `Mail.Send` for
personal accounts. Use `/me/sendMail` for delegated delivery; retain
`/users/{sender}/sendMail` for organizational app-only delivery. A Graph 202
means accepted for processing; receipt still needs a mailbox check.
[Graph sendMail contract](https://learn.microsoft.com/en-us/graph/api/user-sendmail?view=graph-rest-1.0).

Generic SMTP presets supply connection defaults, not guarantees about provider
account permissions. Do not add separate custom APIs for every SMTP vendor.
No user credentials are sent without configured TLS, except an explicitly
unauthenticated `tls: none` relay. Reject password/OAuth plus cleartext SMTP.

## Configuration and migration

Continue using `config/alerting.yml`, its existing path resolution and strict
validation. Accept either the legacy singular `transport` or the new
`profiles`/`default_profile` form; reject a file that supplies both. A profile
owns its sender and transport. Existing routes gain an optional `profile` key;
routes without it use `default_profile`. Preserve existing root sender fields
for legacy configs, and reject ambiguous sender declarations in profile mode.

Example profile-mode YAML:

```yaml
enabled: true
policy: failure
subject_prefix: "[FRED]"
environments: [prod]
default_profile: relay
profiles:
  relay:
    from_address: pipeline@example.com
    from_name: FRED pipeline
    transport:
      kind: smtp
      host: smtp.example.com
      port: 587
      tls: starttls
      auth: password
      username: pipeline@example.com
      password_env: FRED_SMTP_PASSWORD
      timeout_seconds: 30
      network:
        mode: direct
  outlook-personal:
    from_address: operator@outlook.com
    from_name: FRED pipeline
    transport:
      kind: microsoft_graph
      auth: delegated
      tenant_id: consumers
      client_id: registered-public-client-id
      token_cache_path_env: FRED_OUTLOOK_TOKEN_CACHE
      timeout_seconds: 30
routes:
  failure:
    profile: outlook-personal
    to: [operator@example.com]
  partial:
    to: [data-oncall@example.com]
```

Graph HTTP calls inherit the pipeline's network plan. SMTP's optional
`network` block uses the protocol-specific settings in the network spec.
Standalone alert diagnostics resolve the same pipeline config before testing.
An omitted SMTP network block means direct, matching current SMTP behavior.

For legacy SMTP, map `use_starttls: true` to `tls: starttls`, false to `none`,
and username/password fields to password auth. Reject simultaneous `tls` and
`use_starttls`, or conflicting auth fields. Legacy authenticated cleartext
configs require an explicit migration error and runbook guidance rather than
silently sending credentials. Mark that as the deliberate security exception
to otherwise compatible parsing. All current safe configurations remain valid.

Reject unknown profile references, missing secret references, malformed sender/recipient
addresses, incompatible auth/TLS choices, raw secret fields and `${...}`
interpolation as the existing alert loader does. Validate settings at load
time; resolve secret values only when needed and report an unset value as a
delivery/readiness failure. Diagnostics must not print them.
When disabled, do not bootstrap OAuth, acquire tokens or probe a mail service.

Preserve text/HTML, run JSON attachments and private BCC recipients. Graph
supports attachments in both implemented formats; the text alternative remains
open when BCC requires the structured JSON format.
Send only from the authenticated personal mailbox; delegated send-as for
other mailboxes and SMTP provider failover are deferred.

## OAuth lifecycle

The optional `email-oauth` dependency group uses MSAL `>=1.39,<2`; tests used
1.39.0. Basic SMTP, console/file, existing app-only Graph, and core CLI import
work without it.

Provide an explicit one-time interactive bootstrap command for delegated
profiles, using authorization code + PKCE and a registered loopback redirect.
Use `consumers` for personal Microsoft accounts and a configured tenant for
organizational accounts. Bind the profile to the selected account identity.
Do not launch a browser or wait for interactive login inside a scheduled run.
[Microsoft authorization-code guidance](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow).

Persist the delegated token cache outside the checkout at the path named by
`token_cache_path_env`. POSIX local/daemon deployments require an owned private
directory and 0600 cache, locking across writers, atomic replacement and no
token content in backups/artifacts by default. A cache contains credentials;
fail explicitly on an unsafe path or unavailable secure storage. Other host
platforms need an equivalent credential-storage proof before support is claimed.
MSAL does not provide persistent serialization/locking automatically.
[MSAL token-cache guidance](https://learn.microsoft.com/en-us/entra/msal/python/advanced/msal-python-token-cache-serialization).

Graph delegated profiles request `Mail.Send` and offline refresh capability;
SMTP delegated profiles use the documented Outlook `SMTP.Send` scope and
XOAUTH2 framing. Do not interchange tokens for these audiences. Use the
run-selected HTTP transport for discovery, token refresh and Graph requests;
an MSAL default HTTP client must not silently use independent proxy discovery.
Token refresh is performed when sending, with shared-cache locking and expiry
handling. Revoked consent or `interaction_required` produces a sanitized
reauthorization instruction and failed-delivery result; never a login prompt.

## Delivery ownership and diagnostics

Preserve `send_run_alert` and ECB delivery as the business-outcome owners.
Return a structured delivery result containing profile id, correlation id,
`disabled`, `suppressed`, `previewed`, `accepted`, `failed`, or `unknown`, and
sanitized error category. Console/file are `previewed`, never proof of mail
delivery. SMTP partial-recipient refusal is visible, not reported as complete
acceptance. Do not log Graph error bodies, tokens, MIME attachments or proxy URLs.

One profile handles one alert. Do not automatically retry on another provider
or route after ambiguous SMTP DATA/Graph send completion. Stable message/correlation
ids aid investigation but do not guarantee exactly-once delivery. Dagster's
fallback sensor must use these outcomes and the ownership contract in its spec.

Available commands:

```bash
python -m fred_pipeline alerts check --profile relay --env dev \
  --config config/config.yaml --alerting-file config/alerting.yml
python -m fred_pipeline alerts auth --profile outlook-personal \
  --config config/config.yaml --alerting-file config/alerting.yml
python -m fred_pipeline alerts test --profile relay --env dev \
  --config config/config.yaml --alerting-file config/alerting.yml \
  --to operator@example.com --send
```

`check` validates config/secrets/cache metadata and probes connections without
sending mail. `auth` explicitly bootstraps consent. `test` previews unless
`--send` is supplied and uses an explicit test recipient, not production routes.
Use exit 0 for success, 1 for delivery/readiness failure, 2 for invalid config.
No mailbox creation, app registration or live test email occurs in this spec work.

## Structure, tests and acceptance

Extend `governance/alerting_config.py`, `email_transport.py`, `alerting.py` and
ECB helpers; isolate OAuth/cache handling in a small lazily imported module.
Add provider examples under `resources/` alongside the current SMTP/Graph
examples, and update the alerting runbook after implementation. Match existing
dataclass/protocol and injectable I/O conventions.

| ID | Acceptance evidence |
| --- | --- |
| MAIL-01 | Existing safe YAML/test fixtures retain policies, recipients and behavior; new profile validation and explicit migration errors are covered |
| MAIL-02 | Fake/local SMTP proves STARTTLS, implicit TLS, unauthenticated relay, password and XOAUTH2 paths, BCC privacy and partial-recipient failures |
| MAIL-03 | Personal delegated Graph uses the correct account/endpoint/scope; app-only Graph remains compatible; both retain attachments |
| MAIL-04 | Bootstrap, refresh, revoked consent, concurrent cache writes and missing optional dependencies are tested without live credentials |
| MAIL-05 | All token/Graph/SMTP connections follow the selected run route, TLS remains verified, and errors cannot leak credentials |
| MAIL-06 | Terminal delivery outcomes survive pipeline/Dagster failure handling without changing committed business data; ambiguous sends are not automatically replayed |
| MAIL-07 | Diagnostics default to no send; the selected production profile has separate recorded provider acceptance and real recipient receipt evidence before activation |

Run `python -m pytest tests/test_alerting.py tests/test_ecb_alerting.py` plus
new profile/OAuth/transport tests. Use local SMTP fixtures for TLS behavior and
fake Graph/MSAL clients for OAuth failures. Verify core-only installation in
an environment without OAuth dependencies. Real account/app permissions and
mailbox receipt remain deployment gates, not claims inferred from unit tests.

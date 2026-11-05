# Module: outbound-network

Status: Draft for review. Parent: [Spec 016](README.md).

## Objective and scope

In managed modes, resolve outbound paths once at the beginning of every
network-enabled pipeline invocation, including CLI and Dagster. Support direct
access, explicit proxies,
environment-derived proxies, bypass destinations and automatic fallback in
either direction. Keep API credentials, retry policies and business requests
independent from route selection.

Cover built-in HTTP sources, ECB MID and archive downloads, FOMC Requests
refresh, release-calendar calls, webhooks, Microsoft authentication and Graph,
and SMTP using the protocol-specific connection adapter below. PostgreSQL
connections, Spark/storage SDK traffic, Selenium/browser traffic, and ad hoc
probe scripts are outside this application transport boundary. Selenium must
not silently bypass the selected Requests route: managed-mode FOMC refresh
uses Requests or fails with an explicit unsupported-backend message.

## Configuration contract

Add a validated `network` value to `PipelineConfig` and the recognized YAML
keys. The example below is proposed content for `config/config.example.yaml`:

```yaml
default:
  network:
    mode: auto                  # env | direct | proxy | auto
    preference: proxy_first     # proxy_first | direct_first (auto only)
    inherit_proxy_env: true
    http_proxy_env: PIPELINE_HTTP_PROXY
    https_proxy_env: PIPELINE_HTTPS_PROXY
    no_proxy: [localhost, 127.0.0.1, "::1", ".internal.example"]
    ca_bundle_env: PIPELINE_CA_BUNDLE
    probe:
      connect_timeout_seconds: 3
      read_timeout_seconds: 3
      max_parallel_targets: 4
    origins:
      "https://api.stlouisfed.org:443":
        mode: auto
      "https://internal.example:443":
        mode: direct
```

Proxy values are URLs obtained from named environment variables, including
credentials when necessary; never put credentials in tracked YAML. CA values
are paths to a trusted certificate bundle. Core HTTP proxy support includes
`http://` and `https://` proxy URLs; SOCKS requires a future optional feature.
SMTP CONNECT initially supports `http://` proxy URLs only.

| Mode | Startup and request behavior |
| --- | --- |
| `env` (compatibility default) | Preserve current Requests/SMTP behavior; no added probes or automatic route selection |
| `direct` | Probe and use direct only, ignoring ambient proxies |
| `proxy` | Probe and use the configured/inherited proxy only, except explicit bypass targets |
| `auto` | Probe in configured preference order, then freeze the first usable route |

Use a managed mode to activate this module's startup selection guarantees.
The new proxy/bypass settings are applied in managed modes; reject a misleading
combination of `env` mode and explicit managed proxy/origin overrides.

Precedence is explicit Python overrides > application environment overrides >
environment-specific YAML > default YAML > built-ins. Merge the new network
mapping by field, including origin overrides; do not change unrelated existing
YAML merge behavior. Unknown nested keys, invalid enum values, invalid URLs,
nonpositive timeouts and unsupported proxy schemes raise configuration errors.

Application overrides: `FRED_NETWORK_MODE`, `FRED_NETWORK_PREFERENCE`,
`FRED_HTTP_PROXY`, `FRED_HTTPS_PROXY`, `FRED_NO_PROXY`, `FRED_CA_BUNDLE`.
The proxy variables contain URL values; `FRED_NO_PROXY` is comma-separated.
Explicit application proxy values override named references. With inheritance
enabled, named references fall back to standard proxy variables when unset.
An explicitly configured missing reference without a standard fallback is an
error in `proxy` mode and an unavailable candidate in `auto` mode.

Standard inheritance recognizes lowercase before uppercase `http_proxy`,
`https_proxy`, `all_proxy`, `no_proxy`, with the usual CGI restriction on
uppercase `HTTP_PROXY`. Scheme-specific proxies precede `ALL_PROXY`.
Snapshot them once, independently of later environment changes. Preserve
Requests' current environment behavior completely in legacy `env` mode.
Managed modes must not discover additional OS proxies or PAC settings.

For bypasses, explicit `FRED_NO_PROXY` replaces the YAML list; inherited
standard `NO_PROXY`/`no_proxy` is unioned with that list when inheritance is on.
Match exact DNS hosts, suffixes with label boundaries, IP literals and optional
ports; `*` means all destinations. Do not perform DNS lookups for bypass
matching. CIDR syntax is deferred and rejected rather than misinterpreted.
Normalize default ports and IPv6 consistently. A bypass match is direct-only,
including in proxy mode, and must never fall back to a proxy. Origin-specific
mode overrides precede global mode, but cannot override a bypass match.

## Startup selection and lifecycle

1. Parse configuration and determine the selected workload and all known
   destinations before source downloads, worker submission or business-data
   writes. Existing metadata synchronization must move after preflight for
   managed runs; read-only planning may precede it. Dry runs that fetch real
   sources still preflight. Offline replay, injected test clients and jobs
   with no outbound work do not acquire live networking implicitly.
2. Build a target registry from source factories and selected manifest series,
   resolved base URLs, known download/redirect hosts, enabled calendar refresh,
   and eligible alert/webhook profiles. Probe each distinct origin/protocol
   once, not each series. Source origins are required; notification and
   optional enrichment origins are advisory, preserving existing failure policy.
3. Resolve bypasses first. In strict `direct`/`proxy`, probe only that candidate.
   In `auto`, try the preferred available candidate and try the other at most
   once on an eligible connectivity failure. Skip an absent proxy candidate.
   If both work, choose the preferred one without probing the second.
4. Store an immutable run-owned `NetworkPlan` containing per-target decisions,
   sanitized probe outcomes and resolved private transport settings. Share
   decisions across threads, but give each worker its own HTTP session. Do not
   share a mutable Requests session as a new concurrency assumption.
5. Construct clients from that plan. Close owned sessions and clear their
   cache after the invocation, including error paths. Reusing `FredPipeline`
   for another run must preflight again. Caller-injected clients remain owned
   by their caller and opt into managed networking explicitly.
6. Hold the plan through terminal alerts and the main CLI's ECB hook, then
   discard it. A separately invoked ECB command gets a new plan. Dagster runs
   initialize their plan inside the executing worker, once per job invocation;
   daemon startup and definition import must never probe or cache routes.

For ECB live writers, acquire the authoritative writer lock before source
preflight. A contender must retain Spec 013's prompt, source-call-free busy
behavior. Read-only health jobs have no source preflight unless they send an
enabled notification. Include an invoked ECB hook's known destinations in the
parent run plan so it cannot add a second fallback loop later in the invocation.

Automatic fallback applies only to connectivity failures: connection refused,
unreachable route, DNS failure and connection/read timeout during a safe probe.
A received origin HTTP response proves reachability even if it is 401, 403,
404, 405, 429 or 5xx; it does not prove source availability or authorization.
Do not interpret these statuses as a reason to bypass the configured route.
TLS/certificate failures and proxy authentication/policy failures, including
407 and rejected CONNECT, are terminal configuration/security diagnostics;
they do not trigger an alternate path. Never disable certificate verification.

HTTP probes use a source-approved HEAD or small streaming GET, preserve needed
User-Agent headers, send no API keys, avoid full artifact downloads and close
responses. Use only registered service destinations, not a public connectivity
test site. Do not send mail, POST a webhook, acquire a token, or ingest source
data as a probe. Distinguish a verified origin response from a proxy error page;
CONNECT failure and identifiable proxy-generated errors do not prove origin
reachability. Record uncertainty for plain HTTP and test actual source behavior.

Probe attempts have separate connect/read timeouts, no source retry/backoff
loop, a maximum of two route candidates and bounded target concurrency.
Requests timeouts are not a total DNS/wall-clock guarantee; document observed
startup duration and DNS limits on the deployment host instead of promising a
hard deadline that Requests cannot enforce.

All required destinations must have a route before source writes begin.
Otherwise record a failed preflight stage and terminate the workload; attempt
the configured alert through an available advisory route. Advisory failures
warn and disable that delivery/refresh attempt for the invocation. Notification
failure never changes an otherwise successful data outcome.

## Request and SMTP connection contracts

In managed modes, configure Requests sessions with `trust_env=False` and
explicit per-origin proxy decisions. An empty `proxies` dictionary alone is
insufficient to force direct access when environment inheritance remains on.
Snapshot trusted CA settings explicitly, including existing
`REQUESTS_CA_BUNDLE`/`CURL_CA_BUNDLE` compatibility, because `trust_env=False`
also changes those defaults. Ambient `.netrc` authentication must not be
introduced; characterize and document users relying on it before migration.
[Requests proxy documentation](https://requests.readthedocs.io/en/latest/user/advanced/#proxies)
and [session implementation](https://requests.readthedocs.io/en/latest/_modules/requests/sessions/)
support these integration constraints.

Redirects to registered origins use their already selected route. Undeclared
cross-origin redirects/dynamic download destinations fail with a diagnostic
in managed mode; register known source/CDN targets at startup. Do not lazily
probe new hosts or follow a redirect around strict routing. Strip sensitive
headers across origins. Legacy `env` mode retains existing redirect behavior.

For SMTP, add a connection factory with `direct` and `http_connect` candidates.
SMTP profiles select these through their own `network` block using mode,
preference, `proxy_url_env`, bypass and CA settings; no port-587 assumptions
are derived from `HTTPS_PROXY`. Preflight connects to the SMTP host/port,
reads its greeting, EHLOs, establishes required TLS and quits without AUTH,
MAIL FROM or DATA. STARTTLS uses the destination hostname for certificate
validation through the tunnel. Implicit TLS is established before the greeting.
Actual sending opens a new connection using the stored route decision.
Reject unsupported proxy schemes before the job starts. A proxy that denies
CONNECT to the mail port is a failed route; advise Graph/relay rather than
claiming all corporate proxies support SMTP.

Neither HTTP nor SMTP does per-request route fallback after startup. Preserve
existing source retries on the selected route. Do not replay Graph sendMail,
webhook POSTs or SMTP DATA after an uncertain outcome.

## Structure, commands, and acceptance

Proposed files: `network/config.py`, `network/preflight.py`,
`network/http.py`, `network/smtp.py` under `src/fred_pipeline/`; inject through
existing factories/entry points rather than changing every source's retry loop.
Tests belong in focused `tests/test_network_*.py` files. Use frozen dataclasses,
typed errors and injectable probe/clock/session factories; keep secrets out of
repr, logs, exception chains and Dagster metadata.

Proposed diagnostic command (not implemented):

```bash
python -m fred_pipeline network check --config config/config.yaml --env dev \
  --manifest-dir manifests --json
```

It uses the same registry/planner, emits sanitized route/readiness results,
sends no notifications and writes no business data. Exit 0 when all required
targets are reachable; exit 1 for required-target failure; exit 2 for invalid
configuration. The output distinguishes advisory failures from readiness.

| ID | Acceptance evidence |
| --- | --- |
| NET-01 | Precedence, strict validation, uppercase/lowercase/CGI environment rules, bypass boundaries, IPv6 and CA settings pass fixture tests |
| NET-02 | Proxy-only and direct-only environments succeed in `auto`; both preferences work; strict modes never attempt the other path |
| NET-03 | TLS/407/CONNECT policy errors never cause direct fallback; source HTTP errors never cause route switching |
| NET-04 | Thousands of calls and concurrent workers use at most one successful selection per origin; at most two probes per auto target; a second invocation probes anew |
| NET-05 | Required preflight failure prevents downloads/business writes; advisory mail failure preserves data-run semantics and is visible |
| NET-06 | All listed application HTTP paths and SMTP direct/CONNECT use the same invocation plan; injected fakes and offline runs remain offline |
| NET-07 | Redirect hosts are covered; unregistered hosts fail explicitly; ambient proxy changes do not alter a selected managed route; secrets are redacted |
| NET-08 | Local origin/proxy/SMTP fixtures prove actual routing, TLS hostname/CA validation, cleanup, and no duplicate POST/DATA during failures |

Run new offline tests with `python -m pytest tests/test_network_*.py` and the
existing characterization command in the parent spec. Validate with local
HTTP/CONNECT servers as well as mocks; use real corporate-network checks only
on the configured deployment host. Record skips and host limitations.

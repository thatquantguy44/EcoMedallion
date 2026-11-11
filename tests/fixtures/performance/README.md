# Performance reporting fixtures

These small, synthetic cases are checked in for deterministic replay. They
contain no captured credentials, account details, live responses or warehouse
data. `tests/performance/conftest.py` loads a fresh copy for each request.

| File | Contract exercised |
| --- | --- |
| `success.json` | Full stage timeline, concurrent worker/window durations, source percentiles/retries, baseline history, skipped series and an untrusted display label |
| `partial.json` | Optional calendar failure, missing baseline and incomplete telemetry |
| `early_failure.json` | Planning failure, unreached stages and an unfinished operation |
| `http_retry.json` | FRED transport: HTTP 429 with Retry-After followed by success |
| `span_events.jsonl` | Nested start/finish events for idempotent journal replay |

The JSON report envelopes match the alert attachment structure. JSONL contains
multiple events for each span, while persisted audit rows retain its latest
outcome. Runtime journals may also append `span_updated` amendments when a
completed stage is subsequently marked warned. None of these files should be
regenerated from the implementation under test; their values are independently
chosen examples.

The request statistics in the successful/partial cases correspond to samples
`[1, 1, 1, 3, 3]` seconds: five requests, nine accumulated seconds, p50 one
second and p95 three seconds using the collector's nearest-rank rule.

Run the extended tests with `python -m pytest tests/performance`. Existing
coverage remains in `tests/test_performance*.py`, source-client tests,
`tests/test_alerting.py` and native warehouse tests. PostgreSQL tests create
isolated disposable test databases; no production refresh or email is needed.

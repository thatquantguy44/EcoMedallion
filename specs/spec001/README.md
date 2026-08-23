# Spec 001: Incorporate ECB Data Portal API

Status: proposed build plan
Last verified: 2026-08-22
Primary owner: TBD
Target source key: `ecb`

## 1. Goal

Add the European Central Bank Data Portal API as a first-class pluggable source
in the existing manifest-driven Bronze -> Silver -> Gold pipeline.

The finished implementation should let a manifest entry such as
`source: ecb` flow through the same lifecycle as FRED, BLS, EIA, Treasury,
World Bank, BIS, BEA, Census, SEC, Stooq, Tiingo, and iShares:

1. Validate manifest structure locally with no network.
2. Fetch raw ECB payloads through a rate-limited, retrying `SourceClient`.
3. Archive verbatim payloads in Bronze with source-aware endpoint lineage.
4. Normalize rows into the canonical Silver schema.
5. Merge idempotently on `(source, series_id, observation_date, realtime_start)`.
6. Preserve ECB revision history when requested.
7. Support Bronze replay without constructing a live ECB client.
8. Rebuild existing Gold views from the normalized rows without Gold-layer
   source-specific branching.

## 2. Current Repository Model

The repo is already set up for exactly this kind of addition.

- `README.md` describes the source layer as pluggable and source-agnostic after
  a `SourceClient` normalizes data into the shared row schema.
- `docs/instructions/adding_a_source.md` defines the new-source recipe:
  one client module under `src/fred_pipeline/sources/`, one `SOURCE_FACTORIES`
  registry entry, and one or more manifests with `source: <name>`.
- `src/fred_pipeline/sources/base.py` provides `HTTPSource`, shared retry,
  rate limiting, request headers, error hooks, and the `SourceClient` protocol.
- `src/fred_pipeline/pipeline.py` performs source-aware routing through
  `SOURCE_FACTORIES`, source-specific worker/rate-limit overrides, source-aware
  Bronze lineage, and client-delegated normalization.
- `src/fred_pipeline/data/silver.py` has a separate module-level replay
  normalizer dispatch table. ECB must be added there too, otherwise live
  ingestion can work while Bronze replay falls back to the FRED parser.
- `manifests/manifest.schema.json` already allows arbitrary `source` strings
  and does not need source-specific fields.
- `config/data_licensing.yml` is the source licensing register used by
  governance checks.

## 3. ECB API Facts To Build Against

Use the ECB Data Portal SDMX 2.1 REST API.

Official docs reviewed:

- Data API basics: `https://data.ecb.europa.eu/help/api/data`
- Data examples: `https://data.ecb.europa.eu/help/api/data-examples`
- Content negotiation: `https://data.ecb.europa.eu/help/api/content-negotiation`
- General SDMX web services page:
  `https://data.ecb.europa.eu/help/getting-data-web-services-sdmx-0`

Relevant behavior:

- Base URL: `https://data-api.ecb.europa.eu/service`
- Data endpoint shape: `/data/{flowRef}/{key}`
- `flowRef` may be a simple dataflow id such as `EXR`; ECB docs also describe
  full `agency,flow,version` references.
- Series keys are dot-separated SDMX dimension values in dataflow order.
  Example: `D.USD.EUR.SP00.A` for daily USD/EUR reference exchange rates in
  the `EXR` dataflow.
- Supported query params include `startPeriod`, `endPeriod`, `updatedAfter`,
  `firstNObservations`, `lastNObservations`, `detail`, `includeHistory`, and
  `format`.
- `format=csvdata` returns CSV and is the simplest parser target.
- A live check of
  `/data/EXR/D.USD.EUR.SP00.A?startPeriod=2024-01-01&endPeriod=2024-01-03&format=csvdata`
  returned CSV columns including `KEY`, dimensions, `TIME_PERIOD`, `OBS_VALUE`,
  and many attributes.
- A live check with `includeHistory=true` added `ACTION`, `VALID_FROM`, and
  `VALID_TO`, which can map onto `realtime_start` and `realtime_end`.
- ECB is keyless. No API key should be required.
- No published project-specific rate limit was identified in the official API
  pages. Start conservatively at `60` requests/minute and expose overrides
  through the existing `FRED_SOURCE_RATE_LIMITS` mechanism.

## 4. Design Decisions

### 4.1 Series ID Encoding

Use manifest ids shaped as:

```yaml
series_id: "ECB:<flow_ref>:<key>"
source: ecb
```

Examples:

```yaml
ECB:EXR:D.USD.EUR.SP00.A
ECB:FM:M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA
ECB:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y
```

Rationale:

- It avoids collision with FRED ids and other source ids.
- It keeps source-specific addressing inside `series_id`, matching current
  patterns such as `BIS:<ref_area>` and `USA:NY.GDP.MKTP.CD`.
- It requires no manifest schema changes.
- It records enough information for the client and replay normalizer to route
  the payload without auxiliary config.

Parser contract:

- Accept only `ECB:<flow_ref>:<key>` for initial implementation.
- Split on the first two colons.
- Preserve `flow_ref` and `key` case as supplied except trimming whitespace.
- Raise `ECBAPIError` with a clear message for malformed ids.
- Allow `flow_ref` to be either `EXR` or a full ECB REST flow reference such as
  `ECB,EXR,1.0`.

### 4.2 Payload Format

Fetch `format=csvdata` and store Bronze as a JSON envelope:

```json
{
  "data": "<raw CSV text>",
  "meta": {
    "series_id": "ECB:EXR:D.USD.EUR.SP00.A",
    "flow_ref": "EXR",
    "key": "D.USD.EUR.SP00.A",
    "format": "sdmx-csv",
    "include_history": false
  }
}
```

This follows the BIS pattern: CSV body preserved verbatim inside a JSON payload
that the existing Bronze table can store.

### 4.3 Revision Handling

Implement two modes, controlled by the existing manifest `vintage_enabled` flag.

- `vintage_enabled: false`
  - Request current production data only.
  - Set `realtime_start` and `realtime_end` to empty strings.
  - Merge key behaves as a simple current-value upsert.

- `vintage_enabled: true`
  - Add `includeHistory=true` to the ECB request.
  - Map `VALID_FROM` to `realtime_start`.
  - Map `VALID_TO` to `realtime_end` when present.
  - If `VALID_FROM` is missing, use an empty `realtime_start`.
  - Let `assign_revision_numbers` compute revision numbers uniformly.
  - Preserve `ACTION` in Bronze only; do not add a Silver column.

Open validation item: run a few ECB dataflows beyond `EXR` with
`includeHistory=true` to confirm `VALID_FROM`/`VALID_TO` availability and
whether deleted values are represented with `ACTION=Delete`. If deletes appear,
initial Silver behavior should either skip them or emit missing rows only after
an explicit test confirms downstream semantics.

### 4.4 Incremental Loads

Use the existing `observation_start` planning path:

- Convert `observation_start` to ECB `startPeriod`.
- Convert optional `observation_end` to `endPeriod`.
- Keep `load_type: incremental` as the default.
- For ECB series likely to revise older values, set manifest-level
  `restate_records` or schedule periodic full refreshes, consistent with the
  repo's current revision policy.

Defer `updatedAfter` support to a follow-up. The current pipeline's incremental
contract is observation-window based, and adding update-timestamp watermarks
would require new warehouse state that no other source currently uses.

## 5. Implementation Plan

### Phase 1: ECB Client Module

Add `src/fred_pipeline/sources/ecb.py`.

Core objects:

- `ECBAPIError(SourceError)`
- `ECBClient(HTTPSource)`
- `parse_ecb_series_id(series_id: str) -> tuple[str, str]`
- `normalize_ecb_observations(...) -> list[dict[str, Any]]`
- ECB-specific period/date helpers.

`ECBClient` behavior:

- Default `base_url="https://data-api.ecb.europa.eu/service"`.
- No `_default_query` auth.
- `_request_headers()` should request CSV explicitly:
  `{"Accept": "text/csv"}`.
- `_error_detail()` should return response text, trimmed for readability.
- `observations_endpoint(series_id)` should return `data/{flow_ref}/{key}`.
- `get_observations(...)` should:
  - parse `ECB:<flow_ref>:<key>`;
  - set `format=csvdata`;
  - set `detail=dataonly` by default unless attributes are needed for history;
  - set `startPeriod` and `endPeriod` from pipeline windows;
  - set `includeHistory=true` when `track_vintage`/`realtime_*` kwargs imply
    vintage mode;
  - call `_request(..., as_text=True)`;
  - return the JSON envelope described above.

Important nuance: `SourceClient.get_observations` does not currently receive
`track_vintage` directly; `pipeline._extract` passes `realtime_start` and
`realtime_end` for `vintage_enabled` series. ECB can treat the presence of
those kwargs as a signal to set `includeHistory=true`.

Normalization behavior:

- Parse CSV using `csv.DictReader`, ignoring blank/comment lines.
- Locate fields case-insensitively:
  - `TIME_PERIOD`
  - `OBS_VALUE`
  - `VALID_FROM`
  - `VALID_TO`
  - `ACTION`
- Convert `TIME_PERIOD` to an ISO observation date:
  - `YYYY` -> `YYYY-01-01`
  - `YYYY-S1` / `YYYY-S2` -> `YYYY-01-01` / `YYYY-07-01`
  - `YYYY-Qn` -> quarter start
  - `YYYY-MM` -> month start
  - `YYYY-Www` -> ISO week Monday
  - `YYYY-MM-DD` -> same date
- Parse `OBS_VALUE` with the shared `parse_value`.
- Build rows with exactly `fred_pipeline.transform.SILVER_COLUMNS`.
- Use `_row_hash(series_id, observation_date, realtime_start, raw_value)`.
- Set `source="ecb"`.
- Treat blank/non-numeric values as missing.
- For now, skip rows with unparseable `TIME_PERIOD`.

### Phase 2: Pipeline Registration

Update `src/fred_pipeline/pipeline.py`.

- Import `ECBClient`.
- Add `_make_ecb(config: PipelineConfig) -> SourceClient`.
- Add `"ecb": _make_ecb` to `SOURCE_FACTORIES`.
- Add default rate limit in `_rate_limit_for_source`:
  `"ecb": 60`.
- Add worker default in `_extract_workers_for_source` only if needed. The
  current default is acceptable; no special cap is required for a keyless public
  statistical API beyond the rate limiter.
- Do not add ECB to `SOURCE_KEY_REQUIREMENTS`.

### Phase 3: Replay Dispatch

Update `src/fred_pipeline/data/silver.py`.

- Add an `if source == "ecb":` branch in `_normalize_for_source`.
- Import and call `normalize_ecb_observations(...)`.
- Pass through `track_vintage`, `run_id`, `ingested_at`, and `source`.

This is required for `fred-pipeline replay` to re-derive Silver from archived
ECB Bronze rows without network or a client instance.

### Phase 4: Source Package Export

Update `src/fred_pipeline/sources/__init__.py` if this package exports source
classes for documentation or convenience. Keep exports consistent with the
existing style.

### Phase 5: Manifest

Add a starter manifest: `manifests/ecb_rates.yml`.

Initial entries should be conservative, high-value, and easy to verify:

```yaml
name: ecb_rates
description: >
  European Central Bank Data Portal series (source: ecb). Keyless SDMX 2.1 API.
  series_id is 'ECB:<flow_ref>:<key>'; example EXR key D.USD.EUR.SP00.A.
version: 1

series:
  - series_id: "ECB:EXR:D.USD.EUR.SP00.A"
    title: ECB Reference Exchange Rate - USD per EUR
    category: international
    frequency: d
    units: USD per EUR
    active: true
    source: ecb
    load_type: incremental
    expected_update_frequency: daily
    vintage_enabled: true
    validation_profile: standard
    downstream_use_case: fx_rates
    priority: 2
    min_value: 0
    tags: [fx, ecb, euro-area]
```

Candidate follow-on series after live verification:

- Additional `EXR` currency pairs against EUR.
- `FM` money-market / EURIBOR rates if the series keys are verified.
- `YC` euro area yield curve tenors if the series keys are verified.
- ECB policy and monetary aggregate flows where dimensions and update cadence
  are stable.

Do not activate unverified keys by default. This matches the existing
multi-source convention for newly assembled non-FRED entries.

### Phase 6: Licensing Register

Update `config/data_licensing.yml` with an `ecb` entry.

Initial conservative entry:

- `source: ecb`
- `license_type: open-data`
- `review_status: provisional`
- `redistribution_allowed: true`
- `commercial_use_allowed: true`
- `attribution_required: true`
- `terms_url: https://data.ecb.europa.eu/help`
- Notes: requires primary terms review before external redistribution.

Before relying on this for a commercial or external product, a human should
read the ECB terms/copyright page directly and set `review_status: verified`
with `reviewed_by`.

### Phase 7: Documentation Updates

Update docs after implementation:

- `README.md`
  - source count and source table;
  - data sources list;
  - configuration/API key table, noting ECB is keyless;
  - "Add a series from another source" examples.
- `docs/instructions/adding_a_source.md`
  - include ECB in wired source list and layout;
  - mention ECB as a keyless SDMX CSV source comparable to BIS.
- `docs/instructions/running_multi_source.md`
  - add ECB to key table as no key required;
  - add `ecb_rates.yml` to the activation table;
  - include a one-series dry-run command.
- `docs/deployment/deployment_runbook.md`
  - add ECB egress host: `data-api.ecb.europa.eu`.
- `docs/dictionary/data_dictionary.md`
  - no schema change expected; add ECB lineage note only if the source list is
    enumerated there.

### Phase 8: Tests

Add `tests/test_ecb_client.py`.

Required unit tests:

- `test_parse_ecb_series_id_accepts_flow_and_key`
- `test_parse_ecb_series_id_rejects_bad_shape`
- `test_observations_endpoint_builds_data_endpoint`
- `test_get_observations_requests_csv_and_period_bounds`
- `test_get_observations_sets_include_history_when_vintage_kwargs_present`
- `test_normalize_current_csv_matches_canonical_silver_schema`
- `test_normalize_history_csv_maps_valid_from_to_realtime_fields`
- `test_ecb_period_mapping`
- `test_blank_value_is_missing`
- `test_ecb_rows_pass_dq_and_merge`
- `test_pipeline_routes_ecb_end_to_end`
- `test_replay_routes_ecb_normalizer`
- `test_ecb_client_satisfies_source_protocol`
- `test_ecb_source_is_registered`
- `test_ecb_not_in_source_key_requirements`

Fixture CSV for current mode:

```csv
KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,OBS_STATUS
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956,A
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-03,1.0919,A
```

Fixture CSV for history mode:

```csv
KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE,ACTION,VALID_FROM,VALID_TO
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2024-01-02,1.0956,Replace,2024-01-02T15:57:01.000+01:00,
```

The tests should use the repo's existing fake session fixtures. No test should
perform a network call.

### Phase 9: Optional Live Smoke Test

After unit tests pass, run one live dry run manually. The starter ECB row is now
deliberately active because the EXR series has been verified and cataloged.

```bash
PYTHONPATH=src python -m fred_pipeline validate --manifests manifests
PYTHONPATH=src python -m fred_pipeline run --dry-run --series "ECB:EXR:D.USD.EUR.SP00.A"
```

Then run a local persisted test only if the dry run succeeds:

```bash
PYTHONPATH=src python -m fred_pipeline run \
  --local --db-path /tmp/fred_ecb_smoke.db \
  --series "ECB:EXR:D.USD.EUR.SP00.A" \
  --no-gold
```

Query expected result:

```sql
SELECT source, series_id, count(*) AS rows
FROM silver_fred_observation
WHERE source = 'ecb'
GROUP BY source, series_id;
```

## 6. Acceptance Criteria

Implementation is complete when:

- `fred_pipeline.sources.ecb.ECBClient` implements `SourceClient`.
- `source: ecb` series route through `FredPipeline` with no API key.
- ECB payloads are archived in Bronze as source `ecb`.
- ECB rows normalize into exactly `SILVER_COLUMNS`.
- Current-mode rows merge idempotently in local SQLite.
- History-mode rows populate `realtime_start` from `VALID_FROM`.
- `fred-pipeline replay` reconstructs ECB Silver rows from Bronze.
- `fred-pipeline validate --manifests manifests` passes.
- The ECB test suite passes with no network.
- Existing source tests continue to pass.
- Docs and licensing register mention ECB consistently.

Recommended verification commands:

```bash
PYTHONPATH=src python -m fred_pipeline validate --manifests manifests
python -m pytest tests/test_ecb_client.py
python -m pytest tests/test_sources_base.py tests/test_manifest_schema.py
python -m pytest
```

## 7. Risks And Mitigations

- SDMX dataflow-specific dimension complexity:
  - Mitigation: require explicit `ECB:<flow_ref>:<key>` ids at first, then add a
    discovery helper later.

- Revision/delete semantics may differ across ECB dataflows:
  - Mitigation: preserve raw `ACTION` in Bronze, map only confirmed
    `VALID_FROM`/`VALID_TO` fields into Silver, and add targeted fixtures before
    supporting deletes.

- Very large wildcard queries:
  - Mitigation: starter client should fetch one explicit key per manifest row;
    do not put wildcard keys in active manifests without a specific sizing test.

- No published rate limit:
  - Mitigation: default to `60` rpm, document override via
    `--source-rate-limits ecb=N`, and rely on inherited 429 retry/backoff.

- Replay path omission:
  - Mitigation: include a dedicated replay test and explicit
    `data/silver.py` dispatch entry.

- Licensing uncertainty:
  - Mitigation: add provisional governance entry and require primary terms
    review before external redistribution.

## 8. Follow-Ups After Initial Source Support

- Add an ECB metadata/discovery helper using `/dataflow` and structure
  endpoints to generate manifests from verified dataflows.
- Add `updatedAfter`-based incremental mode if the warehouse gains per-source
  update-timestamp watermarks.
- Add more curated ECB series to `config/series_catalog.yml` for presentation
  semantics once additional ingestion IDs are verified.
- Add cross-source reconciliation pairs where ECB overlaps with FRED/BIS
  market rates or exchange rates.
- Consider a generic SDMX CSV normalizer shared by BIS and ECB if a third SDMX
  source is added.

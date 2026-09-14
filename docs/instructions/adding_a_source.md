# Adding a Data Source

This pipeline was built around FRED, but the FRED-specific surface is small and
isolated. Bronze (verbatim payload), the Silver `MERGE`, data-quality profiles,
audit, and Gold are all **source-agnostic** — they operate on the canonical
silver row schema and never inspect where a row came from. Adding a new source
(BLS, EIA, ECB, ...) means implementing one small contract, not forking the
pipeline.

FRED has been refactored onto the shared transport, and **BLS**, **EIA**,
**ECB**, **US Treasury**, **World Bank**, **BEA**, **Census**, **SEC** (company
financials), and **OECD** are wired through the orchestrator end-to-end as
additional, differently-shaped sources. See `src/fred_pipeline/sources/` and
each source's `tests/test_*_client.py` (each runs its source through
`FredPipeline` and asserts series are dispatched to the right client).
Treasury, World Bank, Census, ECB, OECD, and SEC are keyless (SEC needs a
descriptive User-Agent); EIA and BEA require a key.

(This doc walks the `HTTPSource`-per-source pattern using OECD as
`specs/spec006`'s worked example. `bis.py`, `ishares.py`, `stooq.py`, and
`tiingo.py` aren't covered below — read them directly if extending one of
those; `bis.py` in particular shares OECD's SDMX-CSV parsing shape.)

SEC is the one that exercises the point-in-time machinery: each filing's `filed`
date becomes `realtime_start`, so restatements/amendments land as genuine
vintages. Its `build_sec_manifest` helper generates the (company × concept) grid
programmatically — the seed of a manifest generator for the ~1,000-company
universe. Standardizing heterogeneous XBRL tags into canonical statements + ratios
is implemented in `fred_pipeline.sec_standardization`
(`gold.fred_company_fundamentals` / `fred_company_ratios` /
`v_company_ratio_ranks`); disambiguating quarterly-vs-YTD income-statement
duration facts is the remaining refinement. The demo uses instant balance-sheet
concepts, which have no duration ambiguity.

## The layout

```
src/fred_pipeline/sources/
  base.py       # HTTPSource (rate limit + retry/backoff + _request) and the
                # SourceClient protocol — shared by every source
  fred.py       # FredClient: auth, vintage-cap recovery, vintage batching, discovery
  bls.py        # BLSClient: HTTP 200-on-failure, nested year/period payload
  eia.py        # EIAClient: v2 seriesid route, error-field body, period strings
  ecb.py        # ECBClient: keyless SDMX CSV, dataflow:key ids, revisions
  treasury.py   # TreasuryClient: keyless Fiscal Data, dataset:field series ids, paging
  worldbank.py  # WorldBankClient: keyless, top-level [meta,data] array, 200-on-error
  bea.py        # BEAClient: UserID auth, table:line series ids, Error-block on 200
  census.py     # CensusClient: key-optional, 2-D array payload, predicate series ids
  sec.py        # SECClient: keyless + User-Agent header, XBRL vintages (filed date)
  oecd.py       # OECDClient: keyless SDMX 2.1 CSV, agency:dataflow:key series ids, no vintages
```

`fred_pipeline.fred_client` remains as a thin back-compat shim re-exporting
`FredClient` / `FredAPIError` / `RateLimiter`, so existing imports keep working.

## The contract

A source implements `SourceClient` (`sources/base.py`):

```python
class SourceClient(Protocol):
    source_name: str
    def get_observations(self, series_id: str, **kwargs) -> dict: ...     # raw payload
    def normalize(self, series_id, payload, *, run_id=None,
                  track_vintage=True, source="fred") -> list[dict]: ...   # silver rows
```

* **`get_observations`** returns the *raw* upstream payload, archived verbatim
  in Bronze so Silver/Gold can always be re-derived.
* **`normalize`** maps that payload into the canonical silver schema
  (`transform.SILVER_COLUMNS`): `source, series_id, observation_date,
  realtime_start, realtime_end, value, raw_value, is_missing, row_hash,
  ingested_at, run_id`. The pipeline passes `source=spec.source` so each row is
  tagged with its origin (the leading component of the natural key). Downstream
  code is identical for every source.

## What you inherit vs. what you write

`HTTPSource` gives you, for free:

* the shared `RateLimiter` (one aggregate ceiling, thread-safe),
* retry with exponential backoff + jitter on transient/5xx/429,
* the `_request` engine and error raising.

Per source you override only what genuinely differs:

| Hook | FRED | BLS | EIA |
|---|---|---|---|
| `_default_query()` (auth) | `api_key` + `file_type=json` | `registrationkey` (optional) | `api_key` (required) |
| `_error_detail()` (error body) | `error_message` field | `message[]` list | `error` field |
| `get_observations()` | GET `series/observations`, vintage window | GET `timeseries/data/{id}`, checks `status` on HTTP 200 | GET `seriesid/{id}`, `start`/`end` bounds |
| `normalize()` | `transform.normalize_observations` | maps `year`+`period` → date, blanks realtime | maps `period` string → date, blanks realtime |

Each new source is a good stress test because it breaks a different FRED
assumption — BLS returns HTTP 200 on logical failure with a nested
`year`/`period` shape; EIA nests data under `response.data[]` and dates rows by
a frequency-dependent `period` string; ECB serves SDMX CSV with
`TIME_PERIOD`/`OBS_VALUE` and optional `VALID_FROM` revision timestamps. Yet in
every case the normalized rows
pass the same DQ checks and MERGE into the warehouse idempotently
(`test_bls_rows_pass_dq_and_merge_into_warehouse`,
`test_eia_rows_pass_dq_and_merge`).

## How a source is wired through the orchestrator

The following are implemented — a series declaring `source: bls` flows through
`FredPipeline` with no other changes:

1. **Manifest dimension.** `SeriesSpec` has an optional `source: str = "fred"`
   field (also in `manifests/manifest.schema.json`). The default keeps every
   existing manifest valid; a BLS series sets `source: bls`. See the shipped
   demo `manifests/bls_labor.yml` (inactive by default).
2. **Client selection.** `pipeline.SOURCE_FACTORIES` maps `source` → a client
   factory (currently `{"fred", "bls", "eia", "ecb", "oecd", "treasury",
   "worldbank", "bis", "bea", "census", "sec", "stooq", "ishares", "tiingo"}`
   — this doc covers the first nine end-to-end; see "Worked example: OECD"
   below for the SDMX composite-id case, and `sources/bis.py` /
   `sources/stooq.py` / `sources/ishares.py` / `sources/tiingo.py` directly
   for the rest). `FredPipeline._client_for(spec)`
   resolves and caches the right client per series; unknown sources fail that
   one series (per-series isolation) rather than the run. Clients can also be
   injected via the `clients=` constructor arg (used in tests).
3. **Normalize via the client.** `_finish_series` calls
   `FredPipeline._normalize`, which delegates to the source client's
   `normalize` (falling back to the FRED normalizer for lightweight test
   doubles) and then applies `assign_revision_numbers` uniformly, so revision
   numbering stays source-agnostic.

### Adding another source

EIA is the worked example (`sources/eia.py`, one `SOURCE_FACTORIES` entry, the
inactive demo `manifests/eia_energy.yml`). The recipe for the next one:

1. Add `sources/<name>.py` with a `<Name>Client(HTTPSource)` implementing
   `get_observations` + `normalize` (override `_default_query` / `_error_detail`
   as needed).
2. Register it: one entry in `SOURCE_FACTORIES`.
3. Author a manifest with `source: <name>`.

That's the whole change — nothing in Bronze/Silver/Gold moves.

### Worked example: OECD, a multi-part-id SDMX source (`specs/spec006`)

OECD (`sources/oecd.py`) followed the same recipe but is worth reading as the
example for a source whose natural id doesn't fit a single string cleanly.
OECD's REST path needs three coordinates — agency, dataflow, key — so
`series_id` in the manifest is `OECD:<agency>:<dataflow>:<key>` and
`parse_oecd_series_id()` splits it on `:` before every request. The agency
segment exists because it's genuinely **not** always `OECD` — the same public
catalogue also serves flows owned by `ESTAT` and `IAEG-SDGs`. This is the same
shape `treasury.py` (`<dataset>:<field>`) and `worldbank.py`
(`<country>:<indicator>`) already established for sources whose real
identifier isn't a bare code — a synthetic composite id, not a new mechanism.

Two things worth carrying into the next SDMX source (Eurostat/IMF per
`specs/spec006` §5.1, or any future OECD-family dataflow):

- **Self-contained parsing, deliberately duplicated.** `oecd.py` doesn't
  import `ecb.py`'s CSV helpers even though OECD serves the identical SDMX
  2.1 CSV shape (`TIME_PERIOD`/`OBS_VALUE`) — `bis.py` makes the same choice.
  Each source client owns its own parsing so one source's fix or bug can't
  silently change another's output.
- **No vintages is a real, source-level fact, not a gap to fill in later.**
  OECD's SDMX CSV carries no `VALID_FROM`/`VALID_TO`, so
  `realtime_start`/`realtime_end` are always empty and the manifest ships
  `vintage_enabled: false` — point-in-time queries over OECD data resolve to
  latest-revised only. Don't invent a vintage timestamp to satisfy the
  schema; leave it empty and say why, the way `oecd.py`'s module docstring
  does.

A dedicated `discover-oecd` CLI command doesn't exist and isn't needed:
`catalogs/ecb_discovery.py`'s structure-XML parser reads OECD's dataflow
catalogue unmodified (see `docs/catalog/oecd.md` for the one-line usage) —
another case of reusing existing machinery rather than building a parallel
discovery path per source.

### Per-source lineage in the natural key

Silver rows carry a `source` column, and it is the leading component of the
natural / MERGE key: `(source, series_id, observation_date, realtime_start)`
(`transform.SILVER_COLUMNS`, `silver.SILVER_MERGE_KEYS`, `sql/40_silver.sql`,
and the SQLite mirror in `local_store.py`). The pipeline stamps it from
`spec.source` in `_normalize`, so every row is tagged with its origin and two
sources can never collide even if they shared a `series_id` (manifests still
enforce globally-unique IDs today). Gold aggregations remain keyed on
`series_id`, which stays unambiguous under that guarantee.

### Bronze lineage + replay

Bronze rows also carry `source` and record the actual `endpoint` called
(`series/observations`, `timeseries/data/{id}`, `seriesid/{id}`), with an
`observation_count` taken from the normalized rows so it's accurate for sources
that nest their data. Because Bronze stores the source, the Bronze→Silver
replay (`replay.py`) re-derives each payload with the right normalizer —
dispatched by `silver._normalize_for_source`, which is module-level so replay
never has to construct a client (EIA needs a key just to instantiate).

### Deploy

The default `fred_ingestion` job (`resources/fred_pipeline.job.yml`) runs the
whole `manifests/` directory. For the one-job-per-source pattern — each source
on its own schedule, failing/alerting independently — see
`resources/source_jobs.yml`, which defines paused `bls_ingestion` /
`eia_ingestion` jobs scoped to their manifest files, with the API key wired from
a Databricks secret via `spark_env_vars`. The entrypoints
(`cli.py` / `notebooks/run_pipeline.py`) only require the keys the *active*
sources in the given manifest set actually need (`pipeline.missing_source_keys`),
so a BLS/EIA/ECB-only job doesn't demand a FRED key.

# Running the Pipeline with Multiple Sources (API Keys, Activation, Commands)

How to go from "I have an API key for EIA/BEA/BLS/…" to a fully built
Bronze → Silver → Gold database. Three steps: provide the keys, activate the
series, run. By default, Gold (including the market-terminal analytical views)
rebuilds automatically at the end of every non-dry run; for long local runs you
can use `--no-gold` and then `fred-pipeline gold` to separate API extraction
from the heavy analytical rebuild.

The pipeline only demands keys for sources that have **active** series in the
manifests: a FRED-only run never asks for an EIA key, and vice versa. If an
active source *is* missing its key, `fred-pipeline run` fails fast before
extracting anything, naming the exact environment variable to set.

---

## 1. Provide the API keys

Set environment variables (the recommended way — nothing lands in git):

| Source | Env var | Needed? | Where to get one |
|---|---|---|---|
| FRED | `FRED_API_KEY` | **Required** | https://fred.stlouisfed.org/docs/api/api_key.html |
| EIA | `EIA_API_KEY` | **Required** | https://www.eia.gov/opendata/register.php |
| BEA | `BEA_API_KEY` | **Required** | https://apps.bea.gov/api/signup/ |
| ECB | — | Nothing needed (keyless SDMX API) | — |
| BLS | `BLS_API_KEY` | Optional — keyless works at a lower daily quota | https://data.bls.gov/registrationEngine/ |
| Census | `CENSUS_API_KEY` | Optional — keyless works | https://api.census.gov/data/key_signup.html |
| SEC | `SEC_USER_AGENT` | Not a key — set to your **contact email**; SEC requires an identifying User-Agent | — |
| Tiingo (equity total return) | `TIINGO_API_KEY` | **Required** for `tiingo` series — free account key (personal use) | https://www.tiingo.com |
| US Treasury | — | Nothing needed (fully open) | — |
| World Bank | — | Nothing needed (fully open) | — |
| Stooq (optional equity-price reconciliation) | `STOOQ_API_KEY` | Only needed if you reactivate `equity_stooq.yml`; Gold now falls back to Tiingo prices | Open `https://stooq.com/q/d/?s=spy.us&get_apikey`, complete captcha, copy the `apikey` from the CSV download link |
| iShares/SSGA (ETF holdings) | — | Nothing needed (keyless CSV) | — |

```bash
export FRED_API_KEY="your-fred-key"
export EIA_API_KEY="your-eia-key"
export BEA_API_KEY="your-bea-key"
export SEC_USER_AGENT="you@example.com"
export STOOQ_API_KEY="your-stooq-key"
# optional quota bumps:
export BLS_API_KEY="your-bls-key"
export CENSUS_API_KEY="your-census-key"
```

Alternatively put them in a git-ignored `config/config.yaml`
(`cp config/config.example.yaml config/config.yaml`, then add e.g.
`eia_api_key: "..."` under `default:`). Precedence, highest wins:
**explicit CLI/arg → environment variable → config file → built-in default**
(and on Databricks, the FRED key additionally falls back to a secret scope).
For ECB network routing, the default endpoint is
`https://data-api.ecb.europa.eu/service`; set `ECB_BASE_URL` / `ecb_base_url`
only if your workspace routes ECB requests through an approved proxy or mirror.

## 2. Activate the series you want

Most non-FRED demo manifests ship `active: false` — plus some FRED additions
(`macro_flags.yml`, the `DGS3/DGS7/DGS20` curve tenors, `fed_funding.yml`,
`ice_credit.yml`) — because the series ids were assembled from documentation,
not verified against the live APIs. `ecb_rates.yml` is the exception: it ships
active with nine verified FX and rates series. Each manifest's header says
exactly where to verify its ids.

For each series you want, confirm the id at the source, then flip:

```yaml
# manifests/eia_energy.yml
  - series_id: ELEC.PRICE.US-ALL.M
    ...
    active: true        # was false
```

Manifests you may want to activate or run:

| Manifest | Source | Contents |
|---|---|---|
| `bls_labor.yml`, `bls_cpi_basket.yml`, `bls_cpi_basket_sa.yml` | bls | unemployment; full CPI-U item basket (NSA + SA) |
| `eia_energy.yml` | eia | energy prices/volumes |
| `treasury_fiscal.yml` | treasury | debt/fiscal series |
| `worldbank_global.yml` | worldbank | global indicators |
| `bea_national_accounts.yml` | bea | NIPA tables |
| `ecb_rates.yml` | ecb | active verified starter ECB Data Portal FX and rates series |
| `census_indicators.yml` | census | economic indicators |
| `sec_financials.yml` | sec | company XBRL fundamentals |
| `equity_stooq.yml` | stooq | optional/inactive broad ETFs + large-cap stocks for vendor reconciliation |
| `etf_holdings.yml` | ishares | ETF constituent weights (`gold.index_constituents`) |
| `equity_tiingo.yml` | tiingo | broad ETFs + stocks total return (needs `TIINGO_API_KEY`) |
| `macro_flags.yml` | fred | `USREC` — lights up every `is_recession` column |
| `fed_funding.yml` | fred | EFFR/IORB/OBFR/TGCR/… — funding tape + stress gauge; BGCRRATE remains inactive pending a verified replacement |
| `ice_credit.yml` | fred | ICE BofA OAS — credit-spread table |
| `rates.yml` (tail entries) | fred | DGS3/7/20 (full curve), DPRIME, MORTGAGE30US |

A wrong id is cheap: failures are **per-series isolated** — that one series
errors, everything else loads, and the run finishes `partial` with the error
recorded in `audit_etl_series_run`.

### ECB dataflow discovery

ECB is keyless, and the helper below lists available SDMX dataflows before you
decide which flow to inspect or turn into candidate manifest rows:

```bash
fred-pipeline discover-ecb --list-flows --search exchange
fred-pipeline discover-ecb --list-flows --search rates --json
```

The current implementation lists flows. `specs/spec002` tracks the next slices:
structure inspection and bounded inactive candidate manifest generation.

## 3. Run

```bash
pip install -e ".[local]"      # once; [local] adds polars for faster Gold builds

# 0) sanity-check the manifests (schema, duplicate ids, ...)
fred-pipeline validate --manifests manifests

# 1) DRY RUN — extract + data-quality checks in memory, nothing written.
#    The cheapest way to confirm a new key works and a series id is right:
fred-pipeline run --dry-run --series ELEC.PRICE.US-ALL.M

# 2) real local run: full Bronze → Silver → Gold into one SQLite file
fred-pipeline run --local --db-path fred_local.db
```

Useful flags for `fred-pipeline run`:

| Flag | Effect |
|---|---|
| `--series ID1,ID2` | run only those series (they must be `active`) |
| `--source fred,stooq` | run only active series from those sources |
| `--exclude-source tiingo` | skip active series from those sources |
| `--full` | force a full-history re-pull, ignoring the incremental restate watermark |
| `--no-gold` | extract/write Bronze + Silver, but skip the Gold rebuild |
| `--dry-run` | extract + validate in memory; no writes, no Gold |
| `--local --db-path f.db` | persist to SQLite instead of Databricks/Delta |
| `--extract-workers N` | override the global extraction thread count |
| `--source-workers fred=16,tiingo=1` | override per-source extraction thread counts |
| `--rate-limit-per-minute N` | override the FRED request rate limit |
| `--source-rate-limits fred=60,stooq=20,tiingo=5` | override per-source request-rate caps |
| `--env dev\|test\|prod` | pick the environment / catalog |
| *(no `--local`)* | Databricks/Spark backend — run inside a workspace job |

Rate-safe local full run, separating API extraction from the heavy Gold build:

```bash
fred-pipeline run --local --db-path fred_local.db --full --no-gold \
  --source-workers fred=8,stooq=2,tiingo=1 \
  --source-rate-limits fred=60,stooq=20,tiingo=5
fred-pipeline gold --local --db-path fred_local.db
```

Rebuild Gold from already-persisted Silver without calling external APIs:

```bash
fred-pipeline gold --local --db-path fred_local.db
```

Price ETF constituents from the latest iShares holdings:

```bash
# 1) Refresh holdings first; iShares stays the source of truth for membership.
fred-pipeline run --local --db-path fred_local.db --series IVV --no-gold
fred-pipeline gold --local --db-path fred_local.db

# 2) See which current constituents are missing/stale in Tiingo.
fred-pipeline price-constituents --db-path fred_local.db \
  --index-etf IVV --max-symbols 25 --stale-days 7 --dry-run

# 3) Pull the next quota-safe batch. The runner derives tickers dynamically
#    from gold_index_constituents, pulls one at a time, and stops on Tiingo 429s.
fred-pipeline price-constituents --db-path fred_local.db \
  --index-etf IVV --max-symbols 25 --rate-limit-per-minute 5

# 4) Rebuild Gold after one or more successful pricing batches.
fred-pipeline gold --local --db-path fred_local.db
```

Exit codes: `0` = succeeded or partial, `1` = failed, `2` = an active source
is missing its API key (the message names the env var).

## Operations reference

Every `fred-pipeline <command>` the CLI supports, grouped by what it does.
All commands accept `--env dev|test|prod` and `--config <path>`; only the
flags that differ in meaning are listed per command.

### Build / extract (talks to external APIs)

| Command | What it does | Key flags |
|---|---|---|
| `run` | Full Bronze → Silver → Gold: extracts active series from every source with an active manifest entry, then rebuilds Gold (unless `--no-gold`) | `--local --db-path`, `--series`, `--source` / `--exclude-source`, `--full`, `--dry-run`, `--no-gold`, `--extract-workers`, `--source-workers`, `--rate-limit-per-minute`, `--source-rate-limits` |
| `price-constituents` | Dynamic Tiingo pricing batch: reads current ETF membership from `gold_index_constituents`, prices only missing/stale tickers (by weight rank), stops on a Tiingo quota hit | `--index-etf`, `--max-symbols`, `--stale-days`, `--rate-limit-per-minute`, `--dry-run`, `--rebuild-gold` |
| `discover` | Generates a new manifest from a FRED category/release/search — not a refresh, a discovery/authoring tool | `--category-id` / `--release-id` / `--search`, `--frequencies`, `--min-popularity`, `--max`, `--out`, `--dry-run` |

### Refresh (rebuilds from already-ingested data — no external API calls)

| Command | What it does | Key flags |
|---|---|---|
| `gold` | Rebuilds every Gold table from already-persisted Bronze/Silver | `--local --db-path` |
| `replay` | Rebuilds Silver (+ Gold, unless `--no-gold`) from archived Bronze payloads — reprocesses raw data already on disk, e.g. after fixing a normalization/DQ bug | `--series`, `--no-gold`, `--local --db-path` |

### Maintenance / governance (metadata health, not observation data)

| Command | What it does | Key flags |
|---|---|---|
| `validate` | Schema/duplicate-id check on the manifests — no network, no backend; `--commercial` also fails on any active source whose license doesn't clear commercial use (`config/data_licensing.yml`) | `--manifests`, `--commercial` |
| `reconcile` | FRED-only metadata drift (title/frequency/units, discontinued, not-found) + all-source staleness (any source, via ingested data) | `--series`, `--local --db-path`, `--no-persist`, `--fail-on-drift` |
| `backfill` | Generates point-in-time Gold snapshots over a historical date range into a separate output DB — for backtesting, not a live refresh | `--from`, `--to` (required), `--step monthly\|weekly\|daily`, `--tables`, `--db-path`, `--backfill-db`, `--no-resume` |

## Common workflows

| Goal | Sequence |
|---|---|
| First-ever local run | `validate` → `run --dry-run --series <one id>` (confirm a key/id works cheaply) → `run --local --db-path fred_local.db` |
| Routine daily refresh | `run --local --db-path fred_local.db` (incremental restate-last-N + full Gold rebuild + release calendar, all in one) |
| Large first-time load, rate-limit-safe | `run --local --db-path f.db --full --no-gold --source-workers fred=8,tiingo=1 --source-rate-limits fred=60,tiingo=5` → `gold --local --db-path f.db` (separates slow extraction retries from the one-shot Gold rebuild) |
| Add a brand-new source/series | edit the manifest (`active: true`) → `validate` → `run --dry-run --series <new id>` → `run --local --db-path fred_local.db` |
| Grow ETF constituent coverage | `run --local --db-path f.db --series <ETF ticker>` (refresh holdings **and** rebuild Gold, so `gold_index_constituents` reflects today's snapshot before you plan against it) → `price-constituents --db-path f.db --index-etf <ETF> --dry-run` (preview the next batch) → `price-constituents --db-path f.db --index-etf <ETF> --rate-limit-per-minute 5` (pull it) → `gold --local --db-path f.db` (rebuild once more so price-derived tables pick up the new prices) |
| Health-check the series universe | `reconcile --local --db-path fred_local.db` (safe to run anytime — reads ingested data + FRED metadata only, doesn't touch other sources' quotas) |
| Recover from a bad DQ/normalization bug | fix the code → `replay --local --db-path fred_local.db` (reprocesses already-archived Bronze payloads, no re-fetch, no API quota spent) |
| Backtest "what did Gold look like historically" | `run --local --db-path fred_local.db` (ensure Silver is current) → `backfill --db-path fred_local.db --backfill-db fred_backfill.db --from 2015-01-01 --to 2026-01-01 --step monthly` |
| Onboard a new FRED category/release wholesale | `discover --name <manifest_name> --category-id <id> --dry-run` (preview) → re-run with `--out manifests/<name>.yml` → hand-review the generated YAML → `validate` → `run --dry-run --series <a few ids>` → activate for real |

## 4. Inspect the result

```bash
# per-source coverage & freshness (is anything stale/missing?)
sqlite3 fred_local.db "SELECT * FROM gold_v_source_coverage;"

# the ECON dashboard / curve lab / funding tape (market-terminal views)
sqlite3 fred_local.db "SELECT * FROM gold_macro_indicator_dashboard LIMIT 10;"
sqlite3 fred_local.db "SELECT * FROM gold_treasury_curve_metrics ORDER BY as_of_date DESC LIMIT 5;"
sqlite3 fred_local.db "SELECT * FROM gold_spread_inversion_episode;"

# what failed, if anything (per-series isolation writes the reason here)
sqlite3 fred_local.db "SELECT series_id, status, error_message
                       FROM audit_etl_series_run WHERE status != 'succeeded';"
```

## How it behaves (what you can rely on)

* **Key gating is per-source and activation-aware.** Keys are only required
  for sources with at least one active series (see
  `pipeline.SOURCE_KEY_REQUIREMENTS`); BLS and Census run keyless at reduced
  quotas; ECB, Treasury, World Bank, and SEC need no key at all (SEC wants
  `SEC_USER_AGENT`).
* **Idempotent re-runs.** Silver upserts on the natural key
  `(source, series_id, observation_date, realtime_start)` — activate a
  source, run, fix a bad id, run again; no cleanup needed.
* **Incremental by default.** After the first full load, each run re-pulls
  only the last `restate_last_n` observations per series (default 90) to
  capture revisions; `--full` overrides. See
  `docs/incremental_loading.md`.
* **Constituent pricing is dynamic.** iShares holdings populate
  `gold_index_constituents`; `fred-pipeline price-constituents` reads that
  current membership, compares it with Tiingo `:adjClose` freshness in Silver,
  and runs only a capped batch of missing/stale tickers. No static constituent
  pricing manifest is required.
* **Extraction is source-aware.** Active sources run in separate thread pools
  with separate client-side rate limiters. Tiingo defaults to a low worker cap
  to avoid hourly quota bursts; override with `--source-workers` or
  `FRED_SOURCE_EXTRACT_WORKERS` only when your quota supports it. Request-rate
  caps can be tuned with `--source-rate-limits` or `FRED_SOURCE_RATE_LIMITS`;
  retryable `429`/server responses honor upstream `Retry-After` when present.
* **Extraction progress is streamed.** Completed series are written to
  Bronze/Silver and local SQLite audit tables as soon as their fetch finishes,
  even while other source pools are still retrying or sleeping. Long local runs
  should log submitted source pools and periodic `Run ... progress: X/Y`
  messages instead of staying silent until every source future has completed.
* **Gold always rebuilds.** Every persisted run finishes by rebuilding all
  Gold tables — the classic feature tables *and* the market-terminal views
  (`docs/market_terminal_gold_views.md`). Tables whose inputs aren't ingested
  yet are simply empty and fill in as you activate series (e.g. every
  `is_recession` column stays NULL until `USREC` in `macro_flags.yml` is
  activated). Use `--no-gold` plus the standalone `gold` command when you want
  to separate API extraction retries from the heavy local Gold rebuild.

Related docs: [`architecture.md`](architecture.md) ·
[`adding_a_source.md`](adding_a_source.md) ·
[`incremental_loading.md`](incremental_loading.md) ·
[`deployment_runbook.md`](deployment_runbook.md) (Databricks jobs/secrets) ·
[`data_dictionary.md`](data_dictionary.md).

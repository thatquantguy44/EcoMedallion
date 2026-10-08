# Postgres Warehouse — Connection Guide and Database Overview

**What this is:** the local PostgreSQL copy of the medallion warehouse
(`meta` / `audit` / `bronze` / `silver` / `gold`): how to reach it, what is in
it, and what will trip you up.

**Snapshot:** every count and date below was measured against the running
database on **2026-10-07**. The data itself was last ingested on
**2026-09-07** (see [Freshness](#freshness)). Re-run the queries here before
relying on a number.

**Related:** [`../dictionary/data_dictionary.md`](../dictionary/data_dictionary.md)
(column-level definitions) ·
[`../deployment/postgres_deployment_runbook.md`](../deployment/postgres_deployment_runbook.md)
(provisioning, production concerns) ·
[`../reporting/powerbi_database_connections.md`](../reporting/powerbi_database_connections.md)
(Power BI specifics)

---

## 1. At a glance

| | |
|---|---|
| Engine | PostgreSQL 16.15, in Docker |
| Host / port | `127.0.0.1` : **`55432`** (not Postgres's usual 5432) |
| Database | `macro_medallion` |
| User / password | `fred` / `fred` |
| Container | `fred-pipeline-postgres` |
| Size | ~34 GB across 69 tables and 7 views |
| Connection string | `postgresql://fred:fred@127.0.0.1:55432/macro_medallion` |

> **Security — read this before sharing the connection string.** `fred` is the
> database's superuser, its password is `fred`, and Docker publishes the port
> on **all interfaces** (`0.0.0.0:55432`, confirmed with `docker port`), not
> just localhost. Anything that can reach this machine on that port can read,
> modify or drop the whole warehouse. Section 4 covers a read-only role and
> binding to localhost.

## 2. Is it running?

Three layers have to be up; only the last two are this repository's business.

1. **Docker Desktop** — the engine. It does not start at login unless you enable
   *Settings → General → Start Docker Desktop when you sign in*. Start it with
   `open -a Docker`.
2. **The container** — `docker compose up -d postgres`. The compose file sets
   `restart: unless-stopped`, so once Docker Desktop is running the container
   comes back on its own; you only need the command after an explicit
   `docker compose stop`.
3. **The database** — inside the container.

Quick check:

```bash
docker ps --filter name=fred-pipeline-postgres --format '{{.Status}}'
# Up 3 hours (healthy)
```

**The data does not live in the container.** It lives in a named Docker volume,
`fred-bronze-to-gold-pipeline_fred_postgres_data`. Stopping, removing or
recreating the container is harmless; only `docker volume rm` destroys the
warehouse, and rebuilding it means re-importing a ~32 GB SQLite file.

## 3. Connecting

Pick whichever fits. All use the credentials in section 1.

### psql inside the container (nothing to install)

```bash
docker exec -it fred-pipeline-postgres psql -U fred -d macro_medallion
```

To pipe a script in, add `-i` — without it `docker exec` silently discards
stdin and psql runs nothing:

```bash
docker exec -i fred-pipeline-postgres psql -U fred -d macro_medallion < my_query.sql
```

### psql from the host

macOS ships no `psql`. Install the client only with `brew install libpq`, then:

```bash
psql "postgresql://fred:fred@127.0.0.1:55432/macro_medallion"
```

### Python (psycopg 3)

```python
import psycopg  # pip install "psycopg[binary]"  (or: pip install -e '.[postgres]')

with psycopg.connect("postgresql://fred:fred@127.0.0.1:55432/macro_medallion") as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT observation_date::date, value "
            "FROM gold.fred_latest_observation "
            "WHERE series_id = %s ORDER BY observation_date DESC LIMIT 3",
            ("DGS10",),
        )
        for row in cur.fetchall():
            print(row)
# (datetime.date(2026, 9, 3), 4.77)
```

Use `%s` placeholders for values, never f-strings.

### This repository's own tool

```bash
python scripts/query_gold_layer.py --backend postgres --schema gold --list-tables
python scripts/query_gold_layer.py --backend postgres --query "SELECT COUNT(*) FROM gold.dim_series"
```

No environment variable is needed: `config/warehouse.yml` already defaults to the
DSN above.

### GUI clients and BI tools

DBeaver, TablePlus, pgAdmin, Power BI and similar all take the host, port,
database, user and password from section 1. For Power BI see
[`powerbi_database_connections.md`](../reporting/powerbi_database_connections.md).

## 4. Access control and exposure

The pipeline writes as `fred`, so keep that account for the pipeline. Give
anyone who only needs to read their own role.

This recipe was run and verified against this database (inside a transaction
that was rolled back): the new role can read, and `DELETE` is denied.

```sql
CREATE ROLE analyst_ro LOGIN PASSWORD 'choose-a-real-password';
GRANT CONNECT ON DATABASE macro_medallion TO analyst_ro;
GRANT USAGE ON SCHEMA gold, silver, meta TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA gold, silver, meta TO analyst_ro;
-- tables the pipeline creates later (it connects as fred):
ALTER DEFAULT PRIVILEGES IN SCHEMA gold, silver, meta GRANT SELECT ON TABLES TO analyst_ro;
```

`bronze` is omitted on purpose: it holds raw API payloads (696 MB). Add
`bronze` and `audit` to the `USAGE`/`SELECT` lines if a role needs them.

**Binding to localhost only.** `docker-compose.yml` currently maps
`"55432:5432"`, which publishes on every interface. If nothing off this machine
needs access, change it to `"127.0.0.1:55432:5432"` and recreate the container.
The data is unaffected (it is in the volume).

## 5. How the database is organised

Medallion layers: **bronze** keeps raw API responses, **silver** normalizes them
into one observation table, **gold** holds analytical tables built from silver.
`meta` and `audit` describe the series catalogue and pipeline runs.

| Schema | Tables | Rows (largest) | Size | What it holds |
|---|---:|---|---:|---|
| `bronze` | 1 | 20,308 | 696 MB | `fred_api_response` — raw payloads, one per API call |
| `silver` | 1 | 33,957,274 | ~9.7 GB | `fred_observation` — every observation of every series, all sources, all revisions |
| `gold` | 57 + 7 views | up to 80.6 M | ~23 GB | analytical tables and views (section 7) |
| `meta` | 6 | 3,021 series | 2 MB | series catalogue, lifecycle, drift, manifests |
| `audit` | 4 | 144,311 DQ results | 26 MB | pipeline runs, per-series results, data-quality results, query log |

### Where the data comes from

`silver.fred_observation` by source, as of 2026-10-07:

| Source | Series | Rows | First obs | Last obs |
|---|---:|---:|---|---|
| `tiingo` | 2,388 | 18,068,004 | 1962-01-02 | 2026-09-04 |
| `fred` | 2,576 | 15,607,062 | 1694-11-01 | 2026-09-08 |
| `ecb` | 43 | 236,175 | 1981-01-01 | 2026-09-07 |
| `bis` | 36 | 17,542 | 1946-01-01 | 2026-08-01 |
| `treasury` | 2 | 16,772 | 1993-04-01 | 2026-09-03 |
| `bls` | 70 | 4,180 | 2007-01-01 | 2026-08-01 |
| `ishares` | 511 | 3,043 | 2026-07-16 | 2026-09-04 |
| `worldbank` | 37 | 2,442 | 1960-01-01 | 2025-01-01 |
| `eia` | 2 | 794 | 1986-01-01 | 2026-08-01 |
| `bea` | 2 | 635 | 1947-01-01 | 2026-04-01 |
| `sec` | 3 | 625 | 2006-09-30 | 2026-06-27 |

For `tiingo`, a series is a bare equity ticker (daily prices and dividends,
the source behind `gold.equity_total_return_index`). For `ishares` it is derived
from a fund's published holdings file rather than a price series. Per-source
detail is in [`../catalog/`](../catalog/README.md). `meta.fred_series` lists
3,021 catalogued series, 2,913 flagged active.

### Freshness

Ingestion is a point-in-time job, not a stream. The most recent data in the
warehouse was ingested on **2026-09-07**, and that run finished `partial`
(2,818 series succeeded, 67 failed). Two runs started on 2026-09-06 are still
marked `running` in `audit.etl_run`; they appear to have been interrupted rather
than still working. To check the current state:

```sql
SELECT source, max(ingested_at) AS last_ingested
FROM silver.fred_observation GROUP BY source ORDER BY 2 DESC;

SELECT left(run_id, 8) AS run, status, started_at,
       series_total, series_succeeded, series_failed
FROM audit.etl_run ORDER BY started_at DESC LIMIT 5;
```

## 6. Things that will trip you up

**Dates are stored as `text`, not `date`.** `observation_date`, `realtime_start`,
`realtime_end`, `ingested_at` and `gold.dim_date.date` are ISO-8601 strings in
this database. [`data_dictionary.md`](../dictionary/data_dictionary.md) says
`DATE`, which is the Databricks type; the Postgres copy inherited the SQLite
column types. ISO strings sort correctly, so `ORDER BY` and range filters work
as-is, but arithmetic and date functions need a cast: `observation_date::date`.

**A bare `realtime_start::date` will error.** 18,674,587 silver rows (55%) store
an *empty string* in `realtime_start`, not `NULL`, and `''::date` is invalid
input. These are the series that are not vintage-tracked: every row from
`tiingo`, `bis`, `treasury`, `bls`, `ishares`, `worldbank`, `eia` and `bea`, plus
561,175 `fred` rows. Only `ecb`, `sec` and the remaining `fred` rows carry a real
date (15,282,687 rows in all). `NULL` does not occur. Use:

```sql
NULLIF(realtime_start, '')::date
```

**`series_id` is not unique on its own.** The silver key is
`(source, series_id, observation_date, realtime_start)`. The same raw id can come
from two sources, so filter or join on `source` as well.

**The database does not enforce that key.** `silver.fred_observation` has four
plain indexes and no primary or unique constraint. At snapshot time a duplicate
check on the key found zero duplicates, because the pipeline's merge logic keeps
it unique, but nothing in Postgres would stop a hand-written `INSERT` from
breaking it. In fact no table in `meta`, `audit`, `bronze` or `silver` has any
declared constraint, and in `gold` only `incremental_checkpoint` and
`build_watermark` do.

**`gold.dim_series` is a curated subset.** It has 290 series (268 `fred`,
20 `ecb`, 2 `bls`), not the ~2,900 in silver. Joining silver to it silently
drops everything else, including every `tiingo` and `ishares` row. Use
`meta.fred_series` for the full catalogue.

**Revisions.** Many macro series are revised. `silver` keeps every vintage;
`gold.fred_latest_observation` keeps only the latest revision per
`(series_id, observation_date)` ("as revised today"). Use silver for as-of /
point-in-time work (example below), gold for "current" values.

**The views.** `gold.fred_point_in_time` is a view over `silver.fred_observation`
(a 1:1 mirror, not a copy). It used to be a materialised table; an older copy of
this database still had the table and the pipeline refused to start until it was
dropped. If you restore from an old backup, expect that error.

**Big tables.** `gold.fred_series_zscore_rolling` (80.6 M rows),
`gold.fred_macro_feature_daily` (75.0 M), `gold.fred_feature_transforms` and
`gold.zscore_heatmap` (20.4 M each), `gold.realized_volatility` (17.8 M).
Always filter by `series_id` (silver and `fred_latest_observation` are indexed for
it) and avoid `SELECT *` without a `LIMIT`.

**Empty tables.** `gold.build_watermark`, `gold.incremental_checkpoint` and
`gold.equity_price_reconciliation` have no rows, as does
`meta.series_staleness`. The first two are pipeline bookkeeping; the others are
unpopulated, not broken.

## 7. Gold layer reference

Row counts as of 2026-10-07. Definitions and columns live in
[`data_dictionary.md`](../dictionary/data_dictionary.md); the tables marked †
are **not yet in the dictionary**, so check column names with `\d gold.<table>`
rather than assuming. `gold.powerbi_catalog` (queried below) records each Gold
object's type, module, grain and description, and covers some of these.

### Core series tables

| Table | Rows | Notes |
|---|---:|---|
| `fred_latest_observation` | 20,523,966 | latest revision per `(series_id, observation_date)` |
| `fred_point_in_time` (view) | 33,957,274 | every vintage, mirrors silver |
| `fred_macro_feature_daily` | 74,970,355 | daily calendar × series grid, forward-filled |
| `fred_feature_transforms` | 20,445,547 | `mom`, `diff`, `yoy`, expanding point-in-time-safe `zscore` |
| `fred_series_zscore_rolling` † | 80,617,379 | |
| `zscore_heatmap` † | 20,445,547 | |
| `fred_revision_stats` | 20,464,281 | |
| `fred_cross_series_feature` / `_pit` | 7,107 each | |
| `fred_source_reconciliation` | 349 | |
| `ml_feature_matrix` † | 9,099 | |

### Dimensions and calendars

| Table | Rows | Notes |
|---|---:|---|
| `dim_series` | 290 | curated series metadata (see section 6) |
| `dim_date` | 121,207 | calendar with fiscal, IMM and option-expiry flags and `is_recession` |
| `market_calendar` | 363,621 | |
| `release_calendar` | 56 | |
| `powerbi_catalog` | 54 | one row per Gold object: `object_type`, terminal `module`, `grain`, `intended_visual`, `description` |

### Rates, curve and credit

`treasury_curve` (147,091), `treasury_curve_metrics` (16,154),
`treasury_curve_rolling` (1,024,379), `curve_spread_daily` (115,956),
`curve_spread_rolling` (807,390), `fred_curve_spread` (115,956),
`spread_inversion_episode` (624), `yield_curve_ns_factors` † (16,154),
`credit_spread_daily` (7,397), `credit_spread_rolling` (47,477),
`benchmark_rate_board` (16), `funding_tape_daily` (29,086),
`funding_stress_daily` (1,273).

### Macro, inflation, regime and models

`macro_indicator_dashboard` (290), `macro_indicator_sparkline` (10,406),
`macro_category_summary` (11), `macro_regime_daily` (11,516),
`macro_factor_scores` † (1,630), `macro_factor_loadings` † (19,560),
`macro_anomaly_scores` † (307), `recession_probability_daily` † (2,061),
`inflation_explorer` (5,126), `inflation_contribution` (2,238),
`inflation_forecast` † (16), `global_inflation` (8,473),
`global_policy_rates` (28,519), `fomc_probability` (22),
`fomc_meeting_path` (12), `series_correlation` (78,797),
`series_lead_lag` (200), `series_structural_breaks` † (16).

### Equities

`equity_return_daily` (4,517,001), `equity_total_return_index` (4,517,001),
`realized_volatility` (17,793,383), `index_constituents` (3,043),
`equity_factor_attribution` † (2,061,350),
`equity_factor_implied_return` † (412,447),
`equity_price_reconciliation` † (0), `fred_company_fundamentals` (215),
`fred_company_ratios` (70).

### Views

`fred_point_in_time`, `v_point_in_time`, `v_latest_revised`,
`v_series_latest_value`, `v_series_revision_summary`, `v_source_coverage`,
`v_company_ratio_ranks`.

## 8. Example queries

All run against this database; the first three are the common patterns.

**Latest value of a series with its metadata**

```sql
SELECT s.series_id, s.title, s.units, l.observation_date::date AS obs_date, l.value
FROM gold.fred_latest_observation l
JOIN gold.dim_series s USING (series_id)
WHERE s.series_id = 'DGS10'
ORDER BY l.observation_date DESC LIMIT 3;
-- DGS10 | 10-Year Treasury Constant Maturity Rate | Percent | 2026-09-03 | 4.77
```

**Point-in-time: what was known on a given date**

One row per observation, using the newest vintage published on or before the
as-of date:

```sql
SELECT DISTINCT ON (observation_date)
       observation_date::date AS obs_date,
       realtime_start::date   AS known_from,
       value
FROM silver.fred_observation
WHERE source = 'fred' AND series_id = 'GDPC1'
  AND NULLIF(realtime_start, '')::date <= DATE '2020-06-15'
ORDER BY observation_date DESC, realtime_start DESC
LIMIT 3;
-- 2020-01-01 | 2020-05-28 | 18974.702   (the estimate available then, not today's)
```

**Yield-curve slope with the recession flag**

```sql
SELECT as_of_date::date AS d, round(slope_10y2y::numeric, 2) AS slope_10y2y,
       is_inverted_10y2y, is_recession
FROM gold.treasury_curve_metrics
ORDER BY as_of_date DESC LIMIT 3;
-- 2026-09-03 | 0.43 | 0 | 0
```

**Find out what a table is for**

`gold.powerbi_catalog` gives each Gold object's type, the terminal module it
serves, and its grain:

```sql
SELECT object_name, object_type, module, grain
FROM gold.powerbi_catalog
WHERE object_name IN ('treasury_curve_metrics', 'recession_probability_daily')
ORDER BY object_name;
-- recession_probability_daily | fact | ML   | 1 / date
-- treasury_curve_metrics      | fact | CURV | 1 / date
```

Its `description` and `intended_visual` columns say more. List the tables in
any schema with `\dt gold.*` in psql, or:

```sql
SELECT table_name, lower(table_type) AS kind
FROM information_schema.tables
WHERE table_schema = 'gold' AND table_name LIKE 'fomc%';
```

**Row counts.** Postgres's own estimates (`pg_stat_user_tables.n_live_tup`) read
0 for every table here because statistics were never collected, so use
`SELECT count(*)` when you need a number. On the 75 M-row tables that takes a
few seconds.

## 9. Populating and rebuilding

There are two ways data gets in.

- **Run the pipeline against Postgres.** Set `primary_backend: postgres` in
  `config/warehouse.yml`, then `python -m fred_pipeline run --env dev`. The
  backend creates missing schemas and tables itself. Do **not** pass `--local`;
  it forces SQLite. If the backend cannot initialize, the run now stops with
  exit code 2 before any extraction instead of silently running in memory.
- **Seed from an existing SQLite file** with
  `scripts/copy_sqlite_to_postgres.py`. It **replaces** the target schemas
  unless you pass `--append`, so check what is already in the database first
  (the row-count query above). See the
  [runbook](../deployment/postgres_deployment_runbook.md) §D2.

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `connection refused` on 55432 | Container is down. `open -a Docker`, then `docker compose up -d postgres`. |
| `relation "gold.x" does not exist` | Check the exact name with `\dt gold.*`. The pipeline creates tables on first connect, so a name from the dictionary may be absent from an older copy. |
| `invalid input syntax for type date: ""` | A bare `realtime_start::date`. Use `NULLIF(realtime_start, '')::date`. |
| `"fred_point_in_time" is not a view` | An old table is squatting on the view's name. Confirm its row count equals silver's, then `DROP TABLE gold.fred_point_in_time;`; the next run recreates the view. |
| Query runs for minutes | A big table without a `series_id` filter. Filter first, add `LIMIT`. |
| Joined to `dim_series` and rows vanished | It only holds 290 curated series (section 6). |
| `Every configured warehouse backend failed` | The run is refusing to start without a warehouse. Fix the connection, or pass `--dry-run` if you really want no persistence. |

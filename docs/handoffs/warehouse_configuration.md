# Warehouse Configuration — Pluggable Storage Backends

**Purpose:** Configure where Gold layer tables are persisted (SQLite, Databricks, Postgres, DuckDB, or in-memory).
**File:** `config/warehouse.yml`
**Code:** `src/fred_pipeline/io/warehouse_factory.py`

> **Adding Postgres?** This doc is the operator guide (setup/query/troubleshooting).
> For the full design rationale, the `market_terminal` contract it has to satisfy,
> and the phased build plan, see **[`specs/spec004`](../../specs/spec004/README.md)**.

---

## Overview

The pipeline supports multiple storage backends, allowing you to:

1. **Develop locally** with SQLite (default)
2. **Switch to Databricks** for production without code changes
3. **Fallback gracefully** if the primary backend fails

All configuration is done via `config/warehouse.yml` or environment variables. The CLI can override file settings with flags (`--local`, `--db-path`).

---

## Backends

### Local (SQLite) — Development & Testing

**Best for:** Local development, CI, demos, quick inspection.

**Configuration:**

```yaml
default:
  primary_backend: local
  backends:
    local:
      db_path: ./fred.db
```

**Setup:**

```bash
FRED_API_KEY=... python -m fred_pipeline run --env dev
# Creates ./fred.db with all tables
```

**Query Gold tables:**

```bash
sqlite3 fred.db "SELECT * FROM gold_fred_latest_observation LIMIT 10;"
```

**Pros:**
- Zero setup, works on laptop
- Fast iteration
- No credentials needed
- Easy to back up and version

**Cons:**
- Single-user only (SQLite doesn't support concurrent writes)
- Not suitable for production
- Limited query performance on large datasets

### Local (Postgres) — Planned, not yet implemented

**Status:** Scoped in [`specs/spec004`](../../specs/spec004/README.md); no
code yet. Priority: this is the **next** backend to build, ahead of DuckDB —
a sibling project (`market_terminal`) is blocked on a Postgres write path
from this pipeline (see the spec's §1 for the exact dependency).

**Why you'd want this over SQLite:** concurrent local writes (SQLite allows
only one writer at a time), and a schema-qualified `gold.<table>` naming
convention that matches what downstream consumers expect from a real
Postgres/Delta deployment — unlike SQLite's flat `gold_<table>` naming.

**Planned configuration shape** (see the spec for the finalized field names):

```yaml
default:
  primary_backend: postgres
  backends:
    postgres:
      dsn: postgresql://fred:fred@localhost:5432/fred_dev
```

Until this ships, use `local` (SQLite) for local development — it remains the
zero-setup default regardless of when Postgres lands.

### Databricks (Delta Lake) — Production

**Best for:** Production workloads, multi-user environments, BI tools.

**Configuration:**

```yaml
environments:
  prod:
    primary_backend: databricks
    backends:
      databricks:
        workspace_url: https://my-workspace.cloud.databricks.com
        http_path: /sql/1.0/warehouses/abc123
        catalog: macro_prod  # or omit to use environment's catalog
```

**Setup:**

```bash
export DATABRICKS_HOST=https://my-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi123...
export FRED_API_KEY=...
python -m fred_pipeline run --env prod
```

**Query Gold tables:**

```sql
SELECT * FROM macro_prod.gold.fred_latest_observation LIMIT 10;
```

**Pros:**
- Multi-user, concurrent writes
- Production-grade performance
- Integrates with Unity Catalog governance
- Native to Power BI & BI tools
- Audit/versioning built-in (Delta Lake)

**Cons:**
- Requires Databricks workspace + warehouse
- Monthly costs
- Needs credentials management

### DuckDB — Experimental (Future)

**Status:** Not yet implemented.

Planned for high-performance analytical workloads without Databricks cost.

---

## Fallback Chain

If the primary backend fails to initialize, the pipeline tries fallback backends in order:

```yaml
prod:
  primary_backend: databricks
  fallback_backends:
    - duckdb       # Try DuckDB if Databricks fails
    - local        # Fall back to SQLite if DuckDB fails
  backends:
    databricks: {...}
    duckdb: {...}
    local:
      db_path: /tmp/fred_emergency.db
```

**Example flow:**

1. Try Databricks → `ConnectionError` (workspace down)
2. Try DuckDB → `FileNotFoundError` (disk full)
3. Fall back to local SQLite at `/tmp/fred_emergency.db`
4. Run completes, warns about fallbacks in logs

This ensures the pipeline never silently fails to write; it degrades gracefully.

---

## Configuration Precedence

From highest to lowest:

1. **CLI flags** — `--local`, `--db-path`, `--dry-run`
2. **Environment variables** — `FRED_WAREHOUSE_CONFIG`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
3. **Config file** — `config/warehouse.yml` (or `$FRED_WAREHOUSE_CONFIG`)
4. **Built-in defaults** — Local SQLite at `./fred.db`

**Example:**

```bash
# Use config file setting (prod → Databricks)
python -m fred_pipeline run --env prod

# Override with CLI flag → use local SQLite instead
python -m fred_pipeline run --env prod --local --db-path /tmp/emergency.db

# Dry run (in-memory, no writes)
python -m fred_pipeline run --env prod --dry-run
```

---

## Common Scenarios

### Scenario 1: Quick Local Development

**Setup:** No config needed.

```bash
FRED_API_KEY=... python -m fred_pipeline run --env dev
```

**Result:** `./fred.db` created with all tables, ready to query or export.

### Scenario 2: Export to market_terminal

**Step 1:** Run pipeline locally.

```bash
python -m fred_pipeline run --local --db-path fred.db
```

**Step 2:** Export Gold tables as CSV or Parquet.

```bash
# From Python or a script:
import sqlite3
conn = sqlite3.connect("fred.db")

# List all Gold tables
tables = conn.execute("""
    SELECT name FROM sqlite_master
    WHERE type='table' AND name LIKE 'gold_%'
""").fetchall()

# Export one table
import pandas as pd
df = pd.read_sql("SELECT * FROM gold_fred_latest_observation", conn)
df.to_csv("gold_observations.csv", index=False)
```

**Step 3:** Ingest into market_terminal.

### Scenario 3: CI/CD Pipeline

**In `.github/workflows/pipeline.yml`:**

```yaml
- name: Run FRED pipeline
  env:
    FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
  run: |
    python -m fred_pipeline run \
      --env test \
      --local \
      --db-path /tmp/fred_ci.db \
      --series GDPC1,UNRATE  # subset for speed

- name: Upload results
  uses: actions/upload-artifact@v3
  with:
    name: fred_tables
    path: /tmp/fred_ci.db
```

**Result:** Artifact contains full pipeline output.

### Scenario 4: Production on Databricks

**In `config/warehouse.yml`:**

```yaml
environments:
  prod:
    primary_backend: databricks
    fallback_backends: [local]
    backends:
      databricks:
        workspace_url: https://prod.cloud.databricks.com
        http_path: /sql/1.0/warehouses/prod_id
        catalog: macro_prod
      local:
        db_path: /mnt/emergency/fred.db
```

**In Databricks job:**

```bash
export DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
export FRED_API_KEY=${FRED_API_KEY}
python -m fred_pipeline run --env prod
```

**Result:** Gold tables written to Delta Lake in `macro_prod.gold.*`, with fallback to Unity Catalog volume if needed.

---

## Troubleshooting

### "All warehouse backends failed. Falling back to in-memory dry-run."

**Meaning:** Warehouse initialization failed; run executed but **no writes occurred**.

**Check:**

1. Is `config/warehouse.yml` readable?
2. For local: is `db_path` a writable directory?
3. For Databricks: are `DATABRICKS_HOST` and `DATABRICKS_TOKEN` set?
4. Check logs for specific backend errors.

**Fix:**

```bash
# Verify local SQLite works
FRED_API_KEY=... python -m fred_pipeline run --local --db-path ./debug.db

# If that works, the issue is with your custom config
```

### "Could not initialize Spark for Databricks backend"

**Meaning:** Databricks backend requested but Spark is not available.

**Fix:**

```bash
# Option 1: Install pyspark
pip install pyspark

# Option 2: Use local SQLite instead
python -m fred_pipeline run --local

# Option 3: Run in a Databricks job (Spark is pre-installed)
```

### SQLite "database is locked"

**Meaning:** Concurrent writes attempted (SQLite doesn't support this).

**Fix:**

- For local dev: run one pipeline at a time.
- For production: use Databricks instead.

### "Cannot read from warehouse" in subsequent runs

**Meaning:** Pipeline can write but subsequent reads fail (e.g., reading Bronze for incremental load).

**Fix:**

- Verify the path/credentials are the same as the previous run.
- Check disk space (SQLite, DuckDB).
- Check Databricks cluster/warehouse status.

---

## Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `FRED_WAREHOUSE_CONFIG` | Path to warehouse config file | `/etc/fred/warehouse.yml` |
| `DATABRICKS_HOST` | Databricks workspace URL | `https://my.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token | `dapi123...` |
| `FRED_LOCAL_DB_PATH` | Override local SQLite path | `/tmp/fred.db` |
| `FRED_POSTGRES_DSN` | Postgres connection string (planned — see `specs/spec004`) | `postgresql://fred:fred@localhost:5432/fred_dev` |

**Note:** Environment variables override the config file but are overridden by CLI flags.

---

## Adding New Backends

To add a new warehouse backend (e.g., Postgres, BigQuery, Snowflake):

1. **Implement** a new module implementing the `Warehouse` protocol
   (`src/fred_pipeline/io/warehouse.py:24-45`) — a self-contained class in its
   own sibling module (e.g. `src/fred_pipeline/io/postgres_store.py`),
   structured like `LocalWarehouse` (`src/fred_pipeline/io/local_store.py`),
   not an edit to `warehouse.py` itself.
   - Methods: `sync_meta`, `restate_start`, `write_bronze`, `read_bronze`,
     `merge_silver`, `build_gold`, `write_lifecycle`, `write_drift`,
     `latest_observation_dates`, `write_staleness`, `write_release_calendar`,
     `persist_run`, `persist_dq`, `close`.
   - Reuse the same pure-Python compute-module layer `LocalWarehouse` already
     depends on (`features`, `transform`, `terminal_views`, etc.) — do **not**
     delegate to `fred_pipeline.bronze`/`.silver`/`.gold`/`.meta`, which are
     hard-locked to a live Spark session.

2. **Register** in `warehouse_factory.py::WarehouseFactory._build_backend()`
   ```python
   elif backend_name == "postgres":
       from fred_pipeline.io.postgres_store import PostgresWarehouse
       return PostgresWarehouse(self.config, **backend_config)
   ```

3. **Document** in `config/warehouse.yml` with example config

4. **Test** with a new `tests/test_<backend>_warehouse.py` (there is no
   `tests/test_warehouse_factory.py` today — only `test_local_store.py` and
   `test_spark_integration.py` cover this layer, so a new backend needs its
   own test file, not an addition to a shared factory test)

See [`specs/spec004`](../../specs/spec004/README.md) for the full Postgres
design (driver choice, DDL strategy, table-naming contract, phased plan) —
follow that spec rather than re-deriving these decisions from scratch.

---

## Current Status

| Backend | Status | Notes |
|---|---|---|
| Local (SQLite) | ✅ Production-ready | Default, fully tested |
| Databricks | ✅ Production-ready | Requires workspace |
| Local (Postgres) | 📋 Spec'd, not implemented | See `specs/spec004` — next backend to build |
| DuckDB | ⏳ Planned | Write side stubbed (`NotImplementedError`); read side (`DuckDBConnection`) already works |
| BigQuery | ⏳ Planned | No design yet; low priority (no known demand) |
| Snowflake | ⏳ Planned | No design yet; low priority (no known demand) |

---

## See Also

- `config/warehouse.yml` — Configuration template
- `src/fred_pipeline/io/warehouse_factory.py` — Factory implementation
- `src/fred_pipeline/io/local_store.py` — LocalWarehouse implementation
- `src/fred_pipeline/io/warehouse.py` — Warehouse protocol definition
- `src/fred_pipeline/io/database_connection.py` — the separate **read-path**
  `DatabaseConnection` protocol/factory (used by `scripts/query_gold_layer.py`
  and Power BI) — a different abstraction from the write-path `Warehouse`
  protocol above; a new backend usually needs both.
- [`specs/spec004`](../../specs/spec004/README.md) — Postgres backend design
  and build plan

# Power BI Database Connections — All Backends

**Purpose:** Connect Power BI to your Gold layer (SQLite, Databricks, Postgres, DuckDB) to build reports.
**Code:** `src/fred_pipeline/io/database_connection.py`

---

## Quick Connect

### SQLite (Local Development)

1. **Open Power BI Desktop**
2. **Home** → **Get Data** → **SQL Server** (or **More...** if not visible)
3. **Database** field: leave empty
4. **Advanced options** → **Connection string:**
   ```
   Driver={ODBC Driver 17 for SQL Server};Server=<sqlite_path>;TrustServerCertificate=yes
   ```
   Replace `<sqlite_path>` with your actual path (e.g., `C:\Users\you\fred.db`)

**Alternative (Direct):**
1. **Get Data** → **SQLite database**
2. **File path:** `./fred.db` (or full path)
3. Click **Connect**

### Databricks

1. **Home** → **Get Data** → **Databricks** (if you have the connector installed)
2. **Server hostname:** `your-workspace.cloud.databricks.com`
3. **HTTP path:** `/sql/1.0/warehouses/abc123def456`
4. **Data Connectivity mode:** Select **Import** (for reports) or **DirectQuery** (for live data)
5. Click **Sign in** and authenticate with your Databricks token

**Setup required:**
- Databricks connector for Power BI installed
- Personal Access Token (PAT) from Databricks workspace

### Postgres

Use this for either a local Postgres target or a managed/service Postgres
target once Gold tables are published there.

1. **Home** → **Get Data** → **PostgreSQL database**
2. **Server:** `localhost:55432` for local, or your service host/port
3. **Database:** `macro_medallion` locally, or the service database name
4. **Data Connectivity mode:** Select **Import** for snapshots or
   **DirectQuery** for live reads

Local default DSN:

```text
postgresql://fred:fred@localhost:55432/macro_medallion
```

Service targets should use a secrets-backed DSN such as
`FRED_POSTGRES_SERVICE_DSN` or `DATABASE_URL`.

### DuckDB

DuckDB connector for Power BI is not yet available in the official gallery.

**Workaround:** Export DuckDB to Parquet, then connect to Parquet in Power BI:

```python
import duckdb

conn = duckdb.connect("fred.duckdb", read_only=True)
conn.execute("""
    COPY (SELECT * FROM gold_fred_latest_observation)
    TO 'observations.parquet' (FORMAT PARQUET)
""")
conn.close()
```

Then in Power BI: **Get Data** → **Parquet** → select file.

---

## Python Script Connection

For custom integrations or data validation before Power BI:

```python
from fred_pipeline.io.database_connection import DatabaseConnectionFactory

# Connect to SQLite
db = DatabaseConnectionFactory.create(
    backend="local",
    db_path="./fred.db"
)

# Query
rows = db.query("SELECT * FROM gold_fred_latest_observation LIMIT 100")
print(f"Retrieved {len(rows)} rows")

# Stream large tables
for batch in db.query_stream(
    "SELECT * FROM gold_fred_latest_observation",
    batch_size=50000
):
    print(f"Processed {len(batch)} rows")

db.close()
```

Connect to Databricks:

```python
db = DatabaseConnectionFactory.create(
    backend="databricks",
    workspace_url="https://my-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/abc123",
    catalog="macro_prod"
)

# Query (same interface as SQLite)
rows = db.query("SELECT * FROM gold.macro_indicator_dashboard")
db.close()
```

Connect to Postgres:

```python
db = DatabaseConnectionFactory.create(
    backend="postgres",
    target="service",
    dsn_env="FRED_POSTGRES_SERVICE_DSN",
)

rows = db.query("SELECT * FROM gold.macro_indicator_dashboard")
db.close()
```

---

## Backend Comparison

| Aspect | SQLite | Databricks | Postgres | DuckDB |
|---|---|---|---|---|
| **Installation** | Built-in | Cloud service | Local service or managed DB | `pip install duckdb` |
| **Setup Time** | <1 min | 15–30 min | 5–15 min | 5 min |
| **Cost** | Free | $0.30–$5/hr | Free local; service varies | Free |
| **Multi-user** | No | Yes | Yes | No |
| **Concurrent Writes** | No | Yes | Yes | No |
| **Query Performance** | Slow on 1M+ rows | Fast | Good | Very fast |
| **Power BI Native** | Yes | Yes | Yes | Workaround (export) |
| **Best For** | Local dev | Production | Shared dev/deploy target | High-speed analytics |

---

## Setup for Each Environment

### Development (Laptop)

**Use:** SQLite
**Setup:**
```bash
# Run pipeline
FRED_API_KEY=... python -m fred_pipeline run --local --db-path fred.db

# Open in Power BI
# → Get Data → SQLite database → fred.db
```

**Pro:** No setup, instant iteration
**Con:** Single-user only

### CI/CD (GitHub Actions)

**Use:** SQLite (artifact storage)
**Setup:**
```yaml
- name: Run FRED pipeline
  run: |
    FRED_API_KEY=${{ secrets.FRED_API_KEY }} \
    python -m fred_pipeline run --local --db-path fred.db

- name: Upload
  uses: actions/upload-artifact@v3
  with:
    name: fred_data
    path: fred.db
```

**Then:** Download artifact, connect Power BI locally for inspection

### Staging (Team Collaboration)

**Use:** Databricks
**Setup:**
```yaml
# In config/warehouse.yml
environments:
  staging:
    primary_backend: databricks
    backends:
      databricks:
        workspace_url: https://staging.cloud.databricks.com
        http_path: /sql/1.0/warehouses/staging_id
        catalog: macro_staging
```

**Run:**
```bash
DATABRICKS_HOST=... DATABRICKS_TOKEN=... \
python -m fred_pipeline run --env staging
```

**Power BI:** Connect to the same Databricks workspace

### Production (Live Reports)

**Use:** Databricks with fallback
**Setup:**
```yaml
environments:
  prod:
    primary_backend: databricks
    fallback_backends: [duckdb, local]
    backends:
      databricks:
        workspace_url: https://prod.cloud.databricks.com
        http_path: /sql/1.0/warehouses/prod_id
        catalog: macro_prod
```

**Power BI:** Connect to Databricks with incremental refresh on Gold tables

---

## Sharing Data with market_terminal

### Option 1: Direct Database Connection

If market_terminal also supports these backends:

```python
# In market_terminal
from fred_pipeline.io.database_connection import DatabaseConnectionFactory

db = DatabaseConnectionFactory.create(
    backend="databricks",
    workspace_url="...",
    http_path="...",
    catalog="macro_prod"
)

# Query Gold tables directly
gold_inflation = db.query(
    "SELECT * FROM gold.global_inflation WHERE observation_date >= :cutoff",
    {"cutoff": "2024-01-01"}
)
```

### Option 2: Export to CSV/Parquet

```bash
# Export from SQLite
sqlite3 fred.db << EOF
.mode csv
.output global_inflation.csv
SELECT * FROM gold_global_inflation;
.quit
EOF

# market_terminal ingests CSV
```

### Option 3: API Endpoint

Expose Gold tables via a simple API:

```python
from flask import Flask, jsonify
from fred_pipeline.io.database_connection import DatabaseConnectionFactory

app = Flask(__name__)
db = DatabaseConnectionFactory.create(backend="local", db_path="fred.db")

@app.route("/api/inflation")
def inflation():
    rows = db.query("SELECT * FROM gold_global_inflation")
    return jsonify(rows)

if __name__ == "__main__":
    app.run()
```

market_terminal calls `GET /api/inflation` to fetch the latest data.

---

## Troubleshooting

### "Driver not found" in Power BI (SQLite)

**Solution:** Install ODBC driver for SQLite
- **Windows:** Download from [sqliteodbc.sf.net](http://sqliteodbc.sf.net)
- **macOS:** `brew install sqliteodbc`
- **Linux:** `apt-get install sqliteodbc`

Then use connection string:
```
Driver={SQLite3 ODBC Driver};Database=./fred.db
```

### "Cannot connect to Databricks" in Power BI

**Checklist:**
- [ ] Databricks connector installed (`Power BI → Extensions → Get more extensions`)
- [ ] PAT token valid (check Databricks settings)
- [ ] HTTP path correct (find in SQL warehouse settings)
- [ ] Network allows egress to `*.cloud.databricks.com`

### "Connection timeout" after first query

**Cause:** Databricks warehouse spun down (auto-stop).
**Fix:** Manually start the warehouse or increase auto-stop timeout in Databricks settings.

### Power BI model bloated (too many rows imported)

**Solution:** Filter at query time, not in Power BI
- **Bad:** Import all 100M rows, then filter in Power BI
- **Good:** Query with `WHERE observation_date >= CURRENT_DATE - INTERVAL 2 YEARS`

### Need to update live data without re-importing in Power BI

**Solution:** Use **DirectQuery** instead of **Import** (Databricks only)
- All queries hit the live warehouse
- No refresh schedule needed
- Slightly slower than Import mode

---

## Common Queries for Power BI

These are SQL templates you can paste directly into Power BI's "Web" or database connector advanced editor:

**Latest observations per series (for dashboards):**
```sql
SELECT series_id, observation_date, value
FROM gold_fred_latest_observation
WHERE observation_date >= date('now', '-1 month')
```

**Global inflation (multi-country):**
```sql
SELECT country, iso3, observation_date, cpi_yoy_pct
FROM gold_global_inflation
WHERE observation_date >= date('now', '-2 years')
ORDER BY country, observation_date DESC
```

**Funding stress gauge (financial):**
```sql
SELECT observation_date, stress_score, stress_bucket
FROM gold_funding_stress_daily
WHERE observation_date >= date('now', '-1 year')
ORDER BY observation_date DESC
```

---

## See Also

- `docs/handoffs/warehouse_configuration.md` — How to configure backends
- `docs/handoffs/powerbi_wave_0_kernel.md` — Building the Power BI data model
- `docs/handoffs/powerbi_report_14_health.md` — Complete Report 14 specification
- `src/fred_pipeline/io/database_connection.py` — Connection factory code

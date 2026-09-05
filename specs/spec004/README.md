# Spec 004: Database Backend Integration — Postgres (Priority) and Beyond

Status: proposed build plan
Last verified: 2026-09-04
Primary owner: TBD
Target: `PostgresWarehouse` (write path) + `PostgresConnection` (read path),
local-first

## 1. Goal

A sibling project, `market_terminal` (`../market_terminal` relative to this
repo), is migrating off live FRED/API calls to read **only** from this
pipeline's Gold layer. Its own handoff doc
(`docs/features/GOLD_DB_MIGRATION_HANDOFF.md`) has a locked decision (D3,
2026-07-17): *"DB backend: SQLite file for local/dev; **Postgres or
Databricks/Delta for deployment**, behind one connection abstraction
(`GoldStore`)."* Its open-decisions section (§12) is explicit that this is
**blocking**, quoting: *"Blocks: Phase 0 (`GoldStore` implementation); all
subsequent phases in deployed environments... Postgres | Standard; `pg`
driver; **pipeline's `--postgres` publish mode**."*

In plain terms: a downstream consumer is waiting on a Postgres write path from
this pipeline that does not exist yet. That is the concrete "why now," not a
hypothetical — and it's why local Postgres is priority 1 rather than a
directly-to-cloud design: it's the cheapest way to validate the write-path
design before anyone touches cloud secrets, it's what was directly requested,
and it removes SQLite's single-writer limitation for local multi-process use
(the `market_terminal` terminal and this pipeline writing/reading
concurrently, for instance).

This document is a spec and build plan, not code. It scopes what a Postgres
backend needs to satisfy (this pipeline's own `Warehouse`/`DatabaseConnection`
contracts, and the real external contract `market_terminal` already committed
to), makes the design decisions a future implementer would otherwise have to
re-derive, and phases the work. It also answers "what's next after Postgres"
for the broader "various databases" framing, with evidence rather than a
guess.

## 2. Current State

**Two separate, currently-incomplete pluggable-backend systems exist today —
this is the central fact everything else builds on.** They were built at
different times for different purposes and share no code today, even for the
same engine (SQLite).

### 2.1 Write path — `Warehouse` Protocol

`src/fred_pipeline/io/warehouse.py:24-45` defines a `@runtime_checkable
Protocol` with 14 methods: `sync_meta`, `restate_start`, `write_bronze`,
`read_bronze`, `merge_silver`, `build_gold`, `write_lifecycle`, `write_drift`,
`latest_observation_dates`, `write_staleness`, `write_release_calendar`,
`persist_run`, `persist_dq`, `close`. A shared helper, `dq_rows()`, lives in
the same file and is reused by every backend rather than reimplemented.

Selection is via `WarehouseFactory`/`WarehouseConfig`
(`src/fred_pipeline/io/warehouse_factory.py`, 212 lines), configured by
`config/warehouse.yml`. `_build_backend()` (lines 160-196):

```python
if backend_name == "local":
    from fred_pipeline.local_store import LocalWarehouse
    return LocalWarehouse(self.config, db_path=backend_config.get("db_path", "fred.db"))
elif backend_name == "databricks":
    from fred_pipeline.warehouse import SparkWarehouse
    ...
    return SparkWarehouse(self.config, spark)
elif backend_name == "duckdb":
    raise NotImplementedError("DuckDB backend not yet implemented. Use 'local' for now.")
else:
    raise ValueError(f"Unknown warehouse backend: {backend_name}")
```

**`local` and `databricks` work. `duckdb` is a named-but-stubbed branch that
always raises. There is no branch at all for `postgres`** — requesting it
today falls through to the generic `ValueError`.

`WarehouseFactory.build()` tries the primary backend, then each
`fallback_backends` entry in order, catching any exception per attempt; if
every backend fails, it returns `None` (in-memory dry-run) rather than
raising — the "fail gracefully, never silently lose a run" behavior
`docs/handoffs/warehouse_configuration.md` already documents.

**Reference implementation — `LocalWarehouse`**
(`src/fred_pipeline/io/local_store.py`, 1,413 lines) is fully self-contained:
its own `_SCHEMA` string (one `executescript()` call defining every table,
view, and index), its own additive-migration mechanism
(`_ADDED_COLUMNS` tuple + `ALTER TABLE ADD COLUMN`, needed because SQLite's
`CREATE TABLE IF NOT EXISTS` is a no-op against a pre-existing file), its own
row-by-row `_insert`/`_insert_frame` (via `executemany`, not a bulk/COPY
path). `build_gold()` wraps the full rebuild in one transaction
(`BEGIN`/commit/rollback). Grep-verified table count:
**56 Gold tables** (`grep -c "CREATE TABLE IF NOT EXISTS gold_"
local_store.py`), 6 views, 5 indexes; 69 `CREATE TABLE` statements total
across all five schemas (meta=6, audit=3, bronze=1, silver=1, gold=56) —
that 69 figure is real but is *every layer*, not Gold alone; don't confuse it
with the Gold-specific count. Two other numbers already in circulation in
this repo are stale: `docs/handoffs/INTEGRATION_READY.md:16,301` and
`docs/reporting/powerbi_data_model_schema.md:5,1158` both say **"46 Gold
tables"** — that was accurate at an earlier point and has not been updated as
tables were added since. Treat 56 as current ground truth; re-verify with a
fresh grep before quoting a table count anywhere, including in this spec's own
future revisions.

**A correction worth recording precisely, because an earlier research pass on
this exact question got it slightly wrong:** `sql/50_gold.sql` (the
Databricks-SQL mirror of the Gold schema, discussed in §2.3) does define all
56 Gold tables by name, matching `local_store.py` exactly — it is not missing
any. What's true is that **6 of the 56** (`fred_point_in_time`,
`fred_latest_observation`, `fred_macro_feature_daily`,
`fred_feature_transforms`, `fred_curve_spread`, `fred_revision_stats`) are
defined via `CREATE OR REPLACE TABLE gold.X AS SELECT ...` — real,
computed Spark-SQL query logic — while the other 50 are shape-only
`CREATE TABLE IF NOT EXISTS (columns...)`, populated by the Python job. That
is a genuinely different risk profile for those 6 (the SQL file's own query
logic could in principle drift from what the Python engine computes — this
research pass did not compare the two, so no claim of actual drift is made
here), not a "missing tables" problem.

**`LocalWarehouse` also implements two methods that are *not* part of the
`Warehouse` Protocol at all** — `persist_run_state(run)` and
`persist_series_run(series_run)` — called directly by
`pipeline.py::_persist_incremental_audit` (around line 800) for
crash-resilient, per-series audit writes, gated by a plain class attribute
`supports_incremental_audit = True` (not a Protocol member; `SparkWarehouse`
doesn't set it, so it defaults to `False` there). A backend that wants
progressive audit persistence must implement both methods and set the flag.

**`SparkWarehouse`** lives in the *same file* as the Protocol
(`io/warehouse.py:64-227`), not its own module. It is a thin adapter with
**no DDL in Python at all** — it assumes Delta tables already exist (created
by hand-run `sql/*.sql` per environment) and delegates to sibling modules
(`fred_pipeline.bronze`, `.silver`, `.gold`, `.meta`, `.io.spark_io`) for the
actual write logic (Delta `MERGE` via `DeltaTable.forName(...).merge(...)`,
or `df.write.format("delta").mode("append")`). Table names are three-part,
Unity-Catalog-style: `PipelineConfig.table(schema, name)` →
`f"{catalog}.{schema}.{name}"` (e.g. `macro_prod.gold.fred_latest_observation`).
`close()` is a no-op — Spark session lifecycle is managed by the Databricks
runtime, not this class.

**Important correction to a hypothesis that seemed plausible but is wrong: a
Postgres backend cannot delegate to `fred_pipeline.bronze`/`.silver`/`.gold`/
`.meta` the way `SparkWarehouse` does.** Those modules are hard-locked to a
live Spark session — `fred_pipeline/writer/gold.py`'s `build_gold()`
unconditionally calls `get_spark()` and executes Spark-SQL-dialect query
strings (window functions, `explode(sequence(...))`, Delta `MERGE`). Calling
them without Spark installed and running defeats the entire point of a
lighter-weight backend. **What `LocalWarehouse` actually reuses — and what a
Postgres backend should reuse the same way — is a family of pure-Python
compute modules with no Spark or SQLite dependency at all**: `features`,
`transform`, `regime_stats`, `ns_model`, `recession_model`, `macro_pca`,
`ml_features`, `equity_views`, `equity_factor_attribution`, `global_views`,
`terminal_views`, `zscore_views`, `sec_standardization`, `inflation_model`,
`anomaly`, and the polars-accelerated `gold_polars`. This is not incidental —
it's a **named, intentional repo-wide principle**, stated verbatim in
`docs/handoffs/completed/market_terminal_gold_views.md` §1: *"One Python
engine, two backends... No metric is expressed as unverifiable Spark SQL."*
`writer/gold.py`'s own `_build_cross_series()` docstring confirms the pattern
by name: it builds a Gold table on Spark "by reusing the pure-Python
reference... guaranteeing parity with the local backend." **A Postgres
backend should extend this to "one engine, three backends"** — structurally
mirror `LocalWarehouse` (self-contained DDL/DML, its own connection
management) while calling the same pure-Python compute layer `LocalWarehouse`
already depends on, not attempt to share `SparkWarehouse`'s delegation path.

### 2.2 Read path — `DatabaseConnection` Protocol (separate from 2.1)

`src/fred_pipeline/io/database_connection.py` (538 lines) defines a *second*
Protocol — `query`, `query_stream`, `execute`, `table_names`, `close`, plus
context-manager dunders — with **three working implementations already**:
`SQLiteConnection`, `DatabricksConnection`, and `DuckDBConnection` (lazy-
imports `duckdb`, connects **read-only**:
`duckdb.connect(self.db_path, read_only=True)`; its `execute()` raises
`NotImplementedError("DuckDB connection is read-only")`).
`DatabaseConnectionFactory.create(backend, **kwargs)` dispatches on
`"local"/"sqlite"`, `"databricks"`, `"duckdb"` — again, no Postgres branch;
an unknown value raises `ValueError(f"Unknown backend: {backend}")`. There's
also `DatabaseConnectionFactory.from_warehouse_config(...)`, which derives a
read connection from the same `WarehouseConfig` object the write-side factory
uses.

This is consumed by `scripts/query_gold_layer.py` (a CLI:
`--backend {local,sqlite,databricks,duckdb}`) and
`docs/reporting/powerbi_database_connections.md` (the Power BI connection
guide, which already has a "Sharing Data with market_terminal" section built
on this exact factory).

**A concrete bug in this path, worth fixing as part of the Postgres work, not
just wiring around:** `scripts/query_gold_layer.py`'s `list_tables()` filters
Gold tables via `t.startswith("gold_")` (line 161) — correct for SQLite's flat
naming, but it will silently report **zero** Gold tables against a
schema-qualified connection (Postgres, or Databricks with its
`catalog.gold.table` naming) once one exists, since neither name starts with
the literal string `"gold_"`. This needs a schema-aware check, not a
string-prefix heuristic.

### 2.3 `sql/*.sql` — the Databricks-SQL mirror (context, not what runs)

Eight files, run by hand per environment against Databricks SQL / Unity
Catalog, in dependency order: `00_catalog_schemas.sql` (catalog + 6 schemas +
1 volume) → `10_meta.sql` (6 tables) → `20_audit.sql` (3 tables) →
`30_bronze.sql` (1 table) → `40_silver.sql` (1 table) → `50_gold.sql` (56
tables, see §2.1's correction) → `60_views.sql` (5 views) → `fnGold.sql` (1
standalone helper UDF). Heavily Delta/Databricks-dialect-specific — `USING
DELTA` on every table, `CREATE VOLUME`, `TBLPROPERTIES
(delta.autoOptimize...)`, Spark-SQL builtins (`explode(sequence(...))` for
calendar generation, `add_months()`, `last_value(...) OVER (...)`,
`ARRAY<STRING>` columns, `DATEDIFF`), and a Databricks-SQL `CREATE OR REPLACE
FUNCTION fnGold(...) LANGUAGE SQL` UDF whose own comment already names the
exact cross-dialect problem this spec has to solve generally: *"For SQLite
mirror, substitute inline: fnGold('gold', 'table') → gold_table."* None of
this is portable to Postgres by mechanical find-replace — see §5's DDL
decision.

### 2.4 No DB driver dependency exists today

`pyproject.toml` core dependencies: `requests`, `PyYAML`, `numpy`. Optional
groups: `spark` (`pyspark`, `delta-spark`), `local` (`polars`), `dev` (test
tooling). **No `psycopg`, `psycopg2`, `sqlalchemy`, or `duckdb` is declared
anywhere in the repo** (confirmed by repo-wide grep). `sqlite3` needs no entry
(stdlib).

### 2.5 Existing "add a backend" guidance — aspirational, not implemented

`docs/handoffs/warehouse_configuration.md` (the active operator guide for
this whole area) already has an "Adding New Backends" section and a "Current
Status" table naming PostgreSQL as *"Planned — Community contribution
welcome"* — the only place in the repo Postgres is named for the write path,
with zero corresponding code. That section has two inaccuracies this spec's
companion handoff update (§9) fixes: it instructs implementing a new backend
"in `src/fred_pipeline/io/warehouse.py`" (the correct pattern, per §2.1, is a
new sibling module implementing the Protocol structurally via duck typing —
`warehouse.py` itself should not grow a third concrete class), and it
references `tests/test_warehouse_factory.py`, which **does not exist** — only
`tests/test_local_store.py` and `tests/test_spark_integration.py` currently
test this layer.

## 3. Non-Goals

- **BigQuery and Snowflake.** Zero code, zero config, and the only place
  either is named anywhere in either repo is the same aspirational status-
  table row as PostgreSQL. No downstream consumer asks for either. Revisit
  only if real demand appears — do not build speculative support now.
- **Migrating local dev off SQLite.** `LocalWarehouse` stays the zero-setup
  default. Postgres is additive/opt-in for people who want concurrent local
  writes or a closer-to-production engine, not a replacement.
- **Full cloud/production Postgres hardening** (managed-service choice, TLS,
  IAM, connection pooling at scale, secrets rotation). Deferred to a
  lightweight Phase 5 pointer doc (§6), not designed in depth here — this
  matches "local Postgres is priority 1."
- **Redesigning the Gold schema.** `docs/handoffs/completed/
  market_terminal_gold_views.md` shows all 7 of its phases marked
  **IMPLEMENTED** — the schema (dim_series, dim_date, and ~54 fact tables
  across ECON/INFL/CURV/BMRK/FUND/CRDT/REGIME/STAT/equity/ML surfaces) is a
  solved, stable problem. This spec is purely a new persistence backend for
  an existing schema, not a schema change.
- **A generic multi-dialect ORM or query-builder layer.** See §5's driver
  decision — this repo's existing style is deliberately "thin hand-rolled SQL
  + lazily-imported driver," across two independent Protocols. Introducing an
  ORM here would be a first, unjustified by anything in evidence.
- **Hand-transcribing full DDL for all 56 tables inside this document.**
  This spec commits to the *strategy* (§5's type-mapping rules, where the DDL
  lives, naming convention) — writing the actual column-by-column Postgres
  schema is implementation-time work against `local_store.py`'s existing
  `_SCHEMA` string as source of truth, not something to freeze in prose here.

## 4. The `market_terminal` Contract (a hard external constraint)

Read directly from `../market_terminal/docs/features/GOLD_DB_MIGRATION_HANDOFF.md`
(their own words, not inferred):

- **Table-name resolution differs per backend (§5.2 of their doc, verbatim):**
  their `GoldStore` resolves a **logical** name (e.g.
  `gold.macro_indicator_dashboard`) to a **physical** one: SQLite → flat
  `gold_macro_indicator_dashboard` (matches `LocalWarehouse` exactly);
  **Postgres/Delta → schema-qualified `gold.macro_indicator_dashboard`**
  (Delta additionally catalog-prefixed). **A Postgres backend must therefore
  use a real `gold` Postgres schema with unprefixed table names inside it —
  not SQLite's flat convention.** This is a real requirement from a real
  consumer, not a style preference. Their doc explicitly flags this SQLite
  prefix as something they hadn't confirmed on their own side ("Confirm the
  exact SQLite prefix against the pipeline's `LocalWarehouse`... before
  coding") — this spec closes that loop with the confirmed fact from §2.1.
- **Env var convention (their §5.1):** `MACRO_DB_URL=sqlite:./data/fred_local.db`
  for local/dev (this pipeline's `--local` output), or
  `MACRO_DB_URL=postgres://user:pass@host/db` for deploy ("pipeline Postgres
  publish"), or `MACRO_DB_BACKEND=databricks` + `DATABRICKS_HOST`/
  `HTTP_PATH`/`TOKEN` + `MACRO_CATALOG=macro_prod` for Delta. Drivers are
  described as lazy/optional ("`pg` for Postgres... exactly like the current
  `optionalRequire` pattern") — the same lazy-import convention this pipeline
  already uses for `pyspark`/`delta-spark`/`duckdb`.
- **The `GoldStore` interface they expect (their §5.3):** `latest<T>(table,
  where?)`, `history<T>(table, key, limit?)`, `asOf<T>(table, asOf, where?)`
  (point-in-time), `raw<T>(sql, params?)` (escape hatch), `health()`. Nothing
  here this pipeline needs to build — it's their client code — but it shapes
  what a healthy Postgres read surface needs to support (point-in-time
  queries via `realtime_start`/`realtime_end`, which the Gold schema already
  carries).
- **They already have a working Postgres/DuckDB read path for market
  (equity) data** — `src/app/api/market/[view]/route.ts`'s
  `readFromDb(MARKET_DB_URL, view)` — described in their doc as "the
  template for the `GoldStore` abstraction we generalize." This is
  independent corroboration (not just a stated preference) that Postgres is
  a real, exercised target for them, not merely aspirational.
- Their own deploy-target decision (Postgres vs. Databricks/Delta) is **still
  open on their side** (§12, `🔴 DECISION NEEDED — Deploy target DB backend`).
  See Open Decision #1 in §10 for what this means for sequencing.

## 5. Design Decisions

**Driver: `psycopg` (v3), not `psycopg2`, not SQLAlchemy.** Both existing
Protocols in this repo are consistently "thin hand-rolled SQL + a
lazily-imported driver + manual row handling, no ORM" (`LocalWarehouse` uses
raw `sqlite3`; `DatabricksConnection` uses the raw `databricks.sql` module).
`psycopg` v3's native `row_factory=dict_row` fits that shape directly.
`psycopg2` is in maintenance mode upstream; `psycopg` v3 is the actively
developed driver and has native `COPY` support — which matters concretely:
`specs/spec003/README.md` already diagnosed row-by-row `executemany` as the
dominant cost of SQLite's Gold rebuild at scale (32M+ rows) and proposed
set-based SQL as the fix. Building Postgres's write path on `COPY` (or
`INSERT ... SELECT` for the two dominant tables, per §6 Phase 1) from day one
avoids introducing that exact class of problem into a brand-new backend.
SQLAlchemy would be the first ORM/engine-abstraction dependency in a codebase
that has deliberately avoided one across two independent, hand-written
Protocols — it would fight the established pattern for no benefit this repo
has asked for.

**DDL location: a new `_SCHEMA`-equivalent Python string in a new
`src/fred_pipeline/io/postgres_store.py`**, mirroring `local_store.py`'s
actual load-bearing mechanism (executed on connect, with the same additive-
migration discipline, adapted per the point below) — not `sql/*.sql`, which
is a secondary, Databricks-dialect-locked mirror per its own header comment
(§2.3), not something a Postgres backend can port from mechanically.

**Type-mapping rules** (rules, not a full 56-table translation — that's
implementation work): SQLite `TEXT` → Postgres `TEXT`; SQLite's
bool-as-`INTEGER` → native `BOOLEAN`; `REAL` → `DOUBLE PRECISION`; ISO-string
timestamps (`_encode()`'s `datetime/date → isoformat str`) → native
`TIMESTAMPTZ`; JSON-as-text (`_encode()`'s `list/dict → json.dumps`) →
native `JSONB`. `LocalWarehouse._encode()` exists specifically to work around
SQLite's lack of these native types — a Postgres-side equivalent should be
much thinner, since Postgres can store most Python values directly via
`psycopg`'s type adapters.

**Migrations: native `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`**, run
unconditionally from an explicit, reviewable declarative list on every
connect — simpler than porting SQLite's manual `_ADDED_COLUMNS` tuple +
`PRAGMA table_info` existence-check loop, since Postgres's `IF NOT EXISTS`
variant makes the existence-check unnecessary. Keep the *discipline* of an
explicit list (for auditability and to avoid the exact class of bug
`specs/spec003/README.md` §2.1 already documented — a column added to the
schema string without a matching migration entry, which broke every Gold
rebuild against a pre-existing SQLite file until caught), just not the
existence-check boilerplate.

**`PostgresWarehouse` ↔ `PostgresConnection` relationship: two separate
classes** (matching the existing independence between `LocalWarehouse` and
`SQLiteConnection` — they don't share code today even for the same engine),
**with one small shared helper** for DSN resolution (~15-20 lines: accept
either a `dsn` string or discrete `host`/`port`/`database`/`user`/`password`
fields, env-var fallback) — a deliberate, narrow exception to the
"no shared connection code" precedent, justified because that logic would
otherwise be duplicated verbatim between the two classes for no reason.

**`config/warehouse.yml` shape** — support both a single `dsn` field
(preferred — it lets the exact same string be shared with `market_terminal`'s
own `MACRO_DB_URL` convention) and discrete fields as a fallback, `dsn`
taking precedence when present:

```yaml
backends:
  postgres:
    dsn: postgresql://fred:fred@localhost:5432/fred_dev
    # or, if dsn is omitted:
    # host: localhost
    # port: 5432
    # database: fred_dev
    # user: fred
    # password: ${FRED_POSTGRES_PASSWORD}
```

Add `FRED_POSTGRES_DSN` to the environment-variables table in
`docs/handoffs/warehouse_configuration.md` (§9), following the same
CLI > env var > config file > default precedence already documented there.
Never commit a real password to `config/warehouse.yml`'s tracked template.

**CLI surface — flagged as an open decision (§10 #3), not settled here.**
`market_terminal`'s doc assumes a `--postgres` publish-mode flag exists; this
pipeline's only other non-default backend (Databricks) uses zero dedicated
flags today (`--env` + `config/warehouse.yml` only). Recommend supporting
both: `--env <name>` + config as the primary mechanism (consistent with
Databricks), plus a `--postgres [dsn]` convenience flag mirroring the
existing `--local [--db-path]` pair — but confirm before building, since it's
a user-facing surface, not an internal implementation detail.

## 6. Proposed Approach

### Phase 1: `PostgresWarehouse` (write), minimal viable

New `src/fred_pipeline/io/postgres_store.py`, structured like
`LocalWarehouse`: self-contained (own DDL, own connection management),
implements all 14 `Warehouse` Protocol methods plus
`supports_incremental_audit = True` / `persist_run_state` / `persist_series_run`
(§2.1), reuses the same pure-Python compute-module layer `LocalWarehouse`
already depends on (§2.1's correction) rather than delegating to
Spark-locked writer modules. Tables live in a real `gold` (and `meta`/
`audit`/`bronze`/`silver`) Postgres schema, unprefixed inside it — matching
§4's confirmed `market_terminal` contract. For the two tables spec003 already
identified as dominant by row count (`fred_point_in_time`,
`fred_latest_observation`), write native `INSERT INTO ... SELECT` /
window-function SQL from day one, not a Python-materialization pass — the
same fix spec003 is retrofitting onto SQLite, built in correctly the first
time here.

Validate against a small fixture first (mirroring how `test_local_store.py`
tests are structured), before touching Phase 2 or later.

### Phase 2: `PostgresConnection` (read) + wiring

New class in `database_connection.py` implementing `DatabaseConnection`
(`table_names(schema="gold")` via `information_schema.tables WHERE
table_schema = %s` — `DatabricksConnection` already has a directly-adaptable
template for schema-qualified listing). Register in
`DatabaseConnectionFactory.create()`. **Fix the `query_gold_layer.py`
`startswith("gold_")` bug (§2.2)** to be schema-aware rather than working
around it for Postgres only — Databricks has the identical latent bug today,
just unexercised because no one has hit it yet.

### Phase 3: Registration, config, docs

`postgres:` block in `config/warehouse.yml`; `elif backend_name ==
"postgres":` in `WarehouseFactory._build_backend()` with a friendly
`ImportError` message if `psycopg` isn't installed (mirror how
`DatabricksConnection`'s lazy import already handles a missing driver); new
`postgres = ["psycopg[binary]>=3.1"]` optional-dependency group in
`pyproject.toml` (never a core dependency, matching `spark`/`local`'s
existing optional-group pattern); the `warehouse_configuration.md` update
(§9 of this spec); a Postgres quick-connect section in
`docs/reporting/powerbi_database_connections.md` (a real side-benefit: Power
BI has a native PostgreSQL connector, simpler than the SQLite-via-ODBC or
DuckDB-via-Parquet-export workarounds already documented there for the other
backends).

### Phase 4: Local dev workflow + tests

A `docker-compose.yml` at the repo root (single `postgres:16` service) —
genuinely green-field, there is no docker precedent anywhere in this repo
today (Open Decision #4, §10). New `tests/test_postgres_warehouse.py`
mirroring `test_local_store.py`'s test names and structure, plus a new
skip-if-unavailable Postgres fixture (mirroring how
`test_spark_integration.py` already skips cleanly when Spark/Java isn't
present locally — this matters, since the sandbox this pipeline has been
developed in has no Java runtime, and Postgres availability will vary the
same way across environments). Add an output-parity test: run identical
Bronze/Silver fixtures through both `LocalWarehouse.build_gold()` and
`PostgresWarehouse.build_gold()` and diff row sets — this is *output* parity
across two independent implementations, not *function* parity the way
`test_gold_polars_parity.py` checks two code paths that both call into the
same underlying logic; the assertion strategy is necessarily different. New
CI job in `.github/workflows/ci.yml` mirroring the existing
`spark-integration` job's shape (dedicated job, a `services: postgres:` block
instead of a pip-installed dependency, `pytest -q tests/test_postgres_warehouse.py`).

### Phase 5: Deployment/secrets pointer (non-local Postgres)

A new sibling doc, `docs/deployment/postgres_deployment_runbook.md`,
mirroring `docs/deployment/deployment_runbook.md`'s Part A–E skeleton
(A: provisioning — managed-service choice, schema/role bootstrap; B: config
values; C: quant/data decisions, mostly N/A here since it inherits pipeline
defaults; D: go-live sequence; E: acceptance checks + secrets appendix).
Explicitly a lightweight pointer, not a full design — consistent with
"local Postgres is priority 1," and gated on Open Decision #1 (§10): don't
over-build this phase before `market_terminal`'s own deploy-target decision
is actually confirmed.

## 7. What's Next After Postgres

**DuckDB — recommended, with evidence, not preference.** It already has a
`NotImplementedError` stub with a clear shape on the write side
(`warehouse_factory.py:189-193`) *and* a fully working, already-registered
read-only `DuckDBConnection` on the read side (§2.2) — this is "finish an
already-half-started thing," structurally different from BigQuery/Snowflake
(§3), which have zero code anywhere. A second, independent data point:
`market_terminal`'s own existing market-price read path already reads
"Postgres/DuckDB" per its own doc (§4), reinforcing DuckDB as the natural
second target rather than a guess. Scope this as a forward pointer / likely
`spec005`, not a phase of this spec — DuckDB's read-side story is already
solved; only its write side needs the same treatment Postgres gets here.

## 8. Acceptance Criteria

- **Phase 1:** `isinstance(PostgresWarehouse(...), Warehouse)` is `True`
  (the Protocol is `@runtime_checkable`); `build_gold()` against a shared
  test fixture produces 56 Gold tables + 6 views with row counts matching
  `LocalWarehouse` on the same fixture.
- **Phase 2:** `DatabaseConnectionFactory.create("postgres", dsn=...).query("SELECT 1")`
  succeeds; `query_gold_layer.py --backend postgres --list-tables` shows
  only `gold.*`-schema tables (proving the schema-aware fix, not the old
  string-prefix heuristic).
- **Phase 3:** `warehouse_from_config()` with `primary_backend: postgres`
  builds successfully end to end; re-reading
  `docs/handoffs/warehouse_configuration.md` after the edit shows neither of
  the two inaccuracies from §2.5 anymore.
- **Phase 4:** `docker compose up -d && pytest tests/test_postgres_warehouse.py`
  succeeds from a clean checkout with no manual Postgres setup beyond
  Docker itself; the new CI job is green.
- **Phase 5:** the runbook doc exists and cross-references this spec; no
  code acceptance criteria apply (it's a runbook, not an implementation).

## 9. Suggested First Implementation Slice

Phase 1 only, validated against a small fixture first — mirroring spec003's
own discipline of not building later phases speculatively before the first
one proves out. Get `PostgresWarehouse` passing the equivalent of
`test_local_store.py`'s core scenarios (persist all layers, idempotent
re-run, additive migration applies to a pre-existing db) before touching the
read side (Phase 2) or any config/CLI wiring (Phase 3).

## 10. Open Decisions

**🔴 #1 — `market_terminal`'s own deploy-target decision is still
unconfirmed on their side.** Their D3 lists Postgres *or* Databricks/Delta as
live options, not a locked choice (§4). Phases 1–4 here (local Postgres,
write + read) are worth building regardless of their eventual answer — they
serve this repo's own goals too (concurrent local access beyond SQLite's
single-writer limit). Phase 5 (cloud deployment hardening) should wait for
their confirmation rather than being over-built speculatively before there's
a real consumer for it.

**🔴 #2 — Three DDL sources will exist once Postgres ships, and can drift
independently.** `local_store.py`'s `_SCHEMA`, `sql/50_gold.sql` (already
carrying real computed-CTAS logic for 6 tables, per §2.1's correction — not
just a passive mirror), and the new Postgres schema. Worth a lightweight
schema-sync check (matching table/view name sets across all three) before a
third leg drifts from the other two, rather than discovering it the way the
`gold_dim_date` additive-migration miss was discovered (spec003 §2.1) — after
it broke something.

**🔴 #3 — CLI surface.** Confirm whether Postgres gets a dedicated
`--postgres [dsn]` flag (matching `--local [--db-path]`) alongside `--env` +
config, or config-only like Databricks today. `market_terminal`'s doc assumes
the flag exists; this repo's only precedent (Databricks) has none. See §5.

**🔴 #4 — Local Postgres provisioning mechanism.** Confirm Docker Compose as
the default local dev story — there's no in-repo precedent either way
(zero Docker files exist in this repo today), so this is a real convention
decision, not a rubber stamp of something already established.

**🟡 #5 (lower stakes) — `gold_fred_point_in_time` view vs. table.**
`specs/spec003/README.md` §7 already left this open for SQLite/performance
reasons (it's a 1:1 copy of `silver_fred_observation`, arguably better as a
view than a materialized copy). Worth resolving before or alongside writing
its Postgres DDL — Postgres supports both real and materialized views well —
rather than mechanically porting SQLite's current materialized-table choice
by default just because that's what exists today.

## 11. Follow-Ups

- DuckDB write-side implementation (§7), once Postgres Phase 1–4 validate the
  general "third backend" pattern.
- The `sql/50_gold.sql` computed-CTAS-vs-shape-only split (§2.1) as an
  independent small documentation cleanup — it's not a bug, but it's worth
  being explicit in that file's own header about which 6 tables carry real
  query logic vs. which 50 are pure schema mirrors, since a future reader
  will otherwise reasonably assume uniform treatment.
- Resolve Open Decision #5 (`gold_fred_point_in_time` view-vs-table) before
  Postgres Phase 1 writes that specific table's DDL, not after.

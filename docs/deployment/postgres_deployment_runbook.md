# Postgres Deployment Runbook

This runbook closes [Spec004](../../specs/spec004/README.md) Phase 5. It
covers the operational steps for using Postgres as a local or managed/service
warehouse target for the FRED medallion pipeline.

Scope: Postgres only. The code already supports the local Docker target,
managed/service DSN resolution, read queries through `PostgresConnection`, and
pipeline writes through `PostgresWarehouse`. This document records the human
decisions, credentials, and smoke checks needed to operate that path.

## Ownership at a glance

| Area | Owner | What |
|---|---|---|
| Database provisioning | Engineering / Platform | Local Docker service or managed Postgres database |
| Credentials and secrets | Platform | DSN storage, password rotation, SSL policy, app access |
| Pipeline configuration | Engineering | `config/warehouse.yml`, env vars, copy/smoke commands |
| Consumer access | Engineering / BI | Read grants for `gold`, DBeaver/Power BI connection details |

Track each item below with the checkboxes. Local development can be completed
entirely with Docker Compose; a managed/service target needs a provider-owned
database and credentials.

---

# Part A - Provisioning

## A1. Choose the target mode

Postgres configuration is intentionally split into two target profiles:

| Target | Use when | DSN source |
|---|---|---|
| `local` | Laptop/dev/CI with Docker or Homebrew Postgres | Defaults to `postgresql://fred:fred@localhost:55432/macro_medallion` unless overridden |
| `service` | Managed or hosted Postgres | Must be supplied by env var, secret, or explicit config |

- [ ] Target mode chosen: `local` or `service`
- [ ] Database name chosen, defaulting to `macro_medallion`
- [ ] Owner confirmed for credential storage and rotation

## A2. Local Docker Postgres

The repo ships a local Postgres 16 service in `docker-compose.yml`:

```bash
docker compose up -d postgres
docker compose ps postgres
```

Local connection details:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `55432` on the host, mapped to `5432` inside the container |
| Database | `macro_medallion` |
| User | `fred` |
| Password | `fred` |
| DSN | `postgresql://fred:fred@localhost:55432/macro_medallion` |

Quick health check:

```bash
docker exec fred-pipeline-postgres pg_isready -U fred -d macro_medallion
```

- [ ] Container is running and healthy
- [ ] Host port `55432` is reachable
- [ ] Local DSN is stored or intentionally using the built-in local default

## A3. Managed/service Postgres

For a service target, provision a normal PostgreSQL database through the chosen
provider. The pipeline does not require provider-specific features.

Minimum database requirements:

| Requirement | Notes |
|---|---|
| PostgreSQL version | 16 preferred; recent supported versions should work |
| Database | `macro_medallion` or an environment-specific equivalent |
| Role | Application role with `CONNECT`, `CREATE` on the database, schema ownership or equivalent DDL rights, and DML on medallion schemas |
| Schemas | `meta`, `audit`, `bronze`, `silver`, `gold`; `PostgresWarehouse` can create these if the role has permission |
| SSL | Follow provider policy; most service targets should require SSL |
| Backups | Provider snapshots or PITR enabled before first production write |

Provider-neutral bootstrap sketch:

```sql
CREATE DATABASE macro_medallion;
CREATE ROLE fred_pipeline LOGIN PASSWORD '<managed-secret>';
GRANT CONNECT, CREATE ON DATABASE macro_medallion TO fred_pipeline;
```

After connecting to `macro_medallion` as an admin:

```sql
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
ALTER SCHEMA meta OWNER TO fred_pipeline;
ALTER SCHEMA audit OWNER TO fred_pipeline;
ALTER SCHEMA bronze OWNER TO fred_pipeline;
ALTER SCHEMA silver OWNER TO fred_pipeline;
ALTER SCHEMA gold OWNER TO fred_pipeline;
```

`PostgresWarehouse` also bootstraps tables and indexes on connect. Pre-creating
schemas is useful when Platform wants explicit ownership and grants.

- [ ] Managed database provisioned
- [ ] Application role created
- [ ] SSL and backup policy confirmed
- [ ] Schema ownership/grants confirmed

---

# Part B - Configuration

Postgres settings live in `config/warehouse.yml` and can be overridden by env
vars. Resolution order is: explicit `dsn` > `dsn_env` > discrete fields >
target-specific env vars > local default.

## B1. Local config

```yaml
default:
  primary_backend: postgres
  backends:
    postgres:
      target: local
      dsn_env: FRED_POSTGRES_LOCAL_DSN
```

If `FRED_POSTGRES_LOCAL_DSN` is unset, local mode falls back to:

```text
postgresql://fred:fred@localhost:55432/macro_medallion
```

## B2. Service config

```yaml
prod:
  primary_backend: postgres
  backends:
    postgres:
      target: service
      dsn_env: FRED_POSTGRES_SERVICE_DSN
```

Service mode intentionally has no built-in DSN default. Supply one of:

| Variable | Purpose |
|---|---|
| `FRED_POSTGRES_SERVICE_DSN` | Preferred managed/service Postgres DSN |
| `DATABASE_URL` | Common platform fallback DSN |
| `FRED_POSTGRES_DSN` | Generic fallback DSN |

Example service DSN:

```text
postgresql://fred_pipeline:<password>@db.example.com:5432/macro_medallion?sslmode=require
```

- [ ] `config/warehouse.yml` target set correctly
- [ ] Environment variable name chosen
- [ ] No real service password committed to tracked files

---

# Part C - Credentials and Secret Storage

Programmatic access needs either a full DSN or these discrete fields:

| Field | Local default | Service guidance |
|---|---|---|
| Host | `localhost` | Provider host |
| Port | `55432` | Usually `5432` |
| Database | `macro_medallion` | Provider database name |
| User | `fred` | Application role, e.g. `fred_pipeline` |
| Password | `fred` | Provider secret |
| SSL mode | Usually omitted locally | Usually `require` or provider default |

Preferred runtime interface for this repo is a DSN stored in one of the env
vars from Part B. Store that env var value in your OS keyring, password
manager, CI secret store, or managed platform secret system.

## C1. macOS Keychain example

Store a local DSN:

```bash
security add-generic-password \
  -a "$USER" \
  -s fred-pipeline-postgres-local-dsn \
  -w "postgresql://fred:fred@localhost:55432/macro_medallion"
```

Load it into the shell before running the pipeline:

```bash
export FRED_POSTGRES_LOCAL_DSN="$(
  security find-generic-password \
    -a "$USER" \
    -s fred-pipeline-postgres-local-dsn \
    -w
)"
```

Use a separate keychain item for service credentials:

```bash
security add-generic-password \
  -a "$USER" \
  -s fred-pipeline-postgres-service-dsn \
  -w "postgresql://fred_pipeline:<password>@db.example.com:5432/macro_medallion?sslmode=require"
```

## C2. Python keyring example

`keyring` is not a repo dependency; this is an operator convenience if you
already use it.

```bash
python -m pip install keyring
python -c 'import keyring; keyring.set_password("fred_pipeline", "FRED_POSTGRES_SERVICE_DSN", "postgresql://fred_pipeline:<password>@db.example.com:5432/macro_medallion?sslmode=require")'
export FRED_POSTGRES_SERVICE_DSN="$(python -c 'import keyring; print(keyring.get_password("fred_pipeline", "FRED_POSTGRES_SERVICE_DSN"))')"
```

## C3. CI or managed runtime

Store the DSN as a masked secret named `FRED_POSTGRES_SERVICE_DSN` or
`DATABASE_URL`. Do not split host/user/password across multiple secrets unless
the runtime requires it; one DSN matches the pipeline's config path and the
downstream `market_terminal` convention.

- [ ] DSN stored in an approved secret/keyring system
- [ ] Local and service secrets use separate names
- [ ] Rotation owner and cadence documented outside this repo

---

# Part D - Go-live sequence

## D1. Local smoke path

Start local Postgres:

```bash
docker compose up -d postgres
```

Install the optional driver if needed:

```bash
python -m pip install -e '.[postgres]'
```

List Gold tables:

```bash
python scripts/query_gold_layer.py \
  --backend postgres \
  --schema gold \
  --list-tables
```

Run a bounded smoke query:

```bash
python scripts/query_gold_layer.py \
  --backend postgres \
  --query "SELECT COUNT(*) AS n FROM gold.fred_latest_observation"
```

## D2. Copy an existing SQLite warehouse into local Postgres

Use this when you want to seed local Postgres from `fred_local.db`:

```bash
python scripts/copy_sqlite_to_postgres.py \
  --sqlite-db fred_local.db \
  --postgres-db macro_medallion \
  --container fred-pipeline-postgres \
  --user fred
```

The copy script maps SQLite flat names to Postgres schemas:

| SQLite | Postgres |
|---|---|
| `gold_fred_latest_observation` | `gold.fred_latest_observation` |
| `silver_fred_observation` | `silver.fred_observation` |

The script streams through `COPY` and does not write intermediate CSV files.

## D3. Write directly to Postgres

Set `primary_backend: postgres` in the target environment config, or provide a
Postgres backend config that resolves to the desired DSN. Then run the normal
pipeline command for that environment.

Example local env var:

```bash
export FRED_POSTGRES_LOCAL_DSN="postgresql://fred:fred@localhost:55432/macro_medallion"
```

Example service env var:

```bash
export FRED_POSTGRES_SERVICE_DSN="postgresql://fred_pipeline:<password>@db.example.com:5432/macro_medallion?sslmode=require"
```

Run the pipeline using the environment whose warehouse config selects Postgres:

```bash
FRED_API_KEY=... python -m fred_pipeline run --env dev
```

## D4. DBeaver connection

For local Docker Postgres:

| DBeaver field | Value |
|---|---|
| Driver | PostgreSQL |
| Host | `localhost` |
| Port | `55432` |
| Database | `macro_medallion` |
| Username | `fred` |
| Password | `fred` |
| SSL | Disabled/Default for local Docker |

For service Postgres, use the provider host, usually port `5432`, the
application username/password, and the provider-required SSL mode.

- [ ] Local smoke query passes
- [ ] Optional SQLite copy completed, if needed
- [ ] Pipeline writes directly to the chosen Postgres target
- [ ] DBeaver or BI read access confirmed

---

# Part E - Acceptance Checks

Run these checks before considering the Postgres target ready.

## E1. Object surface

Expected local copy/warehouse surface:

| Layer | Expected |
|---|---|
| Schemas | `meta`, `audit`, `bronze`, `silver`, `gold` |
| Gold base tables | 57 |
| Gold views | 7 |

Check with:

```sql
SELECT table_schema, COUNT(*) AS n
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema IN ('meta', 'audit', 'bronze', 'silver', 'gold')
GROUP BY table_schema
ORDER BY table_schema;

SELECT table_schema, COUNT(*) AS n
FROM information_schema.views
WHERE table_schema = 'gold'
GROUP BY table_schema;
```

## E2. Query smoke checks

```bash
python scripts/query_gold_layer.py \
  --backend postgres \
  --schema gold \
  --list-tables

python scripts/query_gold_layer.py \
  --backend postgres \
  --query "SELECT COUNT(*) AS n FROM gold.fred_latest_observation"
```

For service targets, add:

```bash
python scripts/query_gold_layer.py \
  --backend postgres \
  --postgres-target service \
  --postgres-dsn-env FRED_POSTGRES_SERVICE_DSN \
  --schema gold \
  --list-tables
```

## E3. Pipeline acceptance

For local development:

```bash
pytest -q tests/test_postgres_warehouse.py
```

For service deployment:

- [ ] `PostgresWarehouse` initializes successfully
- [ ] A pipeline run can write Bronze, Silver, Gold, and audit rows
- [ ] `gold.fred_latest_observation` is queryable by downstream consumers
- [ ] Credentials are read from a secret/keyring path, not a tracked file
- [ ] Backup/restore expectations are documented with the provider

## E4. Handoff to downstream consumers

Provide downstream consumers with:

| Item | Local | Service |
|---|---|---|
| Backend type | `postgres` | `postgres` |
| Host/port | `localhost:55432` | Provider host/port |
| Database | `macro_medallion` | Service database |
| Gold naming | `gold.<table>` | `gold.<table>` |
| DSN env var | `FRED_POSTGRES_LOCAL_DSN` | `FRED_POSTGRES_SERVICE_DSN` or `DATABASE_URL` |

`market_terminal` should consume the same service DSN through its own secret
path and read from schema-qualified Gold tables such as
`gold.macro_indicator_dashboard`.

---

# Troubleshooting

## Local port confusion

Use `55432` from the host. The container listens on `5432` internally, but
Docker maps host `55432` to container `5432`.

## Service mode fails without a DSN

This is expected. `target: service` has no default because real service
credentials must come from a secret-backed DSN. Set `FRED_POSTGRES_SERVICE_DSN`,
`DATABASE_URL`, `FRED_POSTGRES_DSN`, or an explicit `dsn` in an untracked
config.

## Permission denied creating schemas or tables

The application role needs `CREATE` on the database and ownership or sufficient
DDL rights on the medallion schemas. Either grant those rights or pre-create
the schemas/tables through an admin-managed migration process.

## DBeaver cannot connect locally

Confirm Docker is running, the container is healthy, and DBeaver is using
`localhost` port `55432`, not `5432`.

```bash
docker compose ps postgres
docker exec fred-pipeline-postgres pg_isready -U fred -d macro_medallion
```

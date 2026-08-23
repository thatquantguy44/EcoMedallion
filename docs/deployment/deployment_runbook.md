# Deployment Runbook & Decision Register

Everything in this repo that is **not code** — the workspace provisioning steps
and the human decisions — collected in one place so a team can take the pipeline
live. The code is complete; this is the "who provisions what" and "who decides
what" checklist.

Scope: Databricks (Unity Catalog + Delta + Jobs + Secrets + Asset Bundles). For
a laptop/CI run no provisioning is needed — see the local quickstart in the
README (`python -m fred_pipeline run --local`).

## Ownership at a glance

| Area | Owner | What |
|---|---|---|
| Workspace provisioning | **Engineering** | Catalogs, schemas, tables, volume, bundle deploy, jobs |
| Secrets, permissions, egress, monitoring, cost | **Platform** | Secret scope + keys, grants, network policy, alerts, cluster policy |
| Series universe & data policy | **Quant** | Which series are active, per-series validation/vintage/restate, spreads |

Track each item below with the checkboxes; nothing here requires a code change.

---

# Part A — Workspace provisioning (Engineering / Platform)

### A1. Unity Catalog: catalogs + schemas + raw volume
Run `sql/00_catalog_schemas.sql` **once per environment**, replacing `{{catalog}}`
with `macro_dev`, `macro_test`, `macro_prod`. Creates the catalog, the six
schemas (`meta`, `audit`, `bronze`, `silver`, `gold`, `sandbox`), and the
`bronze.raw` volume used for JSON archival.

- [ ] `macro_dev` created
- [ ] `macro_test` created
- [ ] `macro_prod` created

### A2. Delta tables + views
Run the remaining DDL **in order**, same `{{catalog}}` substitution, per
environment (idempotent — all `CREATE ... IF NOT EXISTS`):

`10_meta.sql` → `20_audit.sql` → `30_bronze.sql` → `40_silver.sql` →
`50_gold.sql` → `60_views.sql`

> These mirror `fred_pipeline` exactly. Bronze/Silver already include the
> multi-source `source` column and the `(source, series_id, observation_date,
> realtime_start)` Silver key — no manual change needed.

- [ ] DDL applied to dev / test / prod

### A3. Secret scope + API keys (Platform)
One scope (default name `fred`) holds every source key; the field name **is** the
secret key (matches `resources/source_jobs.yml`).

```bash
databricks secrets create-scope fred
databricks secrets put-secret   fred api_key        # FRED  (required)
databricks secrets put-secret   fred bls_api_key    # BLS   (optional — keyless works)
databricks secrets put-secret   fred eia_api_key    # EIA   (required if EIA active)
databricks secrets put-secret   fred bea_api_key    # BEA   (required if BEA active)
databricks secrets put-secret   fred census_api_key # Census(optional — keyless works)
```

Treasury, World Bank, Census, and SEC are keyless. **SEC** needs a descriptive
`SEC_USER_AGENT` (your contact) set as an env var / `spark_env_vars`, not a
secret — SEC 403s without it. Optionally set `SEC_PERIOD` (`quarterly` default /
`annual`) to choose the income-statement duration.

- [ ] FRED key stored (all envs)
- [ ] EIA / BEA keys stored **iff** those sources are activated (see Part C)
- [ ] `SEC_USER_AGENT` set iff SEC is activated

### A4. Egress / network policy (Platform)
Clusters must be allowed to reach the source APIs. Confirm outbound HTTPS to:

| Source | Host | Needed when |
|---|---|---|
| FRED | `api.stlouisfed.org` | always |
| BLS | `api.bls.gov` | a `source: bls` series is active |
| EIA | `api.eia.gov` | a `source: eia` series is active |
| ECB | `data-api.ecb.europa.eu` | a `source: ecb` series is active; override with `ECB_BASE_URL` only for approved proxy/mirror routes |
| US Treasury | `api.fiscaldata.treasury.gov` | a `source: treasury` series is active |
| World Bank | `api.worldbank.org` | a `source: worldbank` series is active |
| BEA | `apps.bea.gov` | a `source: bea` series is active |
| Census | `api.census.gov` | a `source: census` series is active |
| SEC | `data.sec.gov` | a `source: sec` series is active |

- [ ] Egress confirmed for the sources you will run

### A5. Compute / cluster policy (Platform)
The job clusters in `resources/*.yml` specify `node_type_id: Standard_DS3_v2`
(**Azure**). **Decision:** confirm or change the node type for your cloud
(AWS/GCP), worker count, and any org cluster policy / instance pool.

- [ ] Node type + workers confirmed per cloud
- [ ] Cluster policy / pool applied (if org-mandated)

### A6. Asset Bundle deploy
Set the bundle variables in `databricks.yml` (or `-t` target overrides):
`workspace_host`, `service_principal` (prod `run_as`), `catalog`, `manifests_path`.

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run fred_ingestion -t dev      # first manual run
```
Promote `-t test` then `-t prod` after acceptance (Part E).

- [ ] `workspace_host` / `service_principal` set
- [ ] Deployed to dev / test / prod

### A7. Jobs & schedules
All schedules ship **PAUSED** on purpose.

- **`fred_ingestion`** (`resources/fred_pipeline.job.yml`) — runs the whole
  `manifests/` dir; daily `0 15 12 * * ?` UTC.
- **`bls_ingestion` / `eia_ingestion`** (`resources/source_jobs.yml`) — per-source
  templates scoped to their manifest files, with the key injected via
  `spark_env_vars`. Use these **only** if you adopt the one-job-per-source model
  (then scope `fred_ingestion` to FRED-only manifests so a series isn't ingested
  twice — harmless, just wasteful).

- [ ] **Decision:** single job vs. one-job-per-source (Part B, D3)
- [ ] Schedule(s) reviewed and unpaused when ready

### A8. Grants / permissions (Platform)
Grant read on `gold` (and `silver` for PIT) to quant/BI consumers; restrict
`bronze`/`audit`/`meta` writes to the job principal.

- [ ] Grants applied per environment

### A9. Monitoring & alerts (Platform)
- Run-failure email is wired in the job YAML (`email_notifications.on_failure`).
- App-level notifications post a run summary to a Slack-compatible webhook:
  set `alert_webhook_url` + `notify_on` (Part B).

- [ ] Failure email recipients confirmed
- [ ] Alert webhook set (or intentionally skipped)
- [ ] Cost controls / budget alerts configured

### A10. Query-level access logging (Platform)
Once Gold/Silver tables are registered in Unity Catalog, every query against
them is already captured in Unity Catalog's built-in audit log (system
tables — query history / `system.access.audit`) — no pipeline code needed to
get this. Closes `docs/handoffs/governance_and_access_control.md` item 2 for
the Databricks path; the local SQLite backend gets its own lightweight
`audit_query_log` table instead (every `LocalWarehouse.query()` call logs
`queried_at` / a hash of the SQL text / an optional `caller` label — see
`io/local_store.py`).

- Enable/verify system tables (`system.access.audit`) are turned on for the
  workspace — this is a workspace-level admin setting, not something the
  Asset Bundle controls.
- Decide retention: system tables retain audit logs for a fixed window by
  default (verify current retention in the workspace admin console — this
  changes over time); export to a longer-retention table first if
  compliance needs longer than the default.
- Decide who can read the audit log itself — usually a narrower group than
  who can read Gold; a grants question, same shape as A8 above.

- [ ] System tables / audit logging enabled for the workspace
- [ ] Retention window confirmed against compliance requirements
- [ ] Audit-log read access scoped (narrower than Gold read access)

---

# Part B — Configuration decisions (values to choose)

Set via env var, the (git-ignored) `config/config.yaml`, or bundle/job params.
Precedence: explicit arg > env var > config file > default. See the full
env-var table in the README.

| Setting | Config key | Default | Decision |
|---|---|---|---|
| Target catalog | `--env` | `dev` → `macro_dev` | promotion path |
| FRED request rate | `rate_limit_per_minute` | `120/min` | keep unless FRED tier differs |
| BLS / EIA / ECB request rate | client default | `25` / `60` / `60` per min | tune to your quota (BLS cap is **daily**) |
| Restate window | `restate_last_n` | `90` obs | global incremental re-pull depth |
| Complete vintage history | `complete_vintage_history` | `false` | `true` = full ALFRED backfill (heavier) |
| Extract concurrency | `extract_workers` | `8` | threads sharing the rate limiter |
| Alert level | `notify_on` | `failure` | `never` / `failure` / `always` |
| Alert webhook | `alert_webhook_url` | — | Slack-compatible URL |
| Raw archive path | `raw_volume_path` | `/Volumes/<catalog>/bronze/raw` | override only if non-default |
| Secret scope / key | `secret_scope` / `secret_key` | `fred` / `api_key` | change only if your scope differs |

---

# Part C — Quant decisions (data policy)

These are domain judgments the pipeline exposes but does **not** decide. Most are
per-series manifest fields under `manifests/` (validated by
`manifests/manifest.schema.json`).

### Q1. Source activation
- [ ] Which of the ~2,300 FRED series stay `active: true`?
- [ ] Activate any of the inactive demo manifests? `bls_labor.yml`,
      `eia_energy.yml`, `treasury_fiscal.yml`,
      `worldbank_global.yml`, `bea_national_accounts.yml`,
      `census_indicators.yml`, `sec_financials.yml` are all `active: false`
      today. EIA and BEA **require** a key (A3); ECB / Treasury / World Bank /
      Census / SEC are keyless (SEC needs a User-Agent); `ecb_rates.yml` ships
      active with nine verified starter FX and rates series.
      For SEC at scale, generate the manifest with
      `fred_pipeline.sources.sec.build_sec_manifest` rather than by hand.
- [ ] **Verify the remaining demo series IDs live** once keys exist (blocked in
      the build env by egress). Quick check: `python -m fred_pipeline run
      --dry-run --manifests manifests/eia_energy.yml` after setting
      `active: true`.

### Q2. Per-series data policy (manifest fields)
| Field | Decision | Notes |
|---|---|---|
| `vintage_enabled` | revision-sensitive series → `true` (default) | captures ALFRED point-in-time; `false` for never-revised market series |
| `validation_profile` | `strict` / `standard` / `lenient` | `strict` fails the series on any DQ breach; `standard` warns |
| `min_value` / `max_value` | sane bounds where known | e.g. rates in a plausible band, non-negative prices |
| `restate_records` | per-series override of `restate_last_n` | larger for deeply-revised series (GDP, payrolls) |
| `expected_update_frequency` | for freshness/staleness checks | e.g. monthly / weekly |
| `priority`, `*_owner`, `downstream_use_case` | governance metadata | ownership + lineage |

### Q3. Derived features
- [ ] `config/spreads.yml`: which same-frequency 2-leg spread/ratio pairs beyond
      the original Treasury curve set (e.g. credit spreads, PCE−CPI)?
- [ ] `config/cross_series.yml`: which **cross-frequency / cross-source /
      composite** features (e.g. debt-to-GDP, weighted activity indices), and
      their weights? Mechanism is built; **the feature set is a quant call.**

### Q4. Sign-off
- [ ] Approve DQ rules / validation profiles
- [ ] Validate Gold outputs (latest, point-in-time, feature matrix, spreads,
      revision stats) against expectations

---

# Part D — Go-live sequence

1. **Local dry run** (no workspace): `FRED_API_KEY=… python -m fred_pipeline run
   --dry-run` → confirms extraction + DQ against the live API.
2. **Provision dev** (A1–A3), deploy bundle (A6), run `fred_ingestion` once.
3. **Acceptance on dev** (Part E).
4. **Decision: job model** (single vs. per-source, A7/D-note) and **finalize the
   active series universe** (Part C) via manifest PRs.
5. **Promote to test → prod** (A6 with `-t test` / `-t prod`); set prod
   `run_as` service principal; apply grants (A8) and alerts (A9).
6. **Unpause schedules** (A7) once a manual prod run is clean.

> One-job-per-source note: if adopting `source_jobs.yml`, scope `fred_ingestion`
> to FRED-only manifests (e.g. move BLS/EIA manifests to a subfolder and point
> each job at its own path) so a series isn't processed by two jobs.

---

# Part E — Acceptance checks (per environment)

Run after the first ingestion in an environment:

- [ ] `audit.etl_run` has a row with status `succeeded` (or `partial` with a
      known reason); `audit.etl_series_run` covers every active series.
- [ ] `audit.data_quality_result` shows expected checks; no unexpected failures.
- [ ] `silver.fred_observation` populated; spot-check `source` values and, for a
      vintage series, multiple `realtime_start` rows per `observation_date`.
- [ ] Gold tables built: `gold.fred_latest_observation`,
      `gold.fred_point_in_time`, `gold.fred_macro_feature_daily`,
      `gold.fred_feature_transforms`, `gold.fred_curve_spread`,
      `gold.fred_cross_series_feature`, `gold.fred_cross_series_feature_pit`,
      `gold.fred_source_reconciliation`, `gold.fred_company_fundamentals`,
      `gold.fred_company_ratios`, `gold.fred_revision_stats`; views
      `gold.v_latest_revised` / `gold.v_point_in_time` / `gold.v_source_coverage`
      / `gold.v_company_ratio_ranks` resolve.
- [ ] Coverage view sanity: `SELECT source, count(*), sum(is_stale) FROM
      gold.v_source_coverage GROUP BY source` — no unexpected stale series.
- [ ] Metadata governance: run `reconcile` (FRED series only) and review
      `meta.fred_series_drift` / `meta.fred_series_lifecycle`.
- [ ] A failure alert fires (test the webhook / email path).

---

## Appendix — secrets & env vars quick reference

**Secret scope `fred`:** `api_key` (FRED), `bls_api_key`, `eia_api_key`.

**Key env vars** (12-factor overrides; full list in the README):
`FRED_API_KEY`, `BLS_API_KEY`, `EIA_API_KEY`, `FRED_RATE_LIMIT_PER_MINUTE`,
`FRED_RESTATE_LAST_N`, `FRED_COMPLETE_VINTAGE_HISTORY`, `FRED_EXTRACT_WORKERS`,
`FRED_NOTIFY_ON`, `FRED_ALERT_WEBHOOK_URL`, `FRED_CONFIG_FILE`.

**Not built (optional future work, not required for go-live):** per-source
metadata reconciliation for BLS/EIA/ECB (reconcile is FRED-only and skips other
sources); per-frequency EIA incremental windowing; multi-series batching per
source. See `docs/adding_a_source.md`.

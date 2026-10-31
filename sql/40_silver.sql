-- Silver layer: normalized, deduplicated observations.
-- Parameterize {{catalog}}. Column names mirror fred_pipeline.transform.SILVER_COLUMNS
-- plus revision_number added by transform.assign_revision_numbers.
-- Natural key (MERGE key): (source, series_id, observation_date, realtime_start).

USE CATALOG {{catalog}};

CREATE TABLE IF NOT EXISTS silver.fred_observation (
    source           STRING  NOT NULL,   -- upstream API: 'fred', 'bls', ...
    series_id        STRING  NOT NULL,
    observation_date DATE    NOT NULL,
    realtime_start   DATE,
    realtime_end     DATE,
    value            DOUBLE,
    raw_value        STRING,   -- original upstream string ('.' preserved as-is)
    exact_value      STRING,   -- canonical Decimal string for precision-sensitive sources
    is_missing       BOOLEAN,
    row_hash         STRING,   -- sha256 change-detection hash
    revision_number  INT,      -- 1..N per (series_id, observation_date)
    provider_valid_from STRING,
    provider_valid_to   STRING,
    base_currency     STRING,
    quote_currency    STRING,
    unit_multiplier   INT,
    observation_status STRING,
    observation_action STRING,
    is_deleted        BOOLEAN,
    observation_event_id STRING,
    ingested_at      TIMESTAMP,
    run_id           STRING
)
USING DELTA
PARTITIONED BY (series_id)
COMMENT 'Normalized observations (multi-source), idempotent via MERGE on natural key'
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.enableChangeDataFeed = true
);

-- Append-only observed state ledger.  This is deliberately separate from the
-- compact current-state table above: captured-as-of consumers must retain an
-- A -> B -> A correction sequence and source withdrawals.
CREATE TABLE IF NOT EXISTS silver.fred_observation_history (
    observation_event_id STRING NOT NULL,
    source STRING NOT NULL, series_id STRING NOT NULL, observation_date DATE NOT NULL,
    realtime_start DATE, realtime_end DATE, value DOUBLE, raw_value STRING,
    exact_value STRING, is_missing BOOLEAN, row_hash STRING, revision_number INT,
    provider_valid_from STRING, provider_valid_to STRING,
    base_currency STRING, quote_currency STRING, unit_multiplier INT,
    observation_status STRING, observation_action STRING, is_deleted BOOLEAN,
    ingested_at TIMESTAMP NOT NULL, run_id STRING
)
USING DELTA
PARTITIONED BY (series_id)
COMMENT 'Immutable observed normalized states for captured-as-of reconstruction'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- spec011 accepted releases and compact point-in-time security history.
CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_release (
    release_id STRING NOT NULL, release_url STRING,
    release_at_local STRING, publication_at_local STRING,
    publication_at_utc TIMESTAMP, rss_pub_at TIMESTAMP,
    prior_release_id STRING, is_consecutive BOOLEAN,
    full_count BIGINT, update_count BIGINT,
    new_count BIGINT, updated_count BIGINT, deleted_count BIGINT,
    parser_version STRING, source_schema_hash STRING, dictionary_hash STRING,
    reconciliation_status STRING, coverage_status STRING,
    timestamp_precision STRING, status STRING,
    accepted_at TIMESTAMP, run_id STRING
)
USING DELTA
COMMENT 'Accepted ECB eligible-assets releases and point-in-time coverage';

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_version (
    asset_key STRING NOT NULL, isin_code STRING, other_reg_number STRING,
    is_eligible BOOLEAN,
    haircut_category STRING, asset_type STRING, reference_market STRING,
    denomination STRING, issuance_date DATE, maturity_date DATE,
    issuer_csd STRING, coupon_definition STRING, coupon_rate DECIMAL(20,8),
    issuer_name STRING, issuer_group STRING, issuer_residence STRING,
    guarantor_name STRING, guarantor_group STRING, guarantor_residence STRING,
    non_own_use_haircut DECIMAL(20,8), own_use_haircut DECIMAL(20,8),
    potentially_own_usable_covered_bond STRING, climate_factor STRING,
    source_fields_json STRING, source_schema_version STRING,
    dictionary_hash STRING, row_hash STRING,
    change_type STRING, change_origin STRING,
    valid_from_release_id STRING, valid_from_utc TIMESTAMP,
    valid_to_release_id STRING, valid_to_utc TIMESTAMP,
    ingested_at TIMESTAMP, run_id STRING
)
USING DELTA
COMMENT 'Half-open SCD2 intervals for securities listed as eligible by the ECB'
TBLPROPERTIES (delta.enableChangeDataFeed = true);

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_change (
    release_id STRING NOT NULL, asset_key STRING NOT NULL,
    declared_change_type STRING, computed_change_type STRING,
    prior_row_hash STRING, new_row_hash STRING,
    reconciliation_status STRING, change_origin STRING,
    publication_at_utc TIMESTAMP, run_id STRING
)
USING DELTA
COMMENT 'ECB-declared and full-snapshot-computed security changes';

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_dictionary (
    dictionary_hash STRING NOT NULL, domain STRING NOT NULL, code STRING NOT NULL,
    definition STRING, short_definition STRING, attributes_json STRING,
    release_id STRING, ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Versioned ECB eligible-assets code dictionary';

-- spec011 Phase 2: run-scoped historical rebuild workspace. Promotion reads
-- only a completed run and never mutates these evidence tables.
CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_backfill_run (
    run_id STRING NOT NULL,
    requested_from_date DATE, requested_to_date DATE,
    registry_sha256 STRING, status STRING,
    snapshot_count BIGINT, missing_count BIGINT, quarantined_count BIGINT,
    version_count BIGINT, change_count BIGINT, final_asset_count BIGINT,
    started_at TIMESTAMP, updated_at TIMESTAMP, completed_at TIMESTAMP,
    error_detail STRING
)
USING DELTA
COMMENT 'Run ledger for isolated ECB historical rebuilds';

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_release_stage (
    run_id STRING NOT NULL, release_id STRING NOT NULL, snapshot_date DATE,
    publication_at_local STRING, publication_at_utc TIMESTAMP,
    timestamp_precision STRING, prior_release_id STRING,
    is_consecutive BOOLEAN, full_count BIGINT,
    new_count BIGINT, updated_count BIGINT, deleted_count BIGINT,
    parser_version STRING, source_schema_hash STRING,
    reconciliation_status STRING, coverage_status STRING, status STRING,
    source_url STRING, source_entry_path STRING,
    source_entry_sha256 STRING, archive_sha256 STRING,
    error_detail STRING, staged_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_id)
COMMENT 'Run-scoped ECB archive release and coverage staging ledger';

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_version_stage (
    run_id STRING NOT NULL, asset_key STRING NOT NULL,
    isin_code STRING, other_reg_number STRING, is_eligible BOOLEAN,
    haircut_category STRING, asset_type STRING, reference_market STRING,
    denomination STRING, issuance_date DATE, maturity_date DATE,
    issuer_csd STRING, coupon_definition STRING, coupon_rate DECIMAL(20,8),
    issuer_name STRING, issuer_group STRING, issuer_residence STRING,
    guarantor_name STRING, guarantor_group STRING, guarantor_residence STRING,
    non_own_use_haircut DECIMAL(20,8), own_use_haircut DECIMAL(20,8),
    potentially_own_usable_covered_bond STRING, climate_factor STRING,
    source_fields_json STRING, source_schema_version STRING, row_hash STRING,
    change_type STRING, change_origin STRING,
    valid_from_release_id STRING, valid_from_utc TIMESTAMP,
    valid_to_release_id STRING, valid_to_utc TIMESTAMP,
    staged_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_id)
COMMENT 'Run-scoped compact SCD2 ECB archive history before promotion';

CREATE TABLE IF NOT EXISTS silver.ecb_eligible_asset_change_stage (
    run_id STRING NOT NULL, release_id STRING NOT NULL, asset_key STRING NOT NULL,
    computed_change_type STRING, prior_row_hash STRING, new_row_hash STRING,
    reconciliation_status STRING, change_origin STRING,
    publication_at_utc TIMESTAMP, staged_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_id)
COMMENT 'Run-scoped ECB archive changes before promotion';

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
    is_missing       BOOLEAN,
    row_hash         STRING,   -- sha256 change-detection hash
    revision_number  INT,      -- 1..N per (series_id, observation_date)
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

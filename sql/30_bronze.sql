-- Bronze layer: raw API payload retention (multi-source).
-- Parameterize {{catalog}}. Column names mirror fred_pipeline.bronze.

USE CATALOG {{catalog}};

CREATE TABLE IF NOT EXISTS bronze.fred_api_response (
    run_id            STRING NOT NULL,
    source            STRING NOT NULL,   -- upstream API: 'fred', 'bls', 'eia', ...
    series_id         STRING NOT NULL,
    endpoint          STRING,            -- the source endpoint actually called
    request_params    STRING,   -- JSON string of request params (api_key redacted)
    response_payload  STRING,   -- verbatim upstream JSON response
    observation_count INT,
    payload_bytes     INT,
    ingested_at       TIMESTAMP
)
USING DELTA
PARTITIONED BY (series_id)
COMMENT 'Raw, verbatim API responses, multi-source (system of record)'
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true
);

-- spec011 ECB MID release and immutable attachment evidence.
CREATE TABLE IF NOT EXISTS bronze.ecb_eligible_asset_release (
    release_id STRING NOT NULL,
    release_url STRING NOT NULL,
    release_type STRING,
    release_title STRING,
    release_at_local STRING,
    publication_at_local STRING,
    publication_at_utc TIMESTAMP,
    rss_pub_at TIMESTAMP,
    discovered_at TIMESTAMP,
    retrieved_at TIMESTAMP,
    status STRING,
    run_id STRING,
    error_detail STRING
)
USING DELTA
COMMENT 'ECB MID eligible-assets release ledger; only accepted rows feed Silver';

CREATE TABLE IF NOT EXISTS bronze.ecb_eligible_asset_artifact (
    release_id STRING NOT NULL,
    artifact_kind STRING NOT NULL,
    file_name STRING,
    source_url STRING,
    content_type STRING,
    encoding STRING,
    compressed_bytes BIGINT,
    uncompressed_bytes BIGINT,
    sha256 STRING,
    md5 STRING,
    signature_raw STRING,
    signature_status STRING,
    signer_subject STRING,
    verified_at TIMESTAMP,
    archive_path STRING,
    payload BINARY,
    retrieved_at TIMESTAMP,
    run_id STRING
)
USING DELTA
COMMENT 'Exact signed ECB MID attachment bytes and verification evidence';

-- Analytical views over Silver: the two canonical lenses from the handoff doc.
-- Parameterize {{catalog}}.

USE CATALOG {{catalog}};

-- latest_revised: current best estimate of each observation (values as revised).
CREATE OR REPLACE VIEW gold.v_latest_revised AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, observation_date
            ORDER BY realtime_start DESC
        ) AS rn
    FROM silver.fred_observation
)
SELECT series_id, observation_date, value, realtime_start, realtime_end,
       is_missing, revision_number, ingested_at
FROM ranked
WHERE rn = 1;

-- point_in_time: every vintage row. Query with a real-time filter, e.g.:
--   SELECT * FROM gold.v_point_in_time
--   WHERE realtime_start <= DATE '2020-06-30'
--     AND (realtime_end   >= DATE '2020-06-30' OR realtime_end IS NULL);
CREATE OR REPLACE VIEW gold.v_point_in_time AS
SELECT series_id, observation_date, realtime_start, realtime_end, value,
       revision_number, is_missing, ingested_at
FROM silver.fred_observation;

-- Convenience: latest value per series (for dashboards / spot checks).
CREATE OR REPLACE VIEW gold.v_series_latest_value AS
WITH latest AS (
    SELECT
        series_id,
        observation_date,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id ORDER BY observation_date DESC
        ) AS rn
    FROM gold.v_latest_revised
    WHERE is_missing = false
)
SELECT series_id, observation_date, value
FROM latest
WHERE rn = 1;

-- Per-series revision-magnitude summary (rolled up from gold.fred_revision_stats):
-- how much a series' observations typically get revised, and how often.
-- Useful for judging how much to trust a series' initial print (e.g.
-- GDP/payrolls are heavily revised; market/price series usually are not).
CREATE OR REPLACE VIEW gold.v_series_revision_summary AS
SELECT series_id,
    COUNT(*)                       AS observation_count,
    AVG(revision_count)            AS avg_revision_count,
    MAX(revision_count)            AS max_revision_count,
    AVG(ABS(revision_pct))         AS avg_abs_revision_pct,
    MAX(ABS(revision_pct))         AS max_abs_revision_pct
FROM gold.fred_revision_stats
GROUP BY series_id;

-- Multi-source coverage & freshness dashboard: latest observation, count, and a
-- staleness verdict per (source, series_id), using the manifest cadence from
-- meta.fred_series. Mirrors gold_v_source_coverage in local_store.py.
CREATE OR REPLACE VIEW gold.v_source_coverage AS
WITH per_series AS (
    SELECT source, series_id,
           MAX(observation_date)            AS latest_observation_date,
           COUNT(DISTINCT observation_date) AS observation_count
    FROM silver.fred_observation
    GROUP BY source, series_id
),
aged AS (
    SELECT p.source, p.series_id, m.category, m.frequency,
           p.latest_observation_date, p.observation_count,
           DATEDIFF(current_date(), p.latest_observation_date) AS days_since_last
    FROM per_series p
    LEFT JOIN meta.fred_series m ON m.series_id = p.series_id
)
SELECT source, series_id, category, frequency, latest_observation_date,
       observation_count, days_since_last,
       CASE
         WHEN frequency IN ('d','daily')       AND days_since_last > 10  THEN true
         WHEN frequency IN ('w','weekly')      AND days_since_last > 21  THEN true
         WHEN frequency IN ('bw','biweekly')   AND days_since_last > 30  THEN true
         WHEN frequency IN ('m','monthly')     AND days_since_last > 75  THEN true
         WHEN frequency IN ('q','quarterly')   AND days_since_last > 200 THEN true
         WHEN frequency IN ('sa','semiannual') AND days_since_last > 380 THEN true
         WHEN frequency IN ('a','annual')      AND days_since_last > 550 THEN true
         ELSE false
       END AS is_stale
FROM aged;

-- Cross-company ranks/percentiles of each SEC-derived ratio within each period
-- (pct_rank 0..1 ascending; rank_desc = 1 is the largest). Mirrors
-- gold_v_company_ratio_ranks in local_store.py.
CREATE OR REPLACE VIEW gold.v_company_ratio_ranks AS
SELECT cik, ratio_name, observation_date, value,
       PERCENT_RANK() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value
       ) AS pct_rank,
       ROW_NUMBER() OVER (
           PARTITION BY ratio_name, observation_date ORDER BY value DESC
       ) AS rank_desc
FROM gold.fred_company_ratios;

-- spec011 ECB eligible-market-assets consumer objects.
CREATE OR REPLACE VIEW gold.ecb_eligible_security_history AS
SELECT v.*,
       at.definition AS asset_type_label,
       hc.definition AS haircut_category_label,
       rm.definition AS reference_market_label,
       cd.definition AS coupon_definition_label
FROM silver.ecb_eligible_asset_version v
LEFT JOIN silver.ecb_eligible_asset_dictionary at
  ON at.dictionary_hash = v.dictionary_hash
 AND at.domain = 'assetTypes' AND at.code = v.asset_type
LEFT JOIN silver.ecb_eligible_asset_dictionary hc
  ON hc.dictionary_hash = v.dictionary_hash
 AND hc.domain = 'assetHaircutCategories' AND hc.code = v.haircut_category
LEFT JOIN silver.ecb_eligible_asset_dictionary rm
  ON rm.dictionary_hash = v.dictionary_hash
 AND rm.domain = 'assetReferenceMarkets' AND rm.code = v.reference_market
LEFT JOIN silver.ecb_eligible_asset_dictionary cd
  ON cd.dictionary_hash = v.dictionary_hash
 AND cd.domain = 'assetCouponDefinitions' AND cd.code = v.coupon_definition;

CREATE OR REPLACE VIEW gold.ecb_eligible_security_current AS
SELECT * FROM gold.ecb_eligible_security_history
WHERE valid_to_utc IS NULL AND is_eligible = true;

CREATE OR REPLACE VIEW gold.ecb_eligible_security_change AS
SELECT * FROM silver.ecb_eligible_asset_change;

CREATE OR REPLACE VIEW gold.ecb_eligible_security_coverage AS
SELECT * FROM silver.ecb_eligible_asset_release;

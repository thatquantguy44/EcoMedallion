-- MMKT snapshot validation for Postgres.
-- Expected configured instrument count: 14.

-- Summary: completeness, status counts, and observation coverage.
SELECT
    COUNT(*) AS instrument_rows,
    COUNT(DISTINCT instrument_id) AS distinct_instruments,
    COUNT(*) = 14 AND COUNT(DISTINCT instrument_id) = 14 AS has_all_14_instruments,
    COUNT(*) FILTER (WHERE status = 'ok') AS ok,
    COUNT(*) FILTER (WHERE status = 'stale') AS stale,
    COUNT(*) FILTER (WHERE status = 'missing') AS missing,
    MIN(observation_date) AS oldest_observation,
    MAX(observation_date) AS newest_observation,
    MAX(as_of_date) AS as_of
FROM gold.mmkt_rate_snapshot;

-- Per-instrument values and freshness details.
SELECT
    instrument_id,
    series_id,
    status,
    observation_date,
    as_of_date,
    staleness_days,
    max_staleness_days,
    quoted_basis,
    quoted_rate_pct,
    normalized_basis,
    mm_yield_pct,
    status_detail
FROM gold.mmkt_rate_snapshot
ORDER BY instrument_id;

-- Exceptions: non-OK rows, observations after the snapshot cutoff, or invalid statuses.
SELECT
    instrument_id,
    series_id,
    status,
    observation_date,
    as_of_date,
    staleness_days,
    status_detail
FROM gold.mmkt_rate_snapshot
WHERE status <> 'ok'
   OR status NOT IN ('ok', 'stale', 'missing')
   OR observation_date > as_of_date
ORDER BY instrument_id;

-- ============================================
-- SILVER: clean_events
-- Deduplicates, casts types, flags issues
-- ============================================

WITH source AS (
    SELECT * FROM raw.raw_events
),

-- Ensure one row per event_id (should already be unique, but defensive)
deduped AS (
    SELECT DISTINCT ON (event_id) *
    FROM source
    ORDER BY event_id, ingestion_timestamp DESC
)

SELECT
    event_id,
    event_date,
    event_timestamp,
    event_code,
    event_base_code,
    event_root_code,
    CAST(goldstein_scale AS DOUBLE)  AS goldstein_scale,
    CAST(num_mentions    AS INTEGER) AS num_mentions,
    CAST(num_articles    AS INTEGER) AS num_articles,
    CAST(avg_tone        AS DOUBLE)  AS avg_tone,
    actor1_code,
    actor1_name,
    actor2_code,
    actor2_name,
    action_geo_country_code,
    CAST(action_geo_lat   AS DOUBLE) AS action_geo_lat,
    CAST(action_geo_long  AS DOUBLE) AS action_geo_long,
    source_url,
    source_domain,
    CASE
        WHEN source_domain IS NULL OR source_domain = ''
        THEN TRUE
        ELSE FALSE
    END                              AS is_domain_missing,
    ingestion_timestamp,
    batch_id
FROM deduped
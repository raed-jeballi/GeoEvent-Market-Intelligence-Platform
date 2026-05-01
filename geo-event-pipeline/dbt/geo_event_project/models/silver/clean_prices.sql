-- ============================================
-- SILVER: clean_prices
-- Casts types, adds commodity name, flags NULLs
-- ============================================

WITH source AS (
    SELECT * FROM raw.raw_prices
),

-- Map tickers to human names
with_names AS (
    SELECT
        *,
        CASE commodity_code
            WHEN 'BZ=F' THEN 'brent_oil'
            WHEN 'GC=F' THEN 'gold'
            WHEN 'SI=F' THEN 'silver'
            WHEN 'NG=F' THEN 'natural_gas'
            ELSE commodity_code
        END AS commodity_name
    FROM source
)

SELECT
    price_id,
    commodity_code,
    commodity_name,
    timestamp,
    CAST(open AS DOUBLE) AS open,
    CAST(high AS DOUBLE) AS high,
    CAST(low AS DOUBLE) AS low,
    CAST(close AS DOUBLE) AS close,
    CAST(volume AS BIGINT) AS volume,
    interval,
    CASE
        WHEN close IS NULL THEN TRUE
        ELSE FALSE
    END 
    AS is_close_missing,
    ingestion_timestamp,
    batch_id
FROM with_names
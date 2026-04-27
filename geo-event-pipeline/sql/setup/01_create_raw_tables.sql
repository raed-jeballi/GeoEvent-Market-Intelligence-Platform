-- ============================================
-- RAW Layer Schema Setup
-- Medallion Architecture: BRONZE
-- ============================================

CREATE SCHEMA IF NOT EXISTS raw;

-- --------------------------------------------
-- 1. Raw GDELT Events
-- Source: GDELT 2.0 15-minute updates
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS raw.raw_events (
    event_id                VARCHAR PRIMARY KEY,
    event_date              DATE NOT NULL,
    event_timestamp         TIMESTAMP NOT NULL,
    event_root_code         VARCHAR,
    event_base_code         VARCHAR,
    event_code              VARCHAR,
    goldstein_scale         DOUBLE,
    avg_tone                DOUBLE,
    num_mentions            INTEGER,
    num_articles            INTEGER,
    actor1_code             VARCHAR,
    actor1_name             VARCHAR,
    actor2_code             VARCHAR,
    actor2_name             VARCHAR,
    action_geo_country_code VARCHAR,
    action_geo_country_name VARCHAR,
    action_geo_lat          DOUBLE,
    action_geo_long         DOUBLE,
    source_url              VARCHAR,
    source_domain           VARCHAR,
    ingestion_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id                VARCHAR NOT NULL
);

-- --------------------------------------------
-- 2. Raw Commodity Prices
-- Source: Yahoo Finance (yfinance)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS raw.raw_prices (
    price_id            VARCHAR PRIMARY KEY,
    commodity_code      VARCHAR NOT NULL,
    timestamp           TIMESTAMP NOT NULL,
    open                DOUBLE,
    high                DOUBLE,
    low                 DOUBLE,
    close               DOUBLE,
    volume              BIGINT,
    interval            VARCHAR NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id            VARCHAR NOT NULL
);

-- --------------------------------------------
-- 3. Pipeline Runs Metadata
-- Tracks every ingestion execution
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS raw.pipeline_runs (
    run_id           VARCHAR PRIMARY KEY,
    pipeline_name    VARCHAR NOT NULL,
    run_status       VARCHAR NOT NULL DEFAULT 'STARTED',
    start_time       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time         TIMESTAMP,
    rows_ingested    INTEGER DEFAULT 0,
    source_watermark VARCHAR,
    error_message    VARCHAR,
    trigger_type     VARCHAR NOT NULL
);

-- --------------------------------------------
-- 4. Data Quality Metrics
-- RAW-level quality checks
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS raw.data_quality_metrics (
    metric_id        VARCHAR PRIMARY KEY,
    check_timestamp  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    table_name       VARCHAR NOT NULL,
    metric_name      VARCHAR NOT NULL,
    metric_value     DOUBLE,
    threshold_value  DOUBLE,
    status           VARCHAR NOT NULL,
    batch_id         VARCHAR
);
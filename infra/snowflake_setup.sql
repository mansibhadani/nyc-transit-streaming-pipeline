-- ─────────────────────────────────────────────────────────────────────────────
--  Snowflake Setup Script
--  Run this once in a Snowflake worksheet before starting the pipeline.
--  Role required: ACCOUNTADMIN (for role/warehouse creation)
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Warehouse ─────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS TRANSIT_WH
    WAREHOUSE_SIZE   = XSMALL
    AUTO_SUSPEND     = 60        -- suspend after 60s idle (cost control)
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ── Database & schemas ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS NYC_TRANSIT;

CREATE SCHEMA IF NOT EXISTS NYC_TRANSIT.TRANSIT_RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TRANSIT.TRANSIT_STAGING;
CREATE SCHEMA IF NOT EXISTS NYC_TRANSIT.TRANSIT_CURATED;

-- ── Roles ─────────────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS TRANSFORMER;
CREATE ROLE IF NOT EXISTS REPORTER;

GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE REPORTER    TO ROLE SYSADMIN;

-- TRANSFORMER: full access to write + transform
GRANT USAGE  ON WAREHOUSE TRANSIT_WH         TO ROLE TRANSFORMER;
GRANT USAGE  ON DATABASE  NYC_TRANSIT         TO ROLE TRANSFORMER;
GRANT ALL    ON SCHEMA NYC_TRANSIT.TRANSIT_RAW     TO ROLE TRANSFORMER;
GRANT ALL    ON SCHEMA NYC_TRANSIT.TRANSIT_STAGING  TO ROLE TRANSFORMER;
GRANT ALL    ON SCHEMA NYC_TRANSIT.TRANSIT_CURATED  TO ROLE TRANSFORMER;
GRANT ALL    ON ALL TABLES IN DATABASE NYC_TRANSIT  TO ROLE TRANSFORMER;
GRANT ALL    ON FUTURE TABLES IN DATABASE NYC_TRANSIT TO ROLE TRANSFORMER;

-- REPORTER: read-only on curated layer
GRANT USAGE  ON WAREHOUSE TRANSIT_WH                    TO ROLE REPORTER;
GRANT USAGE  ON DATABASE  NYC_TRANSIT                    TO ROLE REPORTER;
GRANT USAGE  ON SCHEMA NYC_TRANSIT.TRANSIT_CURATED       TO ROLE REPORTER;
GRANT SELECT ON ALL TABLES IN SCHEMA NYC_TRANSIT.TRANSIT_CURATED TO ROLE REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA NYC_TRANSIT.TRANSIT_CURATED TO ROLE REPORTER;

-- ── Raw tables (written by PySpark) ───────────────────────────────────────────
USE SCHEMA NYC_TRANSIT.TRANSIT_RAW;

CREATE TABLE IF NOT EXISTS EVENTS (
    event_id            VARCHAR(36)     NOT NULL,
    route_id            VARCHAR(10)     NOT NULL,
    stop_id             VARCHAR(20)     NOT NULL,
    stop_name           VARCHAR(100),
    direction           VARCHAR(1),
    scheduled_ts        TIMESTAMP_NTZ,
    actual_ts           TIMESTAMP_NTZ,
    ingested_ts         TIMESTAMP_NTZ   NOT NULL,
    delay_seconds       INTEGER,
    delay_category      VARCHAR(20),
    vehicle_id          VARCHAR(20),
    status              VARCHAR(20)     NOT NULL,
    is_peak_hour        BOOLEAN,
    hour_of_day         INTEGER,
    day_of_week         INTEGER,
    date_local          DATE,
    processing_lag_sec  INTEGER,
    kafka_topic         VARCHAR(50),
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    dq_failure_reason   VARCHAR(100),   -- NULL for clean rows
    CONSTRAINT pk_events PRIMARY KEY (event_id)
)
CLUSTER BY (date_local, route_id)
COMMENT = 'Raw transit events written by PySpark Structured Streaming. Clean rows have dq_failure_reason = NULL.';

CREATE TABLE IF NOT EXISTS DEAD_LETTER (
    LIKE EVENTS
)
COMMENT = 'Records that failed data quality checks. Inspect dq_failure_reason for triage.';

CREATE TABLE IF NOT EXISTS DELAY_AGG_5MIN (
    window_start        TIMESTAMP_NTZ   NOT NULL,
    window_end          TIMESTAMP_NTZ   NOT NULL,
    route_id            VARCHAR(10)     NOT NULL,
    direction           VARCHAR(1),
    delayed_count       INTEGER,
    avg_delay_sec       FLOAT,
    max_delay_sec       INTEGER,
    p95_delay_sec       FLOAT,
    CONSTRAINT pk_delay_agg PRIMARY KEY (window_start, route_id, direction)
)
CLUSTER BY (window_start)
COMMENT = '5-minute tumbling window delay aggregations written by PySpark.';

-- ── Verification ─────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA NYC_TRANSIT.TRANSIT_RAW;
SHOW SCHEMAS IN DATABASE NYC_TRANSIT;

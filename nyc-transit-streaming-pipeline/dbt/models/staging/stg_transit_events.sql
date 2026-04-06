{{
  config(
    materialized = 'view',
    tags         = ['staging', 'transit']
  )
}}

/*
  stg_transit_events
  ------------------
  Thin cleansing layer directly on top of the raw Snowflake table
  populated by PySpark Structured Streaming.

  Responsibilities:
    - Cast / coerce types
    - Rename columns to snake_case standard
    - Filter out dead-letter records
    - No business logic — that lives in curated models
*/

with source as (

    select * from {{ source('transit_raw', 'EVENTS') }}

),

renamed as (

    select
        -- identifiers
        event_id,
        route_id,
        stop_id,
        stop_name,

        -- enums
        direction,
        status,
        delay_category,

        -- timing
        scheduled_ts                                     as scheduled_at,
        actual_ts                                        as actual_at,
        ingested_ts                                      as ingested_at,
        date_local,
        hour_of_day,
        day_of_week,
        is_peak_hour,

        -- delay metrics
        delay_seconds,
        processing_lag_sec,

        -- optional fields
        vehicle_id,

        -- kafka metadata (useful for debugging)
        topic                                            as kafka_topic,
        partition                                        as kafka_partition,
        offset                                           as kafka_offset

    from source
    where dq_failure_reason is null     -- exclude dead-letter rows

),

deduped as (

    -- belt-and-suspenders dedup on top of Spark-level dedup
    select *
    from renamed
    qualify row_number() over (
        partition by event_id
        order by ingested_at desc
    ) = 1

)

select * from deduped

{{
  config(
    materialized  = 'table',
    tags          = ['curated', 'transit', 'timeseries'],
    cluster_by    = ['date_local']
  )
}}

/*
  hourly_delay_agg
  ----------------
  Hourly aggregation across all routes.
  Powers the "Delay Trend" time-series chart in Metabase.

  Grain: one row per (date_local, hour_of_day)
*/

with events as (

    select * from {{ ref('stg_transit_events') }}

),

hourly as (

    select
        date_local,
        hour_of_day,
        is_peak_hour,

        count(*)                                                         as total_events,
        countif(status = 'DELAYED')                                      as delayed_count,
        countif(status = 'CANCELLED')                                    as cancelled_count,
        round(countif(status = 'ON_TIME') / nullif(count(*), 0) * 100, 2) as on_time_pct,
        round(avg(case when delay_seconds > 0 then delay_seconds end), 1) as avg_delay_sec,
        max(delay_seconds)                                                as max_delay_sec,
        count(distinct route_id)                                         as active_routes,

        -- worst route each hour
        mode(route_id) within group (order by delay_seconds desc)        as most_delayed_route

    from events
    group by 1, 2, 3

)

select * from hourly
order by date_local, hour_of_day

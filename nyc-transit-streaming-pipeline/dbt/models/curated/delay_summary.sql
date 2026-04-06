{{
  config(
    materialized  = 'table',
    tags          = ['curated', 'transit', 'delay'],
    cluster_by    = ['date_local', 'route_id']
  )
}}

/*
  delay_summary
  -------------
  Daily per-route delay statistics.
  Powers the "Route Performance" dashboard page in Metabase.

  Grain: one row per (date_local, route_id, direction)
*/

with events as (

    select * from {{ ref('stg_transit_events') }}

),

daily_stats as (

    select
        date_local,
        route_id,
        direction,

        -- volume
        count(*)                                             as total_events,
        countif(status = 'DELAYED')                         as delayed_events,
        countif(status = 'ON_TIME')                         as on_time_events,
        countif(status = 'CANCELLED')                       as cancelled_events,

        -- on-time rate
        round(
            countif(status = 'ON_TIME') / nullif(count(*), 0) * 100, 2
        )                                                    as on_time_pct,

        -- delay distribution (seconds)
        round(avg(case when delay_seconds > 0 then delay_seconds end), 1)
                                                             as avg_delay_sec,
        max(delay_seconds)                                   as max_delay_sec,
        min(delay_seconds)                                   as min_delay_sec,

        -- percentiles
        percentile_cont(0.50) within group (order by delay_seconds)
                                                             as median_delay_sec,
        percentile_cont(0.95) within group (order by delay_seconds)
                                                             as p95_delay_sec,

        -- peak vs off-peak breakdown
        countif(status = 'DELAYED' and is_peak_hour)        as peak_delayed_events,
        countif(status = 'DELAYED' and not is_peak_hour)    as offpeak_delayed_events,

        -- severity breakdown
        countif(delay_category = 'MINOR')                   as minor_delays,
        countif(delay_category = 'MODERATE')                as moderate_delays,
        countif(delay_category = 'SIGNIFICANT')             as significant_delays,
        countif(delay_category = 'SEVERE')                  as severe_delays

    from events
    group by 1, 2, 3

)

select
    *,
    -- rank routes by on_time_pct within each date (worst = rank 1)
    rank() over (
        partition by date_local
        order by on_time_pct asc
    )                                                        as worst_route_rank
from daily_stats

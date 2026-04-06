{{
  config(
    materialized = 'table',
    description  = 'Daily route-level performance metrics — used by Metabase dashboard'
  )
}}

with events as (
    select * from {{ ref('stg_transit_events') }}
),

metrics as (
    select
        date_partition,
        route_id,
        count(*)                                                         as total_events,
        count(case when status = 'ON_TIME'   then 1 end)                 as on_time_count,
        count(case when status = 'DELAYED'   then 1 end)                 as delayed_count,
        count(case when status = 'CANCELLED' then 1 end)                 as cancelled_count,
        round(100.0 * count(case when status = 'ON_TIME' then 1 end)
              / nullif(count(*), 0), 2)                                  as on_time_pct,
        round(avg(case when delay_seconds > 0 then delay_seconds end), 1) as avg_delay_seconds,
        max(delay_seconds)                                               as max_delay_seconds,
        count(distinct trip_id)                                          as unique_trips,
        count(case when is_peak_hour then 1 end)                         as peak_hour_events
    from events
    group by 1, 2
)

select
    *,
    round(avg(on_time_pct) over (
        partition by route_id
        order by date_partition
        rows between 6 preceding and current row
    ), 2) as on_time_pct_7d_avg
from metrics
order by date_partition desc, delayed_count desc

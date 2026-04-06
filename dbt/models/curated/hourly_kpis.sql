-- hourly_kpis.sql
-- System-wide KPI metrics per hour — for the top KPI cards in the dashboard.

with base as (
    select * from {{ ref('stg_transit_events') }}
)

select
    event_date,
    event_hour,
    hour_of_day,

    count(*)                                                as total_events,
    count(distinct route_id)                                as active_routes,
    count(distinct trip_id)                                 as unique_trips,
    sum(is_delayed::integer)                                as total_delayed_events,
    round(avg(delay_seconds), 1)                            as system_avg_delay_seconds,
    max(delay_seconds)                                      as system_max_delay_seconds,
    round(
        sum(is_delayed::integer) / nullif(count(*), 0) * 100,
        2
    )                                                       as system_delay_rate_pct,
    count(case when event_type = 'CANCELLATION' then 1 end) as cancellations,
    current_timestamp()                                     as dbt_updated_at

from base
group by 1, 2, 3

{{
  config(
    materialized = 'table',
    description  = 'Top 20 worst-performing stops by avg delay — refreshed daily'
  )
}}

select
    date_partition,
    stop_id,
    stop_name,
    route_id,
    count(*)                                                          as total_events,
    count(case when status in ('DELAYED', 'CANCELLED') then 1 end)   as problem_events,
    round(100.0 * count(case when status in ('DELAYED','CANCELLED') then 1 end)
          / nullif(count(*), 0), 1)                                   as problem_rate_pct,
    round(avg(nullif(delay_seconds, 0)), 1)                           as avg_delay_seconds,
    max(delay_seconds)                                                as max_delay_seconds,
    row_number() over (
        partition by date_partition
        order by avg(nullif(delay_seconds, 0)) desc nulls last
    )                                                                 as delay_rank
from {{ ref('stg_transit_events') }}
where status in ('DELAYED', 'CANCELLED')
group by 1, 2, 3, 4
having count(*) >= 5
qualify delay_rank <= 20
order by date_partition desc, delay_rank

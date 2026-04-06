-- Fail if curated delay_summary has gaps > 2 hours for the current day
-- (indicates streaming pipeline may have stalled)
with hourly_gaps as (
    select
        date_partition,
        hour_of_day,
        lag(hour_of_day) over (partition by date_partition order by hour_of_day) as prev_hour
    from {{ ref('delay_summary') }}
    where date_partition = current_date()
)
select count(*) as gap_count
from hourly_gaps
where (hour_of_day - coalesce(prev_hour, hour_of_day)) > 2
having count(*) > 0

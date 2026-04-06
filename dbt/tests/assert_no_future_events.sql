-- Fail if any events are scheduled more than 1 hour in the future
-- (indicates a timestamp bug in the producer)
select count(*) as future_event_count
from {{ ref('stg_transit_events') }}
where scheduled_at > dateadd(hour, 1, current_timestamp())
having count(*) > 0

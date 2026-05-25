-- Fails if any mart row has zero or negative counts (sanity only).
select *
from {{ ref('fct_daily_events') }}
where event_count <= 0

with events as (
    select * from {{ ref('stg_events') }}
)

select
    date_trunc('day', occurred_at)::date as event_date,
    event_type,
    count(*) as event_count,
    count(distinct user_id) as distinct_users
from events
group by 1, 2
order by 1, 2

with source as (
    select * from {{ source('raw', 'events') }}
)

select
    event_id,
    event_type,
    occurred_at,
    user_id,
    payload,
    ingested_at
from source

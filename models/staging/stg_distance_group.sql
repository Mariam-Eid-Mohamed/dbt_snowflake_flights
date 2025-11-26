with source as (
    select *
    from {{ source('raw_flights', 'L_DISTANCE_GROUP') }}
)

select
    code::int           as distance_group_id,
    description         as distance_group_desc
from source

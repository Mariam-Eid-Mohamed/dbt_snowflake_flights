with source as (
    select *
    from {{ source('raw_flights', 'L_AIRLINE_ID') }}
)

select
    code::int       as airline_dot_id, --maps to flights DOT_ID_REPORTING_AIRLINE
    description     as airline_dot_name
from source

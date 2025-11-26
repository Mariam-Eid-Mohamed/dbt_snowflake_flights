with source as (
    select *
    from {{ source('raw_flights', 'L_UNIQUE_CARRIERS') }}
)

select
    code            as airline_code, --maps to airline_code in flights 
    description     as airline_name
from source

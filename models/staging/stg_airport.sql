with source as (
    select *
    from {{ source('raw_flights', 'L_AIRPORT') }}
)

select
    code::string               as airport_id,-- maps to origin_airport  dest_airport in flights table
    description         as airport_name
from source

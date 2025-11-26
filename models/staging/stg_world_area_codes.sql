with source as (
    select *
    from {{ source('raw_flights', 'L_WORLD_AREA_CODES') }}
)

select
    code::int       as wac_code,     -- maps to origin_wac / dest_wac
    description     as wac_name
from source
with source as (
    select *
    from {{ source('raw_flights', 'L_CARRIER_HISTORY') }}
),

cleaned as (
    select
        code                                as airline_code,
        split_part(description, '(', 1)     as airline_name,
        regexp_substr(description, '\\d{4}', 1, 1)   as active_from_year,
        regexp_substr(description, '\\d{4}', 1, 2)   as active_to_year
    from source
)

select * from cleaned

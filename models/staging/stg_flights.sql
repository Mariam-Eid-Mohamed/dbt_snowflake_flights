with source as (
    select *
    from {{ source('raw_flights', 'FLIGHTS') }}
),

renamed as (
    select
        -- date fields
        FLIGHTDATE::date                  as flight_date,
        YEAR::int                         as year,
        QUARTER::int                      as quarter,
        MONTH::int                        as month,
        DAYOFMONTH::int                   as day_of_month,
        DAYOFWEEK::int                    as day_of_week,

        -- airport ids / codes
        ORIGINAIRPORTID                   as origin_airport_id,
        DESTAIRPORTID                     as dest_airport_id,
        ORIGIN                            as origin_airport_code,
        DEST                              as dest_airport_code,

        -- origin geography
        ORIGINCITYNAME                    as origin_city_name,
        ORIGINSTATE                       as origin_state_code,
        ORIGINSTATENAME                   as origin_state_name,
        ORIGINWAC                         as origin_wac,

        -- destination geography
        DESTCITYNAME                      as dest_city_name,
        DESTSTATE                         as dest_state_code,
        DESTSTATENAME                     as dest_state_name,
        DESTWAC                           as dest_wac,

        -- airline keys
        REPORTING_AIRLINE                 as airline_code,
        DOT_ID_REPORTING_AIRLINE          as dot_airline_id,
        IATA_CODE_REPORTING_AIRLINE       as iata_airline_code,
        TAIL_NUMBER                       as tail_number,

        -- flight identifiers
        FLIGHT_NUMBER_REPORTING_AIRLINE   as flight_number,

        -- cancellation / diversion
        CANCELLED::int                    as cancelled,
        DIVERTED::int                     as diverted,

        -- distance
        DISTANCE::int                     as distance,
        DISTANCEGROUP::int                as distance_group,
        FLIGHTS::int                      as num_of_flights,

        -- schedule / performance (add more later if you want)
        CRSDEPTIME                        as crs_dep_time,
        CRSARRTIME                        as crs_arr_time,
        CRSELAPSEDTIME                    as crs_elapsed_time

    from source
)

select *
from renamed

{{ config(materialized = 'table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('stg_flights') }}
),

joined AS (
    SELECT
        -- date key
        TO_NUMBER(TO_CHAR(base.flight_date, 'YYYYMMDD')) AS date_id,

        -- airline key
        air.dot_airline_id,

        -- origin airport key
        origin_airport.airport_id AS origin_airport_id,

        -- destination airport key
        dest_airport.airport_id   AS dest_airport_id,

        -- distance group lookup
        dg.distance_group_desc,

        -- measures
        base.num_of_flights,
        base.distance,
        base.distance_group,
        base.cancelled,
        base.diverted,
        base.crs_dep_time,
        base.crs_arr_time,
        base.crs_elapsed_time,

        -- identifiers
        base.flight_date,
        base.flight_number,
        base.tail_number

    FROM base
    LEFT JOIN {{ ref('dim_airline') }} air
      ON base.dot_airline_id = air.dot_airline_id
     

    LEFT JOIN {{ ref('dim_airport') }} origin_airport
      ON base.origin_airport_id = origin_airport.airport_id

    LEFT JOIN {{ ref('dim_airport') }} dest_airport
      ON base.dest_airport_id   = dest_airport.airport_id

    LEFT JOIN {{ ref('stg_distance_group') }} dg        
      ON base.distance_group = dg.distance_group_id
)

SELECT
    -- Natural Primary Key instead of surrogate key
    date_id,
    flight_number,
    origin_airport_id AS origin_airport_sk,
    dest_airport_id   AS dest_airport_sk,

    dot_airline_id,

    flight_date,
    tail_number,

    num_of_flights,
    distance,
    distance_group,
    distance_group_desc,
    cancelled,
    diverted,
    crs_dep_time,
    crs_arr_time,
    crs_elapsed_time

FROM joined

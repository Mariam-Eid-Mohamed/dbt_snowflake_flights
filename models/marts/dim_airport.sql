{{ config(materialized = 'table') }}

-- 1. All airport keys from staging flights (origin + destination)
WITH airport_keys AS (
    SELECT DISTINCT
        CAST(origin_airport_id   AS STRING) AS airport_id,
        origin_airport_code      AS airport_code,
        origin_city_name         AS city_name,
        origin_state_code        AS state_code,
        origin_state_name        AS state_name,
        origin_wac               AS wac
    FROM {{ ref('stg_flights') }}

    UNION

    SELECT DISTINCT
        CAST(dest_airport_id     AS STRING) AS airport_id,
        dest_airport_code        AS airport_code,
        dest_city_name           AS city_name,
        dest_state_code          AS state_code,
        dest_state_name          AS state_name,
        dest_wac                 AS wac
    FROM {{ ref('stg_flights') }}
),

-- 2. Lookup metadata from L_AIRPORT (airport name)
airport_lookup AS (
    SELECT
        airport_id,
        airport_name
    FROM {{ ref('stg_airport') }}
),

-- 3. Lookup WAC names
wac_lookup AS (
    SELECT
        wac_code,
        wac_name
    FROM {{ ref('stg_world_area_codes') }}
),

-- 4. Join everything
joined AS (
    SELECT
        a.airport_id,
        a.airport_code,
        a.city_name,
        a.state_code,
        a.state_name,
        a.wac,

        al.airport_name,
        wl.wac_name

    FROM airport_keys a
    LEFT JOIN airport_lookup al
        ON a.airport_code = al.airport_id
    LEFT JOIN wac_lookup wl
        ON a.wac = wl.wac_code
)

-- 5. Final dimension WITHOUT surrogate key
SELECT
    airport_id,         -- natural key (string, can handle '01A')
    airport_code,
    airport_name,

    city_name,
    state_code,
    state_name,

    wac,
    wac_name
FROM joined

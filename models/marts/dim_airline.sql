{{ config(materialized = 'table') }}

-- 1. Distinct airline keys from flights
WITH airline_keys AS (
    SELECT DISTINCT
        airline_code,
        dot_airline_id,
        iata_airline_code
    FROM {{ ref('stg_flights') }}
),

-- 2. Lookup: airline code -> name (unique carriers)
unique_carriers AS (
    SELECT
        airline_code,
        airline_name
    FROM {{ ref('stg_unique_carriers') }}
),

-- 3. Lookup: DOT numeric id -> name (airline id)
airline_id_lookup AS (
    SELECT
        airline_dot_id,
        airline_dot_name
    FROM {{ ref('stg_airline_id') }}
),

-- 4. Carrier history with is_active flag
carrier_history AS (
    SELECT
        airline_code,
        airline_name,
        active_from_year::int AS active_from_year,
        active_to_year::int   AS active_to_year,
        CASE
            WHEN active_to_year IS NULL
                 OR active_to_year >= DATE_PART(year, CURRENT_DATE)
            THEN TRUE
            ELSE FALSE
        END AS is_active
    FROM {{ ref('stg_carrier_history') }}
),

-- 5. Join everything together
joined AS (
    SELECT
        ak.airline_code,
        ak.dot_airline_id,
        ak.iata_airline_code,

        -- prefer name from unique_carriers, fallback to history airline_name
        COALESCE(uc.airline_name, ch.airline_name) AS carrier_name,
        ai.airline_dot_name,

        ch.active_from_year,
        ch.active_to_year,
        ch.is_active
    FROM airline_keys ak
    LEFT JOIN unique_carriers   uc ON ak.airline_code   = uc.airline_code
    LEFT JOIN airline_id_lookup ai ON ak.dot_airline_id = ai.airline_dot_id
    LEFT JOIN carrier_history   ch ON ak.airline_code   = ch.airline_code
)

-- 6. Final select with surrogate key
SELECT
    airline_code,
    dot_airline_id,
    iata_airline_code,

    carrier_name,
    airline_dot_name,

    active_from_year,
    COALESCE(active_to_year::string, 'Currently active') AS active_to_year_label,
    is_active

FROM joined

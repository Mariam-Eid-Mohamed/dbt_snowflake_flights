{{ config(materialized = 'table') }}

WITH distinct_dates AS (
    SELECT DISTINCT
        flight_date,
        year,
        quarter,
        month,
        day_of_month,
        day_of_week
    FROM {{ ref('stg_flights') }}
),

us_holidays AS (
    SELECT DATE_FROM_PARTS(year, 1, 1) AS holiday_date, 'New Year''s Day' AS holiday_name FROM distinct_dates
    UNION ALL SELECT DATE_FROM_PARTS(year, 7, 4), 'Independence Day' FROM distinct_dates
    UNION ALL SELECT DATE_FROM_PARTS(year, 12, 25), 'Christmas Day' FROM distinct_dates
    UNION ALL SELECT DATE_FROM_PARTS(year, 11, 11), 'Veterans Day' FROM distinct_dates
),

holiday_flags AS (
    SELECT
        d.flight_date,
        MAX(CASE WHEN h.holiday_date = d.flight_date THEN 1 ELSE 0 END) AS is_holiday
    FROM distinct_dates d
    LEFT JOIN us_holidays h
        ON d.flight_date = h.holiday_date
    GROUP BY 1
)

SELECT
    TO_NUMBER(TO_CHAR(d.flight_date, 'YYYYMMDD')) AS date_id,
    d.flight_date,
    d.year,
    d.quarter,
    d.month,
    d.day_of_week,
    d.day_of_month,

    CASE WHEN d.day_of_week IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
    TO_CHAR(d.flight_date, 'Mon') AS month_name,
    TO_CHAR(d.flight_date, 'Day') AS day_name,

    hf.is_holiday

FROM distinct_dates d
LEFT JOIN holiday_flags hf
    ON d.flight_date = hf.flight_date

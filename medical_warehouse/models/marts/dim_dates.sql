{{ config(materialized='table') }}

WITH date_range AS (
    -- Generate a range of dates
    SELECT
        date_day::DATE AS full_date
    FROM (
        SELECT generate_series(
            (SELECT MIN(message_timestamp)::DATE FROM {{ ref('stg_telegram_messages') }}),
            (SELECT MAX(message_timestamp)::DATE FROM {{ ref('stg_telegram_messages') }}),
            '1 day'::INTERVAL
        ) AS date_day
    ) dates
)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,
    full_date,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(WEEK FROM full_date) AS week_of_year,
    EXTRACT(MONTH FROM full_date) AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(YEAR FROM full_date) AS year,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
ORDER BY full_date
-- Test to ensure no messages have future dates
SELECT *
FROM {{ ref('fct_messages') }} fm
JOIN {{ ref('dim_dates') }} dd ON fm.date_key = dd.date_key
WHERE dd.full_date > CURRENT_DATE
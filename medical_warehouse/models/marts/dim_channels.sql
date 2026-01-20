{{ config(materialized='table') }}

WITH channel_stats AS (
    SELECT
        channel_name,
        MIN(message_timestamp) AS first_post_date,
        MAX(message_timestamp) AS last_post_date,
        COUNT(*) AS total_posts,
        AVG(views) AS avg_views
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
),

categorized_channels AS (
    SELECT
        channel_name,
        CASE
            WHEN LOWER(channel_name) LIKE '%pharma%' OR LOWER(channel_name) LIKE '%med%' THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) LIKE '%cosmetic%' OR LOWER(channel_name) LIKE '%beauty%' THEN 'Cosmetics'
            ELSE 'Medical'
        END AS channel_type,
        first_post_date,
        last_post_date,
        total_posts,
        avg_views
    FROM channel_stats
)

SELECT
    {{ dbt_utils.surrogate_key(['channel_name']) }} AS channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
FROM categorized_channels
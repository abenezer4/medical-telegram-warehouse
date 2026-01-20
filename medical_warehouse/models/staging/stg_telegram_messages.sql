{{ config(materialized='view') }}

WITH raw_messages AS (
    SELECT *
    FROM {{ source('raw', 'telegram_messages') }}
),

staged AS (
    SELECT
        message_id::INTEGER AS message_id,
        channel_name::VARCHAR AS channel_name,
        channel_title::VARCHAR AS channel_title,
        TO_TIMESTAMP(message_date, 'YYYY-MM-DD HH24:MI:SS.US') AS message_timestamp,
        message_text::TEXT AS message_text,
        CASE WHEN has_media = 'true' THEN TRUE ELSE FALSE END AS has_media,
        image_path::VARCHAR AS image_path,
        COALESCE(views::INTEGER, 0) AS views,
        COALESCE(forwards::INTEGER, 0) AS forwards,
        LENGTH(message_text) AS message_length,
        CASE WHEN image_path IS NOT NULL THEN TRUE ELSE FALSE END AS has_image
    FROM raw_messages
    WHERE message_text IS NOT NULL AND message_text != ''
)

SELECT * FROM staged
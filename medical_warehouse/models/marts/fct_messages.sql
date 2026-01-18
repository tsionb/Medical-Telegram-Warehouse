{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH messages AS (
    SELECT 
        message_id,
        channel_name,
        message_date,
        message_text,
        LENGTH(message_text) as message_length,
        views,
        forwards,
        CASE WHEN image_path IS NOT NULL THEN TRUE ELSE FALSE END as has_image
    FROM {{ ref('stg_telegram_messages') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY m.message_id) as message_key,
    m.message_id,
    c.channel_key,
    d.date_key,
    m.message_text,
    m.message_length,
    m.views,
    m.forwards,
    m.has_image
FROM messages m
LEFT JOIN {{ ref('dim_channels') }} c ON m.channel_name = c.channel_name
LEFT JOIN {{ ref('dim_dates') }} d ON DATE(m.message_date) = d.full_date

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

SELECT 
    message_id,
    channel_name,
    message_date::timestamp,
    message_text,
    has_media,
    image_path,
    COALESCE(views, 0) as views,
    COALESCE(forwards, 0) as forwards
FROM {{ source('raw', 'telegram_messages') }}
WHERE message_text IS NOT NULL

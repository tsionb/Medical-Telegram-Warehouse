

SELECT 
    message_id,
    channel_name,
    message_date::timestamp,
    message_text,
    has_media,
    image_path,
    COALESCE(views, 0) as views,
    COALESCE(forwards, 0) as forwards
FROM "medical_warehouse"."raw"."telegram_messages"
WHERE message_text IS NOT NULL
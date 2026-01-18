-- Test to ensure view counts are non-negative
SELECT 
    message_id,
    views
FROM "medical_warehouse"."analytics_staging"."stg_telegram_messages"
WHERE views < 0
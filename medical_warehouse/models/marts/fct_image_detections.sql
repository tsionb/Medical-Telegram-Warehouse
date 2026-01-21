{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH image_detections AS (
    SELECT 
        y.message_id,
        y.channel_name,
        y.detected_objects,
        y.object_count,
        y.primary_object,
        y.primary_confidence,
        y.image_category,
        y.has_person,
        y.has_container,
        y.has_medical
    FROM image_analysis.yolo_detections y
),

joined_data AS (
    SELECT 
        f.message_key,
        f.channel_key,
        f.date_key,
        f.views,
        f.forwards,
        f.has_image as has_image_in_message,
        i.detected_objects,
        i.object_count,
        i.primary_object,
        i.primary_confidence,
        i.image_category,
        i.has_person,
        i.has_container,
        i.has_medical,
        CASE 
            WHEN i.image_category = 'promotional' THEN 'Person + Product'
            WHEN i.image_category = 'product_display' THEN 'Product Only'
            WHEN i.image_category = 'lifestyle' THEN 'Person Only'
            ELSE 'Other'
        END as image_category_description
    FROM {{ ref('fct_messages') }} f
    LEFT JOIN image_detections i ON f.message_id = i.message_id
    WHERE f.has_image = TRUE  -- Only messages with images
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY message_key) as image_analysis_key,
    *
FROM joined_data
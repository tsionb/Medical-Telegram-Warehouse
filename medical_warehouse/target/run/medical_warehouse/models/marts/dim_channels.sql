
  
    

  create  table "medical_warehouse"."analytics_marts"."dim_channels__dbt_tmp"
  
  
    as
  
  (
    

WITH channel_stats AS (
    SELECT 
        channel_name,
        COUNT(*) as total_posts,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date,
        AVG(views)::numeric(10,2) as avg_views,
        AVG(forwards)::numeric(10,2) as avg_forwards,
        SUM(CASE WHEN image_path IS NOT NULL THEN 1 ELSE 0 END) as total_images
    FROM "medical_warehouse"."analytics_staging"."stg_telegram_messages"
    GROUP BY channel_name
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY channel_name) as channel_key,
    channel_name,
    CASE 
        WHEN channel_name ILIKE '%pharma%' THEN 'Pharmaceutical'
        WHEN channel_name ILIKE '%cosmetic%' THEN 'Cosmetics'
        WHEN channel_name ILIKE '%med%' OR channel_name ILIKE '%medical%' THEN 'Medical'
        WHEN channel_name ILIKE '%health%' OR channel_name ILIKE '%info%' THEN 'Health Information'
        ELSE 'General Health'
    END as channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views,
    avg_forwards,
    total_images,
    -- SIMPLIFIED: Remove rounding or use CAST
    (total_images::float / NULLIF(total_posts, 0) * 100)::numeric(10,2) as image_percentage
FROM channel_stats
ORDER BY total_posts DESC
  );
  
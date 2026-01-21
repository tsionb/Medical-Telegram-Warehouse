-- Analysis 1: Compare engagement by image category
SELECT 
    image_category,
    image_category_description,
    COUNT(*) as image_count,
    AVG(views)::numeric(10,2) as avg_views,
    AVG(forwards)::numeric(10,2) as avg_forwards,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage
FROM {{ ref('fct_image_detections') }}
WHERE image_category IS NOT NULL
GROUP BY image_category, image_category_description
ORDER BY avg_views DESC;

-- Analysis 2: Objects detected by channel
SELECT 
    c.channel_name,
    c.channel_type,
    COUNT(DISTINCT f.message_id) as images_analyzed,
    STRING_AGG(DISTINCT i.primary_object, ', ') as common_objects,
    ROUND(AVG(i.object_count)::numeric, 2) as avg_objects_per_image,
    SUM(CASE WHEN i.has_person THEN 1 ELSE 0 END) as images_with_people,
    SUM(CASE WHEN i.has_medical THEN 1 ELSE 0 END) as images_with_medical
FROM {{ ref('fct_messages') }} f
JOIN {{ ref('dim_channels') }} c ON f.channel_key = c.channel_key
LEFT JOIN image_analysis.yolo_detections i ON f.message_id = i.message_id
WHERE f.has_image = TRUE
GROUP BY c.channel_name, c.channel_type
ORDER BY images_analyzed DESC;

-- Analysis 3: Impact of people in images on engagement
SELECT 
    has_person,
    COUNT(*) as message_count,
    AVG(views)::numeric(10,2) as avg_views,
    AVG(forwards)::numeric(10,2) as avg_forwards,
    AVG(primary_confidence)::numeric(5,3) as avg_confidence
FROM {{ ref('fct_image_detections') }}
WHERE primary_object IS NOT NULL
GROUP BY has_person
ORDER BY avg_views DESC;
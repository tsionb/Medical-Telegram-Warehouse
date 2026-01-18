

WITH date_series AS (
    SELECT 
        generate_series(
            '2024-01-01'::date,
            '2026-12-31'::date,
            '1 day'::interval
        ) as date_day
)

SELECT 
    TO_CHAR(date_day, 'YYYYMMDD')::integer as date_key,
    date_day as full_date,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    TO_CHAR(date_day, 'Month') as month_name,
    EXTRACT(WEEK FROM date_day) as week_of_year,
    EXTRACT(DAY FROM date_day) as day_of_month,
    EXTRACT(DOW FROM date_day) as day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM date_day) = 0 THEN 'Sunday'
        WHEN EXTRACT(DOW FROM date_day) = 1 THEN 'Monday'
        WHEN EXTRACT(DOW FROM date_day) = 2 THEN 'Tuesday'
        WHEN EXTRACT(DOW FROM date_day) = 3 THEN 'Wednesday'
        WHEN EXTRACT(DOW FROM date_day) = 4 THEN 'Thursday'
        WHEN EXTRACT(DOW FROM date_day) = 5 THEN 'Friday'
        WHEN EXTRACT(DOW FROM date_day) = 6 THEN 'Saturday'
    END as day_name,
    CASE 
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END as is_weekend
FROM date_series
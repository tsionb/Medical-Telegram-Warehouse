
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test to ensure view counts are non-negative
SELECT 
    message_id,
    views
FROM "medical_warehouse"."analytics_staging"."stg_telegram_messages"
WHERE views < 0
  
  
      
    ) dbt_internal_test
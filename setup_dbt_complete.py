
import os
import yaml
from pathlib import Path
import shutil

def setup_dbt_project():
    """Create complete dbt project structure"""
    print("="*60)
    print(" COMPLETE dbt PROJECT SETUP")
    print("="*60)
    
    # Project directory
    project_root = Path("medical_warehouse")
    
    # Remove if exists
    if project_root.exists():
        print(f" Removing existing {project_root}...")
        shutil.rmtree(project_root)
    
    # Create project structure
    print("\n Creating project structure...")
    
    directories = [
        'models/staging',
        'models/marts',
        'tests',
        'macros',
        'seeds',
        'analyses',
        'snapshots'
    ]
    
    for directory in directories:
        dir_path = project_root / directory
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"   {directory}")
    
    # Create __init__.py files
    init_files = [
        'models/__init__.py',
        'models/staging/__init__.py',
        'models/marts/__init__.py',
        'tests/__init__.py',
        'macros/__init__.py',
    ]
    
    for init_file in init_files:
        file_path = project_root / init_file
        file_path.touch()
    
    print("\n Creating configuration files...")
    
    # 1. dbt_project.yml
    dbt_project_content = """name: 'medical_warehouse'
version: '1.0.0'
config-version: 2

profile: 'medical_warehouse'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  medical_warehouse:
    staging:
      materialized: view
      schema: staging
    marts:
      materialized: table
      schema: marts
"""
    
    with open(project_root / "dbt_project.yml", 'w') as f:
        f.write(dbt_project_content)
    print("   dbt_project.yml")
    
    # 2. sources.yml
    sources_content = """version: 2

sources:
  - name: raw
    schema: raw
    tables:
      - name: telegram_messages
        description: "Raw Telegram messages scraped from medical channels"
        columns:
          - name: message_id
            description: "Unique identifier for each message"
            tests:
              - unique
              - not_null
          - name: channel_name
            description: "Name of the Telegram channel"
          - name: message_date
            description: "Timestamp when message was posted"
          - name: message_text
            description: "Content of the message"
          - name: has_media
            description: "Whether the message contains media"
          - name: image_path
            description: "Path to downloaded image file"
          - name: views
            description: "Number of views on the message"
          - name: forwards
            description: "Number of times message was forwarded"
"""
    
    with open(project_root / "models" / "sources.yml", 'w') as f:
        f.write(sources_content)
    print("   sources.yml")
    
    # 3. Create dbt models
    print("\n Creating dbt models...")
    
    # Staging model
    stg_content = """{{
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
"""
    
    with open(project_root / "models" / "staging" / "stg_telegram_messages.sql", 'w') as f:
        f.write(stg_content)
    print("   stg_telegram_messages.sql")
    
    # dim_channels
    dim_channels_content = """{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH channel_stats AS (
    SELECT 
        channel_name,
        COUNT(*) as total_posts,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date,
        AVG(views)::numeric(10,2) as avg_views,
        AVG(forwards)::numeric(10,2) as avg_forwards,
        SUM(CASE WHEN image_path IS NOT NULL THEN 1 ELSE 0 END) as total_images
    FROM {{ ref('stg_telegram_messages') }}
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
    ROUND((total_images::float / NULLIF(total_posts, 0)) * 100, 2) as image_percentage
FROM channel_stats
ORDER BY total_posts DESC
"""
    
    with open(project_root / "models" / "marts" / "dim_channels.sql", 'w') as f:
        f.write(dim_channels_content)
    print("   dim_channels.sql")
    
    # dim_dates
    dim_dates_content = """{{
    config(
        materialized='table',
        schema='marts'
    )
}}

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
"""
    
    with open(project_root / "models" / "marts" / "dim_dates.sql", 'w') as f:
        f.write(dim_dates_content)
    print("   dim_dates.sql")
    
    # fct_messages
    fct_content = """{{
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
"""
    
    with open(project_root / "models" / "marts" / "fct_messages.sql", 'w') as f:
        f.write(fct_content)
    print("   fct_messages.sql")
    
    # Create test
    test_content = """-- Test to ensure view counts are non-negative
SELECT 
    message_id,
    views
FROM {{ ref('stg_telegram_messages') }}
WHERE views < 0
"""
    
    with open(project_root / "tests" / "assert_positive_views.sql", 'w') as f:
        f.write(test_content)
    print("   assert_positive_views.sql")
    
    # Create schema.yml for tests
    schema_content = """version: 2

models:
  - name: dim_channels
    description: "Dimension table for Telegram channels"
    columns:
      - name: channel_key
        description: "Surrogate key for channel"
        tests:
          - unique
          - not_null

  - name: fct_messages
    description: "Fact table containing one row per message"
    columns:
      - name: message_key
        description: "Surrogate key for message"
        tests:
          - unique
          - not_null
      - name: channel_key
        description: "Foreign key to dim_channels"
        tests:
          - relationships:
              to: ref('dim_channels')
              field: channel_key
"""
    
    with open(project_root / "models" / "marts" / "schema.yml", 'w') as f:
        f.write(schema_content)
    print("   schema.yml")
    
    print("\n" + "="*60)
    print(" dbt PROJECT CREATED SUCCESSFULLY!")

    
    return True

def create_dbt_profile():
    """Create dbt profile"""
    print("\n Creating dbt profile...")
    
    profile_content = {
        'medical_warehouse': {
            'target': 'dev',
            'outputs': {
                'dev': {
                    'type': 'postgres',
                    'host': 'localhost',
                    'port': 5432,
                    'user': 'postgres',
                    'pass': '031628',
                    'dbname': 'medical_warehouse',
                    'schema': 'analytics',
                    'threads': 1
                }
            }
        }
    }
    
    # Create .dbt directory
    dbt_dir = Path.home() / '.dbt'
    dbt_dir.mkdir(exist_ok=True)
    
    # Write profiles.yml
    profile_path = dbt_dir / 'profiles.yml'
    with open(profile_path, 'w') as f:
        yaml.dump(profile_content, f, default_flow_style=False)
    
    print(f" Created dbt profile at: {profile_path}")
    print("\n Profile configuration:")
    print(f"  Database: medical_warehouse")
    print(f"  User: postgres")
    print(f"  Schema: analytics")
    
    return True

def main():
    """Main function"""
    try:
        # Setup project
        if not setup_dbt_project():
            return
        
        # Create profile
        if not create_dbt_profile():
            return
        
        
    except Exception as e:
        print(f"\n Error: {e}")

if __name__ == "__main__":
    main()
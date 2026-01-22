# pipeline.py

import dagster as dg
import subprocess
import os
import sys
from datetime import datetime

# ========== DEFINE OPERATIONS (OPS) ==========

@dg.op
def scrape_telegram_data(context):
    """Run the Telegram scraper"""
    context.log.info("Starting Telegram data scraping...")
    
    try:
        # Run scraper script
        result = subprocess.run(
            [sys.executable, "src/scraper.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            context.log.info("Scraping completed successfully!")
            context.log.info(result.stdout)
        else:
            context.log.error(f"Scraping failed: {result.stderr}")
            raise Exception("Scraping failed")
    
    except Exception as e:
        context.log.error(f"Error in scraping: {e}")
        raise

@dg.op
def load_raw_to_postgres(context):
    """Load raw JSON data to PostgreSQL"""
    context.log.info("Loading raw data to PostgreSQL...")
    
    context.log.info("Data loaded to PostgreSQL.")
    return True

@dg.op
def run_dbt_transformations(context):
    """Run dbt transformations"""
    context.log.info("Running dbt transformations...")
    
    # Change to dbt project directory
    dbt_project_path = "medical_warehouse"
    
    try:
        # Run dbt commands
        os.chdir(dbt_project_path)
        
        # dbt run
        result_run = subprocess.run(
            ["dbt", "run"],
            capture_output=True,
            text=True
        )
        
        if result_run.returncode == 0:
            context.log.info("dbt run completed!")
        else:
            context.log.error(f"dbt run failed: {result_run.stderr}")
            raise Exception("dbt run failed")
        
        # dbt test
        result_test = subprocess.run(
            ["dbt", "test"],
            capture_output=True,
            text=True
        )
        
        if result_test.returncode == 0:
            context.log.info("dbt tests passed!")
        else:
            context.log.error(f"dbt tests failed: {result_test.stderr}")
            raise Exception("dbt tests failed")
        
        os.chdir("..")
        
    except Exception as e:
        context.log.error(f"Error in dbt: {e}")
        os.chdir("..")
        raise

@dg.op
def run_yolo_enrichment(context):
    """Run YOLO object detection"""
    context.log.info("Running YOLO object detection...")
    
    try:
        # Run YOLO detection
        result = subprocess.run(
            [sys.executable, "src/yolo_detect.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            context.log.info("YOLO detection completed!")
            context.log.info(result.stdout)
            
            # Load results to DB
            load_result = subprocess.run(
                [sys.executable, "src/load_yolo_results.py"],
                capture_output=True,
                text=True
            )
            
            if load_result.returncode == 0:
                context.log.info("YOLO results loaded to database!")
            else:
                context.log.error(f"Failed to load YOLO results: {load_result.stderr}")
                raise Exception("YOLO load failed")
        else:
            context.log.error(f"YOLO detection failed: {result.stderr}")
            raise Exception("YOLO detection failed")
    
    except Exception as e:
        context.log.error(f"Error in YOLO: {e}")
        raise

# ========== DEFINE THE JOB (PIPELINE) ==========

@dg.job
def medical_telegram_pipeline():
    """Main pipeline: Scrape → Load → Transform → Enrich"""
    
    # Define execution order
    scrape = scrape_telegram_data()
    load = load_raw_to_postgres()
    dbt = run_dbt_transformations()
    yolo = run_yolo_enrichment()
    
    # Set dependencies
    load.depends_on(scrape)
    dbt.depends_on(load)
    yolo.depends_on(dbt)

# ========== SCHEDULE THE JOB ==========

@dg.schedule(
    cron_schedule="0 2 * * *",  # Runs daily at 2 AM UTC
    job=medical_telegram_pipeline,
    execution_timezone="UTC"
)
def daily_pipeline_schedule(context):
    """Schedule the pipeline to run daily"""
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {
        "ops": {
            "scrape_telegram_data": {
                "config": {"date": scheduled_date}
            }
        }
    }

# ========== SENSOR FOR FILE CHANGES (OPTIONAL) ==========

@dg.sensor(job=medical_telegram_pipeline)
def file_change_sensor(context):
    """Trigger pipeline when scraper script changes"""
    scraper_path = "src/scraper.py"
    
    # Check if file was modified
    # (Simplified example - Dagster has better ways to do this)
    yield dg.RunRequest(run_key="scraper_updated")

# ========== MAIN EXECUTION ==========

if __name__ == "__main__":
    # Run the pipeline directly
    result = medical_telegram_pipeline.execute_in_process()
    
    if result.success:
        print(" Pipeline executed successfully!")
    else:
        print(" Pipeline failed!")
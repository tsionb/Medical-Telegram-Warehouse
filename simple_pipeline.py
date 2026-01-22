# simple_pipeline.py

import dagster as dg
import subprocess
import sys

@dg.op
def step1_scrape(context):
    context.log.info("Step 1: Scraping Telegram...")
    subprocess.run([sys.executable, "src/scraper.py"])
    return "Scraping done"

@dg.op
def step2_dbt(context, previous_result):
    context.log.info(f"Step 2: Running dbt... (Previous: {previous_result})")
    subprocess.run(["dbt", "run"], cwd="medical_warehouse")
    return "dbt done"

@dg.op
def step3_yolo(context, previous_result):
    context.log.info(f"Step 3: Running YOLO... (Previous: {previous_result})")
    subprocess.run([sys.executable, "src/yolo_detect.py"])
    subprocess.run([sys.executable, "src/load_yolo_results.py"])
    return "YOLO done"

@dg.job
def simple_pipeline():
    s1 = step1_scrape()
    s2 = step2_dbt(s1)
    step3_yolo(s2)

if __name__ == "__main__":
    result = simple_pipeline.execute_in_process()
    print("Pipeline result:", "Success" if result.success else "Failed")
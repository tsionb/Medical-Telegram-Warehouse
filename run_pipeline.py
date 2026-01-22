"""
Simple script to run the Dagster pipeline
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from dagster_pipeline.pipeline import medical_pipeline_job
from dagster import execute_job

def run_pipeline():
    """Run the pipeline once"""
    print("="*60)
    print(" STARTING MEDICAL TELEGRAM PIPELINE")
    print("="*60)
    
    print("\n This will run:")
    print("  1. Telegram scraping")
    print("  2. Load data to PostgreSQL")
    print("  3. dbt transformations")
    print("  4. YOLO object detection")
    print("  5. Start and test FastAPI")
    print("  6. Generate report")
    
    try:
        result = execute_job(medical_pipeline_job)
        
        print("\n" + "="*60)
        print(" PIPELINE EXECUTION COMPLETE")
        print("="*60)
        
        if result.success:
            print(" All pipeline steps completed successfully!")
            
            # Show outputs if available
            if result.output_for_node("generate_pipeline_report"):
                report = result.output_for_node("generate_pipeline_report")
                print(f"\n Report generated: {report.get('report_path', 'N/A')}")
                print(f" Timestamp: {report.get('timestamp', 'N/A')}")
        else:
            print(" Pipeline execution failed")
            
        print(f"\n Success: {result.success}")
        
    except Exception as e:
        print(f"\n Error executing pipeline: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
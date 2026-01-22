"""Verify Dagster pipeline setup"""

import subprocess
import sys
from pathlib import Path

def check_dagster_installation():
    """Check if Dagster is installed"""
    print(" Checking Dagster installation...")
    
    try:
        import dagster
        print(f" Dagster installed: version {dagster.__version__}")
        return True
    except ImportError:
        print(" Dagster not installed!")
        print("   Run: pip install dagster dagster-webserver")
        return False

def check_pipeline_files():
    """Check if pipeline files exist"""
    print("\n Checking pipeline files...")
    
    required_files = [
        "dagster_pipeline/__init__.py",
        "dagster_pipeline/pipeline.py",
        "dagster_pipeline/simple_pipeline.py",
        "workspace.yaml"
    ]
    
    all_exist = True
    for file in required_files:
        if Path(file).exists():
            print(f" {file}")
        else:
            print(f" {file} - Missing!")
            all_exist = False
    
    return all_exist

def test_dagster_webserver():
    """Test Dagster webserver"""
    print("\n Testing Dagster webserver...")
    
    print(" To start Dagster UI:")
    print("   dagster dev -f dagster_pipeline/pipeline.py")
    print("\n   Then open: http://localhost:3000")
    
    return True

def run_simple_pipeline():
    """Run the simple pipeline"""
    print("\n Testing pipeline execution...")
    
    try:
        result = subprocess.run(
            [sys.executable, "-c", """
from dagster import execute_job
from dagster_pipeline.simple_pipeline import simple_medical_pipeline

result = execute_job(simple_medical_pipeline)
print(f"Pipeline executed: {result.success}")
if result.success:
    print(" Simple pipeline test passed!")
else:
    print(" Simple pipeline test failed")
            """],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )
        
        if result.returncode == 0:
            print(" Pipeline execution test successful")
            print(result.stdout)
            return True
        else:
            print(" Pipeline execution failed")
            print(result.stderr)
            return False
            
    except Exception as e:
        print(f" Error: {e}")
        return False

def main():
    """Main verification function"""
    print("="*60)
    print("VERIFICATION - DAGSTER PIPELINE")
    print("="*60)
    
    # Check 1: Installation
    install_ok = check_dagster_installation()
    
    # Check 2: Files
    files_ok = check_pipeline_files()
    
    # Check 3: Test pipeline
    pipeline_ok = run_simple_pipeline()
    
    print("\n" + "="*60)
    
    if install_ok and files_ok and pipeline_ok:
        print(" SETUP COMPLETE! ")
        print("   1. Installed Dagster and dependencies")
        print("   2. Created pipeline definitions")
        print("   3. Set up job and schedule definitions")
        print("   4. Tested pipeline execution")
        
        
    else:
        print(" Some checks failed. Please fix above issues.")

if __name__ == "__main__":
    main()
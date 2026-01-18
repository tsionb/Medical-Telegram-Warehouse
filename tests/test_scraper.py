"""Test the Telegram scraper setup"""

import os
import sys
import json
from pathlib import Path

def test_environment():
    """Test if environment variables are set"""
    print(" Testing environment variables...")
    
    required_vars = ['API_ID', 'API_HASH']
    all_present = True
    
    for var in required_vars:
        if os.getenv(var):
            print(f" {var} is set")
        else:
            print(f" {var} is NOT set")
            all_present = False
    
    return all_present

def test_directories():
    """Test if required directories exist"""
    print("\n Testing directory structure...")
    
    required_dirs = [
        'data/raw/images',
        'data/raw/telegram_messages',
        'logs',
        'src',
        'scripts'
    ]
    
    all_exist = True
    
    for directory in required_dirs:
        if Path(directory).exists():
            print(f" Directory exists: {directory}")
        else:
            print(f" Missing directory: {directory}")
            all_exist = False
    
    return all_exist

def test_requirements():
    """Test if required packages are installed"""
    print("\n Testing Python packages...")
    
    required_packages = [
        'telethon',
        'python-dotenv',
        'pandas'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f" {package} is installed")
        except ImportError:
            print(f" {package} is NOT installed")
            missing_packages.append(package)
    
    return len(missing_packages) == 0

def main():
    """Run all tests"""
    print(" Running Task 1 Setup Tests")
    print("="*50)
    
    env_ok = test_environment()
    dirs_ok = test_directories()
    packages_ok = test_requirements()
    
    print("\n" + "="*50)
    print(" TEST RESULTS:")
    print(f"Environment Variables: {' PASS' if env_ok else ' FAIL'}")
    print(f"Directory Structure: {' PASS' if dirs_ok else ' FAIL'}")
    print(f"Python Packages: {' PASS' if packages_ok else ' FAIL'}")
    
    if env_ok and dirs_ok and packages_ok:
        print("\n ALL TESTS PASSED!")
        print("You can now run: python src/scraper.py")
        return True
    else:
        print("\n SOME TESTS FAILED")
        print("Please fix the issues above before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
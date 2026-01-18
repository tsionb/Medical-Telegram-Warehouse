"""Create all necessary directories for the project"""

import os
from pathlib import Path

def create_project_structure():
    """Create the complete directory structure"""
    directories = [
        # Data directories
        'data/raw/images',
        'data/raw/telegram_messages',
        'data/processed',
        'data/exports',
        
        # Logs
        'logs',
        
        # Source code organization
        'src/utils',
        'src/models',
        
        # API
        'api/routers',
        
        # Tests
        'tests/unit',
        'tests/integration',
        
        # Notebooks
        'notebooks/exploratory',
        'notebooks/analysis',
    ]
    
    print("Creating project directories...")
    print("-" * 40)
    
    created_count = 0
    existed_count = 0
    
    for directory in directories:
        if not os.path.exists(directory):
            Path(directory).mkdir(parents=True, exist_ok=True)
            print(f" Created: {directory}")
            created_count += 1
        else:
            print(f"✓ Exists: {directory}")
            existed_count += 1
    
    print("-" * 40)
    print(f" Created {created_count} new directories")
    print(f" Found {existed_count} existing directories")
    
    # Create empty __init__.py files
    init_files = [
        'src/__init__.py',
        'src/utils/__init__.py',
        'src/models/__init__.py',
        'api/__init__.py',
        'api/routers/__init__.py',
        'tests/__init__.py',
        'tests/unit/__init__.py',
        'tests/integration/__init__.py',
        'notebooks/__init__.py',
        'notebooks/exploratory/__init__.py',
        'notebooks/analysis/__init__.py',
    ]
    
    print("\nCreating __init__.py files...")
    for init_file in init_files:
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write('')
            print(f" Created: {init_file}")
    
    print("\ Project structure is ready!")

if __name__ == "__main__":
    create_project_structure()
# fix_dbt_errors.py
from pathlib import Path

def fix_dim_channels():
    """Fix the ROUND function error in dim_channels"""
    file_path = Path("medical_warehouse/models/marts/dim_channels.sql")
    
    if not file_path.exists():
        print(" dim_channels.sql not found!")
        return False
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix the ROUND function
    old_line = 'ROUND((total_images::float / NULLIF(total_posts, 0)) * 100, 2) as image_percentage'
    new_line = 'ROUND(((total_images::float / NULLIF(total_posts, 0)) * 100)::numeric, 2) as image_percentage'
    
    if old_line in content:
        content = content.replace(old_line, new_line)
        with open(file_path, 'w') as f:
            f.write(content)
        print(" Fixed ROUND function in dim_channels.sql")
        return True
    else:
        print("  Could not find the line to fix")
        return False

def fix_schema_yml():
    """Fix the schema.yml relationships syntax"""
    file_path = Path("medical_warehouse/models/marts/schema.yml")
    
    if not file_path.exists():
        print(" schema.yml not found!")
        return False
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Fix relationships syntax
    old_syntax = """      - name: channel_key
        description: "Foreign key to dim_channels"
        tests:
          - relationships:
              to: ref('dim_channels')
              field: channel_key"""
    
    new_syntax = """      - name: channel_key
        description: "Foreign key to dim_channels"
        tests:
          - relationships:
              arguments:
                to: ref('dim_channels')
                field: channel_key"""
    
    if old_syntax in content:
        content = content.replace(old_syntax, new_syntax)
        with open(file_path, 'w') as f:
            f.write(content)
        print(" Fixed relationships syntax in schema.yml")
        return True
    else:
        print("  Could not find the syntax to fix")
        return False

def main():
    print(" Fixing dbt errors...")
    print("="*60)
    
    fix1 = fix_dim_channels()
    fix2 = fix_schema_yml()
    
    if fix1 or fix2:
        print("\n" + "="*60)
        print(" Fixes applied!")
        print("\nNow run:")
        print("cd medical_warehouse")
        print("dbt run")
    else:
        print("\n No fixes were needed or files not found")

if __name__ == "__main__":
    main()
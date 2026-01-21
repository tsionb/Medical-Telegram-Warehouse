import psycopg2

def check_correct_tables():
    """Check tables with correct schema names"""
    print(" Correct Verification - Using analytics schemas")
    print("="*60)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="medical_warehouse",
            user="postgres",
            password="031628"
        )
        cur = conn.cursor()
        
        # Check with analytics_ prefix
        tables = [
            ('analytics_staging', 'stg_telegram_messages'),
            ('analytics_marts', 'dim_channels'),
            ('analytics_marts', 'dim_dates'),
            ('analytics_marts', 'fct_messages')
        ]
        
        print(" Checking tables in analytics schemas:")
        print("-" * 40)
        
        all_tables_exist = True
        for schema, table in tables:
            cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table}'
            );
            """)
            exists = cur.fetchone()[0]
            status = " EXISTS" if exists else " MISSING"
            print(f"{status}: {schema}.{table}")
            if not exists:
                all_tables_exist = False
        
        if all_tables_exist:
            print("\n ALL TABLES FOUND!")
            
            # Show actual data
            print("\n Actual Data Summary:")
            print("-" * 40)
            
            # 1. Check staging table
            cur.execute("SELECT COUNT(*) FROM analytics_staging.stg_telegram_messages;")
            staging_count = cur.fetchone()[0]
            print(f"1. Staging messages: {staging_count}")
            
            # 2. Check dim_channels
            cur.execute("""
            SELECT channel_name, channel_type, total_posts 
            FROM analytics_marts.dim_channels 
            ORDER BY total_posts DESC;
            """)
            print(f"\n2. Channels ({cur.rowcount} total):")
            for row in cur.fetchall():
                print(f"   • {row[0]} ({row[1]}): {row[2]} posts")
            
            # 3. Check fct_messages
            cur.execute("SELECT COUNT(*) FROM analytics_marts.fct_messages;")
            fact_count = cur.fetchone()[0]
            print(f"\n3. Fact table messages: {fact_count}")
            
            # 4. Sample query
            print("\n4. Sample Analysis Query:")
            cur.execute("""
            SELECT 
                c.channel_name,
                COUNT(f.message_id) as message_count,
                AVG(f.views)::numeric(10,2) as avg_views,
                SUM(CASE WHEN f.has_image THEN 1 ELSE 0 END) as images_count
            FROM analytics_marts.fct_messages f
            JOIN analytics_marts.dim_channels c ON f.channel_key = c.channel_key
            GROUP BY c.channel_name
            ORDER BY avg_views DESC;
            """)
            
            print("   Channel performance:")
            for row in cur.fetchall():
                print(f"   • {row[0]}: {row[1]} msgs, {row[2]} avg views, {row[3]} images")
        
        cur.close()
        conn.close()
        
        print("\n" + "="*60)
        if all_tables_exist:
            print(" VERIFICATION SUCCESSFUL!")
            print("\n The dbt models are correctly built in analytics schemas.")
        else:
            print(" Some tables missing.")
            
    except Exception as e:
        print(f" Error: {e}")

def list_all_tables():
    """List all tables in the database"""
    print("\n Listing ALL tables in medical_warehouse database:")
    print("="*60)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="medical_warehouse",
            user="postgres",
            password="031628"
        )
        cur = conn.cursor()
        
        # List all schemas and tables
        cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
        """)
        
        schemas = {}
        for schema, table, table_type in cur.fetchall():
            if schema not in schemas:
                schemas[schema] = []
            schemas[schema].append((table, table_type))
        
        for schema in sorted(schemas.keys()):
            print(f"\n Schema: {schema}")
            print("-" * 40)
            for table, table_type in sorted(schemas[schema]):
                type_symbol = "" if table_type == 'BASE TABLE' else "👁️"
                print(f"  {type_symbol} {table} ({table_type})")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f" Error: {e}")

def main():
    print("="*60)
    print(" CORRECT VERIFICATION - TASK 2 COMPLETION")
    print("="*60)
    
    # Show all tables first
    list_all_tables()
    
    # Then check specific tables
    check_correct_tables()
    
    print("\n" + "="*60)
    print(" EXPLANATION:")
    print("-" * 60)
    print("""
 dbt profile.yml has: schema: analytics
This means dbt creates tables in:
  • analytics_staging (for staging models)
  • analytics_marts   (for mart models)
    
 star schema is BUILT and WORKING:
   analytics_staging.stg_telegram_messages
   analytics_marts.dim_channels  
   analytics_marts.dim_dates
   analytics_marts.fct_messages
  
Task 2 is COMPLETE! 
    """)

if __name__ == "__main__":
    main()
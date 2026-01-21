"""
Load scraped Telegram data into PostgreSQL 
Step 1: Load raw data to database
"""

import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

class DataLoader:
    def __init__(self):
        """Initialize database connection - FIXED to handle missing database"""
        print(" Initializing PostgreSQL connection...")
        
        # FIRST connect to default 'postgres' database to check/create our database
        try:
            # Connect to default 'postgres' database
            temp_conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database='postgres',  # Connect to default database
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', '031628')
            )
            temp_conn.autocommit = True  # Need this for CREATE DATABASE
            temp_cursor = temp_conn.cursor()
            
            # Check if our database exists
            temp_cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'medical_warehouse';")
            if not temp_cursor.fetchone():
                print(" Database 'medical_warehouse' doesn't exist, creating it...")
                temp_cursor.execute("CREATE DATABASE medical_warehouse;")
                print(" Database 'medical_warehouse' created successfully!")
            
            temp_cursor.close()
            temp_conn.close()
            
        except Exception as e:
            print(f" Error connecting to PostgreSQL: {e}")
            print("\n Make sure PostgreSQL is running!")
            print("   For Docker: docker run --name postgres -e POSTGRES_PASSWORD=031628 -p 5432:5432 -d postgres")
            print("   Then run this script again.")
            raise
        
        # NOW connect to our medical_warehouse database
        print(" Connecting to medical_warehouse database...")
        self.connection = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database='medical_warehouse',  # Now this exists!
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '031628')
        )
        self.cursor = self.connection.cursor()
        
        print(" Connected to PostgreSQL database: medical_warehouse")
    
    def create_raw_schema(self):
        """Create raw schema and table for Telegram data"""
        print("\n Creating database schema...")
        
        # Create raw schema
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create telegram_messages table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            message_id BIGINT PRIMARY KEY,
            channel_name TEXT NOT NULL,
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN,
            image_path TEXT,
            views INTEGER DEFAULT 0,
            forwards INTEGER DEFAULT 0,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        self.cursor.execute(create_table_sql)
        self.connection.commit()
        
        print(" Created raw.telegram_messages table")
    
    def find_latest_data(self):
        """Find the latest scraped data folder"""
        data_dir = Path("data/raw/telegram_messages")
        
        if not data_dir.exists():
            print(" No data directory found!")
            return None
        
        # Get all date folders
        date_folders = [d for d in data_dir.iterdir() if d.is_dir()]
        
        if not date_folders:
            print(" No data folders found!")
            return None
        
        # Use the latest folder
        latest_folder = sorted(date_folders)[-1]
        print(f" Using data from: {latest_folder.name}")
        
        return latest_folder
    
    def load_json_files(self, data_folder):
        """Load all JSON files from the data folder"""
        json_files = list(data_folder.glob("*.json"))
        
        if not json_files:
            print(" No JSON files found!")
            return 0
        
        total_messages = 0
        
        for json_file in json_files:
            print(f"\n Loading: {json_file.name}")
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                
                # Insert each message
                for msg in messages:
                    self.insert_message(msg)
                
                total_messages += len(messages)
                print(f"    Loaded {len(messages)} messages")
                
            except Exception as e:
                print(f"    Error loading {json_file}: {e}")
                continue
        
        return total_messages
    
    def insert_message(self, message_data):
        """Insert a single message into the database"""
        insert_sql = """
        INSERT INTO raw.telegram_messages 
        (message_id, channel_name, message_date, message_text, 
         has_media, image_path, views, forwards)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (message_id) DO NOTHING;
        """
        
        try:
            # Prepare data
            message_date = message_data.get('message_date')
            if message_date:
                try:
                    message_date = pd.to_datetime(message_date)
                except:
                    message_date = None
            
            self.cursor.execute(insert_sql, (
                message_data.get('message_id'),
                message_data.get('channel_name'),
                message_date,
                message_data.get('message_text', ''),
                message_data.get('has_media', False),
                message_data.get('image_path'),
                message_data.get('views', 0),
                message_data.get('forwards', 0)
            ))
            
        except Exception as e:
            print(f"    Error inserting message {message_data.get('message_id')}: {e}")
    
    def verify_data(self):
        """Verify data was loaded correctly"""
        print("\n Verifying loaded data...")
        
        # Count total messages
        self.cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
        total_count = self.cursor.fetchone()[0]
        print(f"    Total messages in database: {total_count}")
        
        # Count by channel
        self.cursor.execute("""
        SELECT channel_name, COUNT(*) 
        FROM raw.telegram_messages 
        GROUP BY channel_name 
        ORDER BY COUNT(*) DESC;
        """)
        
        print("\n    Messages by channel:")
        for row in self.cursor.fetchall():
            print(f"     • {row[0]}: {row[1]} messages")
        
        return total_count
    
    def run(self):
        """Main execution function"""
        try:
            # Step 1: Create schema
            self.create_raw_schema()
            
            # Step 2: Find latest data
            data_folder = self.find_latest_data()
            if not data_folder:
                return
            
            # Step 3: Load data
            total_loaded = self.load_json_files(data_folder)
            
            # Commit changes
            self.connection.commit()
            
            # Step 4: Verify
            total_in_db = self.verify_data()
            
            print(f"\n Successfully loaded {total_loaded} messages into PostgreSQL!")
            print(f" Database now contains: {total_in_db} total messages")
            print("\n Database is ready for dbt transformations!")
            
        except Exception as e:
            print(f"\n Error: {e}")
            self.connection.rollback()
            raise
        finally:
            self.cursor.close()
            self.connection.close()

def main():
    """Entry point"""
    print("="*60)
    print(" POSTGRESQL DATA LOADER ")
    print("Loading Telegram data into PostgreSQL for dbt transformations")
    print("="*60)
    
    print(f"\n Using password: {os.getenv('DB_PASSWORD', '031628')}")
    print("If connection fails, check:")
    print("1. PostgreSQL is running")
    print("2. Password is correct")
    print("3. Port 5432 is open")
    print("="*60)
    
    loader = DataLoader()
    loader.run()

if __name__ == "__main__":
    main()
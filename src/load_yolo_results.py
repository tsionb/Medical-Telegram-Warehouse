#!/usr/bin/env python3
"""
Load YOLO detection results to PostgreSQL
"""

import pandas as pd
import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YOLOLoader:
    def __init__(self):
        """Initialize database connection"""
        logger.info(" Connecting to PostgreSQL...")
        
        self.connection = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'medical_warehouse'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', '031628')
        )
        self.cursor = self.connection.cursor()
        
        logger.info(" Connected to PostgreSQL")
    
    def create_image_schema(self):
        """Create schema and table for image detection results"""
        logger.info(" Creating image detection schema...")
        
        # Create schema if not exists
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS image_analysis;")
        
        # Create table for YOLO results
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS image_analysis.yolo_detections (
            detection_id SERIAL PRIMARY KEY,
            image_path TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            message_id BIGINT NOT NULL,
            detected_objects TEXT,
            object_count INTEGER DEFAULT 0,
            primary_object TEXT,
            primary_confidence NUMERIC(5,3),
            image_category TEXT,
            has_person BOOLEAN DEFAULT FALSE,
            has_container BOOLEAN DEFAULT FALSE,
            has_medical BOOLEAN DEFAULT FALSE,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (message_id) REFERENCES raw.telegram_messages(message_id)
        );
        """
        
        self.cursor.execute(create_table_sql)
        self.connection.commit()
        
        logger.info(" Created image_analysis.yolo_detections table")
    
    def load_yolo_csv(self, csv_file='data/processed/yolo_detections.csv'):
        """Load YOLO detection results from CSV"""
        csv_path = Path(csv_file)
        
        if not csv_path.exists():
            logger.error(f" CSV file not found: {csv_file}")
            return 0
        
        logger.info(f" Loading YOLO results from: {csv_file}")
        
        # Read CSV
        df = pd.read_csv(csv_file)
        logger.info(f" Found {len(df)} detection records")
        
        # Insert each record
        insert_sql = """
        INSERT INTO image_analysis.yolo_detections 
        (image_path, channel_name, message_id, detected_objects, 
         object_count, primary_object, primary_confidence, image_category,
         has_person, has_container, has_medical)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        
        rows_loaded = 0
        for _, row in df.iterrows():
            try:
                self.cursor.execute(insert_sql, (
                    row['image_path'],
                    row['channel_name'],
                    int(row['message_id']),
                    row['detected_objects'],
                    int(row['object_count']),
                    row['primary_object'],
                    float(row['primary_confidence']) if pd.notna(row['primary_confidence']) else 0,
                    row['image_category'],
                    bool(row['has_person']),
                    bool(row['has_container']),
                    bool(row['has_medical'])
                ))
                rows_loaded += 1
            except Exception as e:
                logger.warning(f" Error inserting row {row['message_id']}: {e}")
                continue
        
        self.connection.commit()
        logger.info(f" Loaded {rows_loaded} detection records to PostgreSQL")
        return rows_loaded
    
    def verify_data(self):
        """Verify loaded data"""
        logger.info(" Verifying loaded data...")
        
        # Count total records
        self.cursor.execute("SELECT COUNT(*) FROM image_analysis.yolo_detections;")
        total_count = self.cursor.fetchone()[0]
        print(f" Total detection records: {total_count}")
        
        # Count by category
        self.cursor.execute("""
        SELECT image_category, COUNT(*) 
        FROM image_analysis.yolo_detections 
        GROUP BY image_category 
        ORDER BY COUNT(*) DESC;
        """)
        
        print("\n Image Categories:")
        for category, count in self.cursor.fetchall():
            print(f"  • {category}: {count} images")
        
        # Show sample
        self.cursor.execute("""
        SELECT channel_name, image_category, detected_objects 
        FROM image_analysis.yolo_detections 
        WHERE object_count > 0 
        LIMIT 5;
        """)
        
        print("\n Sample Detections:")
        for row in self.cursor.fetchall():
            print(f"  • {row[0]}: {row[1]} - Objects: {row[2]}")
        
        return total_count
    
    def run(self):
        """Main execution"""
        try:
            # Step 1: Create schema
            self.create_image_schema()
            
            # Step 2: Load data
            rows_loaded = self.load_yolo_csv()
            
            if rows_loaded > 0:
                # Step 3: Verify
                self.verify_data()
                
                print(f"\n Successfully loaded {rows_loaded} image detection records!")
                print("\n Ready for dbt integration!")
            else:
                print("\n No data loaded. Check your YOLO detection CSV file.")
                
        except Exception as e:
            logger.error(f" Error: {e}")
            self.connection.rollback()
            raise
        finally:
            self.cursor.close()
            self.connection.close()

def main():
    """Entry point"""
    print("="*60)
    print(" YOLO RESULTS LOADER")
    print("Loading object detection results to PostgreSQL")
    print("="*60)
    
    loader = YOLOLoader()
    loader.run()

if __name__ == "__main__":
    main()
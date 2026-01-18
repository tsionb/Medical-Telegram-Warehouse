import json
import os
from pathlib import Path
from datetime import datetime

def verify_task1():
    """Verify all requirements are met"""
    print("="*60)
    print("TASK 1 VERIFICATION CHECK")
    print("="*60)
    
    # Get today's date folder
    today = datetime.now().strftime('%Y-%m-%d')
    data_dir = Path(f"data/raw/telegram_messages/{today}")
    
    if not data_dir.exists():
        print(" ERROR: No data folder found!")
        return False
    
    print(f" Data directory: {data_dir}")
    print()
    
    # Check JSON files
    json_files = list(data_dir.glob("*.json"))
    print(f" JSON files found: {len(json_files)}")
    
    total_messages = 0
    total_images_downloaded = 0
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Count messages
            msg_count = len(data)
            total_messages += msg_count
            
            # Count images in this file
            image_count = sum(1 for msg in data if msg.get('image_path') and os.path.exists(msg['image_path']))
            total_images_downloaded += image_count
            
            print(f"  • {json_file.name}: {msg_count} messages, {image_count} images")
            
            # Check required fields
            if data:
                first_msg = data[0]
                required_fields = ['message_id', 'channel_name', 'message_date', 
                                 'message_text', 'views', 'forwards']
                missing_fields = [field for field in required_fields if field not in first_msg]
                
                if missing_fields:
                    print(f"      Missing fields: {missing_fields}")
            
        except Exception as e:
            print(f"   Error reading {json_file}: {e}")
    
    print(f"\n TOTALS:")
    print(f"  Total channels: {len(json_files)}")
    print(f"  Total messages: {total_messages}")
    print(f"  Total images: {total_images_downloaded}")
    
    # Check image directories
    image_dirs = list(Path("data/raw/images").glob("*"))
    print(f"\n  Image directories: {len(image_dirs)}")
    
    for img_dir in image_dirs:
        image_files = list(img_dir.glob("*.jpg"))
        print(f"  • {img_dir.name}: {len(image_files)} images")
    
    # Verify logs
    log_file = Path("logs/scraper.log")
    if log_file.exists():
        log_size = log_file.stat().st_size
        print(f"\n Log file: {log_file} ({log_size} bytes)")
    else:
        print(f"\n Log file:  NOT FOUND")
    
    # Final assessment
    print("\n" + "="*60)
    print(" REQUIREMENTS MET:")
    print(f"  ✓ At least 3 channels: {len(json_files) >= 3}")
    print(f"  ✓ At least 50 messages per channel: {total_messages >= (len(json_files) * 50)}")
    print(f"  ✓ JSON files in correct structure: {data_dir.exists()}")
    print(f"  ✓ Images downloaded: {total_images_downloaded > 0}")
    print(f"  ✓ Log file created: {log_file.exists()}")
    
    if len(json_files) >= 3 and total_messages >= 150:
        print("\n TASK COMPLETED!")
        return True
    else:
        print("\n  Some requirements not met. Please check above.")
        return False

if __name__ == "__main__":
    verify_task1()
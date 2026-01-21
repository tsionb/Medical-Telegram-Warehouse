"""
YOLO Object Detection for Medical Telegram Images
Analyze images and detect objects
"""

import os
import cv2
import csv
from pathlib import Path
from ultralytics import YOLO
import pandas as pd
from tqdm import tqdm
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_detection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YOLODetector:
    def __init__(self, model_name='yolov8n.pt'):
        """Initialize YOLO detector"""
        logger.info(f" Initializing YOLO detector with model: {model_name}")
        
        # Load pre-trained YOLO model
        self.model = YOLO(model_name)
        
        # Object categories we care about (from YOLO COCO dataset)
        self.object_categories = {
            'person': 0,
            'bottle': 39,
            'cup': 41,
            'bowl': 45,
            'banana': 46,
            'apple': 47,
            'orange': 49,
            'broccoli': 50,
            'carrot': 51,
            'pizza': 53,
            'donut': 54,
            'cake': 55,
            'chair': 56,
            'couch': 57,
            'potted plant': 58,
            'bed': 59,
            'dining table': 60,
            'toilet': 61,
            'tv': 62,
            'laptop': 63,
            'mouse': 64,
            'remote': 65,
            'keyboard': 66,
            'cell phone': 67,
            'book': 73,
            'clock': 74,
            'vase': 75,
            'scissors': 76,
            'teddy bear': 77,
            'hair drier': 78,
            'toothbrush': 79
        }
        
        # Medical-specific objects (YOLO might detect these)
        self.medical_objects = ['bottle', 'pills', 'syringe', 'medical', 'medicine']
        
        logger.info(f" YOLO model loaded. Can detect {len(self.object_categories)} object types")
    
    def detect_objects_in_image(self, image_path):
        """Detect objects in a single image"""
        try:
            # Run YOLO detection
            results = self.model(image_path)
            
            detected_objects = []
            
            # Process each detection
            for result in results:
                for box in result.boxes:
                    # Get object details
                    class_id = int(box.cls[0])
                    class_name = self.model.names[class_id]
                    confidence = float(box.conf[0])
                    
                    # Only include high-confidence detections
                    if confidence > 0.5:  # 50% confidence threshold
                        detected_objects.append({
                            'object': class_name,
                            'confidence': round(confidence, 3),
                            'bbox': box.xyxy[0].tolist()  # Bounding box coordinates
                        })
            
            return detected_objects
            
        except Exception as e:
            logger.error(f" Error processing {image_path}: {e}")
            return []
    
    def classify_image_type(self, detected_objects):
        """Classify image based on detected objects"""
        objects_found = [obj['object'] for obj in detected_objects]
        
        has_person = 'person' in objects_found
        has_product = any(obj in objects_found for obj in ['bottle', 'pills', 'medicine', 'package'])
        has_container = any(obj in objects_found for obj in ['bottle', 'cup', 'bowl', 'box'])
        
        # Classification logic
        if has_person and has_product:
            return 'promotional'  # Person showing product
        elif has_container or has_product:
            return 'product_display'  # Just product/container
        elif has_person and not has_product:
            return 'lifestyle'  # Person, no product
        else:
            return 'other'  # Neither
    
    def process_all_images(self, image_dir='data/raw/images'):
        """Process all images in the directory structure"""
        logger.info(f" Scanning for images in: {image_dir}")
        
        # Find all image files
        image_extensions = ['.jpg', '.jpeg', '.png', '.bmp']
        image_files = []
        
        for ext in image_extensions:
            image_files.extend(Path(image_dir).rglob(f'*{ext}'))
        
        logger.info(f" Found {len(image_files)} images to process")
        
        if len(image_files) == 0:
            logger.error(" No images found! Check your image directory.")
            return []
        
        # Process images
        detection_results = []
        
        for image_path in tqdm(image_files, desc=" Processing images"):
            try:
                # Extract channel name and message_id from path
                # Path format: data/raw/images/{channel_name}/{message_id}.jpg
                relative_path = image_path.relative_to('data/raw/images')
                channel_name = relative_path.parts[0]
                message_id = int(image_path.stem)  # Remove .jpg extension
                
                # Detect objects
                detected_objects = self.detect_objects_in_image(str(image_path))
                
                if detected_objects:
                    # Classify image
                    image_category = self.classify_image_type(detected_objects)
                    
                    # Count object types
                    object_counts = {}
                    for obj in detected_objects:
                        obj_name = obj['object']
                        object_counts[obj_name] = object_counts.get(obj_name, 0) + 1
                    
                    # Create result entry
                    result = {
                        'image_path': str(image_path),
                        'channel_name': channel_name,
                        'message_id': message_id,
                        'detected_objects': ', '.join(object_counts.keys()),
                        'object_count': len(detected_objects),
                        'primary_object': detected_objects[0]['object'] if detected_objects else 'none',
                        'primary_confidence': detected_objects[0]['confidence'] if detected_objects else 0,
                        'image_category': image_category,
                        'has_person': 'person' in object_counts,
                        'has_container': any(obj in object_counts for obj in ['bottle', 'cup', 'bowl']),
                        'has_medical': any(obj in object_counts for obj in self.medical_objects)
                    }
                    
                    detection_results.append(result)
                    
                    # Log interesting findings
                    if 'person' in object_counts:
                        logger.debug(f" Person detected in {image_path.name}")
                    if any(obj in object_counts for obj in self.medical_objects):
                        logger.debug(f" Medical object detected in {image_path.name}")
                
                else:
                    # No objects detected
                    result = {
                        'image_path': str(image_path),
                        'channel_name': channel_name,
                        'message_id': message_id,
                        'detected_objects': 'none',
                        'object_count': 0,
                        'primary_object': 'none',
                        'primary_confidence': 0,
                        'image_category': 'other',
                        'has_person': False,
                        'has_container': False,
                        'has_medical': False
                    }
                    detection_results.append(result)
                
            except Exception as e:
                logger.error(f" Error processing {image_path}: {e}")
                continue
        
        logger.info(f" Processed {len(detection_results)} images")
        return detection_results
    
    def save_results(self, detection_results, output_file='data/processed/yolo_detections.csv'):
        """Save detection results to CSV"""
        # Create output directory
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(detection_results)
        df.to_csv(output_file, index=False)
        
        logger.info(f" Results saved to: {output_file}")
        logger.info(f" Summary: {len(df)} images analyzed")
        
        # Print summary
        if len(df) > 0:
            print("\n DETECTION SUMMARY:")
            print("="*60)
            print(f"Total images analyzed: {len(df)}")
            print(f"Images with detections: {(df['object_count'] > 0).sum()}")
            print(f"Images with people: {df['has_person'].sum()}")
            print(f"Images with containers: {df['has_container'].sum()}")
            print(f"Images with medical objects: {df['has_medical'].sum()}")
            
            print("\n Image Categories:")
            category_counts = df['image_category'].value_counts()
            for category, count in category_counts.items():
                percentage = (count / len(df)) * 100
                print(f"  • {category}: {count} images ({percentage:.1f}%)")
        
        return output_file

def main():
    """Main function"""
    print("="*60)
    print("  YOLO OBJECT DETECTION")
    print("Analyzing medical product images from Telegram")
    print("="*60)
    
    print("\n This will:")
    print("  1. Load YOLOv8 model (automatic download on first run)")
    print("  2. Scan all images in data/raw/images/")
    print("  3. Detect objects in each image")
    print("  4. Classify images into categories")
    print("  5. Save results to CSV")
    
    # Initialize detector
    detector = YOLODetector()
    
    # Process all images
    results = detector.process_all_images()
    
    if results:
        # Save results
        output_file = detector.save_results(results)
        
        print("\n" + "="*60)
        print(" OBJECT DETECTION COMPLETE!")
        print("="*60)
        print(f"\n Results saved to: {output_file}")
        print("\n Next steps:")
        print("  1. Load results to PostgreSQL")
        print("  2. Create dbt model for image analysis")
        print("  3. Integrate with existing star schema")
    else:
        print("\n No results generated. Check logs/yolo_detection.log")

if __name__ == "__main__":
    main()
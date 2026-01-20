import os
import csv
import logging
from pathlib import Path
from ultralytics import YOLO
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
IMAGES_DIR = DATA_DIR / "raw" / "images"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_CSV = PROCESSED_DIR / "yolo_results.csv"

# Create processed directory if it doesn't exist
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# YOLOv8 classes (COCO dataset)
# 0: person
# 39: bottle
# 41: cup
# 45: bowl
PRODUCT_CLASSES = [39, 41, 45] # Simple heuristic for medical/pharmacy products (bottles, etc.)

def classify_image(results):
    """
    Classify image based on detected objects.
    - promotional: Contains person + product
    - product_display: Contains product, no person
    - lifestyle: Contains person, no product
    - other: Neither detected
    """
    detected_classes = []
    has_person = False
    has_product = False
    
    for r in results:
        boxes = r.boxes
        for cls in boxes.cls:
            class_id = int(cls)
            detected_classes.append(class_id)
            if class_id == 0:
                has_person = True
            if class_id in PRODUCT_CLASSES:
                has_product = True
    
    if has_person and has_product:
        return "promotional", detected_classes
    elif has_product and not has_person:
        return "product_display", detected_classes
    elif has_person and not has_product:
        return "lifestyle", detected_classes
    else:
        return "other", detected_classes

def main():
    logger.info("Starting YOLO detection...")
    
    # Load model
    model = YOLO("yolov8n.pt")
    
    results_list = []
    
    # Walk through image directory
    if not IMAGES_DIR.exists():
        logger.warning(f"Images directory not found: {IMAGES_DIR}")
        return

    image_count = 0
    for channel_dir in IMAGES_DIR.iterdir():
        if channel_dir.is_dir():
            channel_name = channel_dir.name
            for image_path in channel_dir.glob("*.jpg"):
                try:
                    message_id = image_path.stem # Filename is message_id.jpg
                    
                    # Run inference
                    results = model(str(image_path), verbose=False)
                    
                    # Classify
                    category, detected_cls = classify_image(results)
                    
                    # Get max confidence (simple metric)
                    max_conf = 0.0
                    if results[0].boxes.conf.numel() > 0:
                        max_conf = float(results[0].boxes.conf.max())

                    results_list.append({
                        "message_id": message_id,
                        "channel_name": channel_name,
                        "image_path": str(image_path.relative_to(BASE_DIR)),
                        "detected_classes": str(list(set(detected_cls))), # Unique classes
                        "confidence_score": max_conf,
                        "image_category": category
                    })
                    
                    image_count += 1
                    if image_count % 10 == 0:
                        logger.info(f"Processed {image_count} images...")
                        
                except Exception as e:
                    logger.error(f"Error processing {image_path}: {e}")

    # Save to CSV
    if results_list:
        df = pd.DataFrame(results_list)
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"YOLO detection complete. Results saved to {OUTPUT_CSV}")
        logger.info(f"Total images processed: {image_count}")
    else:
        logger.info("No images processed or no results generated.")

if __name__ == "__main__":
    main()

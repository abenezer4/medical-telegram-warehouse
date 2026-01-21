import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_db_engine() -> Engine:
    """Create a SQLAlchemy engine from environment variables."""
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    db = os.getenv('POSTGRES_DB')
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def create_raw_schema(engine: Engine):
    """Create the raw schema if it doesn't exist."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    logger.info("Checked/Created raw schema.")

def load_json_to_postgres():
    """Read JSON files from data lake and load into PostgreSQL raw schema."""
    engine = get_db_engine()
    create_raw_schema(engine)
    
    base_path = Path("data/raw/telegram_messages")
    if not base_path.exists():
        logger.error(f"Base path {base_path} does not exist.")
        return

    all_messages = []
    
    # Iterate through date partitions
    for date_dir in base_path.iterdir():
        if date_dir.is_dir():
            # Iterate through channel JSON files
            for json_file in date_dir.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_messages.extend(data)
                        else:
                            all_messages.append(data)
                except Exception as e:
                    logger.error(f"Error reading {json_file}: {e}")

    if not all_messages:
        logger.warning("No messages found to load.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_messages)
    
    # Clean up DataFrame: handle potential missing columns or data types
    # Ensure message_date is handled correctly if needed, or leave as string for dbt to cast
    
    # Load to PostgreSQL
    try:
        df.to_sql(
            'telegram_messages', 
            engine, 
            schema='raw', 
            if_exists='replace', 
            index=False
        )
        logger.info(f"Successfully loaded {len(df)} messages into raw.telegram_messages table.")
    except Exception as e:
        logger.error(f"Error loading telegram_messages to database: {e}")

def load_yolo_to_postgres():
    """Read YOLO results CSV and load into PostgreSQL raw schema."""
    engine = get_db_engine()
    
    csv_path = Path("data/processed/yolo_results.csv")
    if not csv_path.exists():
        logger.warning(f"YOLO results CSV {csv_path} does not exist. Skipping.")
        return

    try:
        df = pd.read_csv(csv_path)
        df.to_sql(
            'yolo_detections', 
            engine, 
            schema='raw', 
            if_exists='replace', 
            index=False
        )
        logger.info(f"Successfully loaded {len(df)} YOLO detections into raw.yolo_detections table.")
    except Exception as e:
        logger.error(f"Error loading yolo_detections to database: {e}")

if __name__ == "__main__":
    load_json_to_postgres()
    load_yolo_to_postgres()

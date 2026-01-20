import os
import json
import asyncio
import argparse
import logging
import sys
import random
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageMediaPhoto

# =============================================================================
# CONFIGURATION
# =============================================================================

load_dotenv()

# Validate required environment variables before proceeding
api_id_str = os.getenv("TG_API_ID")
api_hash = os.getenv("TG_API_HASH")

if not api_id_str or not api_hash:
    print("ERROR: Missing TG_API_ID or TG_API_HASH in .env file")
    print("Create a .env file with:")
    print("  TG_API_ID=your_api_id")
    print("  TG_API_HASH=your_api_hash")
    sys.exit(1)

api_id = int(api_id_str)

# Date string for partitioning output files
TODAY = datetime.today().strftime("%Y-%m-%d")

# Stealth delays (seconds)
DEFAULT_CHANNEL_DELAY_MIN = 30.0
DEFAULT_CHANNEL_DELAY_MAX = 60.0
DEFAULT_MESSAGE_DELAY_MIN = 2.0
DEFAULT_MESSAGE_DELAY_MAX = 5.0

# =============================================================================
# LOGGING SETUP
# =============================================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging to both file and console
logger = logging.getLogger("telegram_scraper")
logger.setLevel(logging.INFO)

# File handler - logs everything to file
file_handler = logging.FileHandler(
    os.path.join(LOG_DIR, f"scrape_{TODAY}.log"),
    encoding="utf-8"
)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Console handler - shows progress in terminal
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def write_channel_messages_json(base_path: str, date_str: str, channel_name: str, messages: List[dict]) -> None:
    """
    Write messages for a specific channel to a JSON file.
    
    Args:
        base_path: Base directory for output
        date_str: Date string for partitioning (YYYY-MM-DD)
        channel_name: Name of the channel
        messages: List of message dictionaries
    """
    json_dir = os.path.join(base_path, "raw", "telegram_messages", date_str)
    os.makedirs(json_dir, exist_ok=True)
    
    json_file_path = os.path.join(json_dir, f"{channel_name}.json")
    
    # Load existing data if file exists
    existing_data = []
    if os.path.exists(json_file_path):
        with open(json_file_path, 'r', encoding='utf-8') as f:
            try:
                existing_data = json.load(f)
            except json.JSONDecodeError:
                existing_data = []
    
    # Append new messages
    existing_data.extend(messages)
    
    # Write back to file
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)

def write_manifest(base_path: str, date_str: str, channel_message_counts: dict) -> None:
    """
    Write a manifest file with information about what was scraped.
    
    Args:
        base_path: Base directory for output
        date_str: Date string for partitioning (YYYY-MM-DD)
        channel_message_counts: Dictionary mapping channel names to message counts
    """
    manifest_dir = os.path.join(base_path, "raw", "manifests", date_str)
    os.makedirs(manifest_dir, exist_ok=True)
    
    manifest_path = os.path.join(manifest_dir, "manifest.json")
    
    manifest_data = {
        "date": date_str,
        "channels_scraped": channel_message_counts,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest_data, f, indent=2)

# =============================================================================
# SCRAPING FUNCTIONS
# =============================================================================

async def scrape_channel(
    client: TelegramClient,
    channel: str,
    base_path: str,
    date_str: str,
    limit: int = 100,
) -> int:
    """
    Scrape a single Telegram channel and save messages + images.
    """
    channel_name = channel.strip('@')
    retries = 0
    max_retries = 3
    
    while True:
        try:
            entity = await client.get_entity(channel)
            channel_title = entity.title
            messages = []

            channel_image_dir = os.path.join(base_path, "raw", "images", channel_name)
            os.makedirs(channel_image_dir, exist_ok=True)

            logger.info(f"Starting stealth scrape of {channel} (limit={limit})")

            # Batching to avoid detection
            messages_in_batch = 0
            batch_limit = 20
            total_scraped_in_session = 0

            async for message in client.iter_messages(entity, limit=limit):
                image_path: Optional[str] = None
                # ... (rest of processing) ...
                
                messages_in_batch += 1
                total_scraped_in_session += 1
                
                if total_scraped_in_session % 100 == 0:
                    long_break = random.uniform(120, 300)
                    logger.info(f"Scraped 100 messages. Taking a LONG safety break ({long_break/60:.1f} min)...")
                    await asyncio.sleep(long_break)
                elif messages_in_batch >= batch_limit:
                    breather = random.uniform(15, 30)
                    logger.info(f"Taking a {breather:.1f}s batch breather...")
                    await asyncio.sleep(breather)
                    messages_in_batch = 0
                else:
                    # Random jitter per message
                    await asyncio.sleep(random.uniform(DEFAULT_MESSAGE_DELAY_MIN, DEFAULT_MESSAGE_DELAY_MAX))

            write_channel_messages_json(base_path, date_str, channel_name, messages)
            logger.info(f"Finished {channel}: {len(messages)} messages saved")
            
            # Large delay between channels
            await asyncio.sleep(random.uniform(DEFAULT_CHANNEL_DELAY_MIN, DEFAULT_CHANNEL_DELAY_MAX))
            return len(messages)

        except FloodWaitError as e:
            wait_time = e.seconds + 5
            logger.warning(f"FloodWait: sleeping {wait_time}s")
            await asyncio.sleep(wait_time)
            retries += 1
            if retries > max_retries: return 0
        except Exception as e:
            logger.error(f"Error scraping {channel}: {e}")
            return 0


async def scrape_all_channels(
    client: TelegramClient,
    channels: List[str],
    base_path: str,
    limit: int = 100,
) -> dict:
    """
    Scrape multiple Telegram channels and organize output.
    """
    await client.start()
    logger.info(f"Authenticated. Scraping {len(channels)} channels stealthily...")

    stats = {}
    channel_counts = {}

    for channel in channels:
        count = await scrape_channel(
            client=client,
            channel=channel,
            base_path=base_path,
            date_str=TODAY,
            limit=limit,
        )
        stats[channel] = count
        channel_counts[channel.strip("@")] = count

    write_manifest(base_path, TODAY, channel_counts)
    logger.info(f"Scraping complete. Total: {sum(stats.values())}")
    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="data")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    client = TelegramClient("telegram_scraper_session", api_id, api_hash)
    
    target_channels = [
        # '@CheMed123', 
        '@lobelia4cosmetics',
        '@tikvahpharma',
        '@Thequorachannel',
    ]

    async def main():
        async with client:
            await scrape_all_channels(client, target_channels, args.path, args.limit)

    asyncio.run(main())
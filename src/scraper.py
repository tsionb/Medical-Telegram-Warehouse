"""
Telegram Scraper for Medical Channels
Extract data from Telegram channels
"""
import os
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError
import time

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TelegramScraper:
    def __init__(self):
        """Initialize the scraper with credentials"""
        self.api_id = int(os.getenv('API_ID'))
        self.api_hash = os.getenv('API_HASH')
        self.phone_number = os.getenv('PHONE_NUMBER')
        
        # Telegram channels to scrape
        self.channels = [
            'CheMed123',        # CheMed Telegram Channel
            'lobelia4cosmetics',      # Medical products
            'tikvahpharma',          # Pharmaceuticals
            'EAHCI',
            'tenamereja'                    
            # more channels from et.tgstat.com/medicine
        ]
        
        # Create directories
        self.create_directories()
        
        logger.info(f"Initialized scraper for {len(self.channels)} channels")
    
    def create_directories(self):
        """Create all necessary directories for data storage"""
        directories = [
            'data/raw/images',
            'data/raw/telegram_messages',
            'logs'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")
    
    async def scrape_channel(self, client, channel_name, max_messages=50):
        """
        Scrape messages from a single Telegram channel
        
        Args:
            client: TelegramClient instance
            channel_name: Username of the channel
            max_messages: Maximum number of messages to scrape
        """
        logger.info(f"Starting to scrape channel: @{channel_name}")
        
        try:
            # Get channel entity
            channel = await client.get_entity(channel_name)
            logger.info(f"Found channel: {channel.title}")
            
            messages_data = []
            image_count = 0
            
            # Scrape messages
            async for message in client.iter_messages(channel, limit=max_messages):
                try:
                    # Extract message information
                    message_info = self.extract_message_info(message, channel_name)
                    
                    # Download image if present
                    if message.photo:
                        image_path = await self.download_image(client, message, channel_name)
                        if image_path:
                            message_info['image_path'] = image_path
                            image_count += 1
                    
                    messages_data.append(message_info)
                    
                    # Small delay to avoid rate limits
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    continue
            
            # Save to JSON
            if messages_data:
                self.save_to_json(messages_data, channel_name)
                logger.info(f" Scraped {len(messages_data)} messages, {image_count} images from @{channel_name}")
            
            return messages_data
            
        except Exception as e:
            logger.error(f" Error scraping @{channel_name}: {e}")
            return []
    
    def extract_message_info(self, message, channel_name):
        """Extract relevant information from a Telegram message"""
        return {
            'message_id': message.id,
            'channel_name': channel_name,
            'message_date': message.date.isoformat() if message.date else None,
            'message_text': message.text or '',
            'has_media': message.media is not None,
            'views': message.views or 0,
            'forwards': message.forwards or 0,
            'scraped_at': datetime.now().isoformat()
        }
    
    async def download_image(self, client, message, channel_name):
        """Download image from a message"""
        try:
            # Create image directory
            image_dir = Path(f"data/raw/images/{channel_name}")
            image_dir.mkdir(parents=True, exist_ok=True)
            
            # Download image
            image_path = image_dir / f"{message.id}.jpg"
            await message.download_media(file=str(image_path))
            
            logger.debug(f"Downloaded image: {image_path}")
            return str(image_path)
            
        except Exception as e:
            logger.error(f"Failed to download image for message {message.id}: {e}")
            return None
    
    def save_to_json(self, messages, channel_name):
        """Save scraped messages to JSON file"""
        # Get today's date for folder naming
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Create date directory
        date_dir = Path(f"data/raw/telegram_messages/{today}")
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        file_path = date_dir / f"{channel_name}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved data to: {file_path}")
        return str(file_path)
    
    async def run(self):
        """Main function to run the scraper"""
        logger.info(" Starting Telegram Scraper...")
        
        # Initialize Telegram client
        client = TelegramClient('session', self.api_id, self.api_hash)
        
        try:
            # Connect to Telegram
            await client.start(phone=self.phone_number)
            logger.info(" Connected to Telegram!")
            
            # Scrape each channel
            for channel in self.channels:
                try:
                    await self.scrape_channel(client, channel, max_messages=50)
                    
                    # Wait between channels to avoid rate limits
                    logger.info(f"Waiting 5 seconds before next channel...")
                    await asyncio.sleep(5)
                    
                except FloodWaitError as e:
                    logger.warning(f"Rate limited. Waiting {e.seconds} seconds...")
                    await asyncio.sleep(e.seconds)
                    continue
                except Exception as e:
                    logger.error(f"Failed to scrape {channel}: {e}")
                    continue
            
            logger.info(" Scraping completed successfully!")
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            await client.disconnect()

def main():
    """Entry point for the scraper"""
    print("="*50)
    print("TELEGRAM MEDICAL CHANNEL SCRAPER")
    print("="*50)
    
    # Check if credentials are set
    if not os.getenv('API_ID') or not os.getenv('API_HASH'):
        print("ERROR: API credentials not found!")
        print("Please add API_ID and API_HASH to your .env file")
        print("Check the .env.example file for reference")
        return
    
    print(f"Channels to scrape: {['chemed_ethiopia', 'lobelia4cosmetics', 'tikvahpharma']}")
    print(f"Data will be saved to: data/raw/")
    print("\nStarting in 3 seconds...")
    time.sleep(3)
    
    # Run the scraper
    scraper = TelegramScraper()
    asyncio.run(scraper.run())

if __name__ == "__main__":
    main()
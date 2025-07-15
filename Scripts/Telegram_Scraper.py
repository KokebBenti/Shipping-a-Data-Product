import json
import os
from datetime import datetime
from telethon.sync import TelegramClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="C:/Users/dell/PycharmProjects/Shipping-a-Data-Product/.env")

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL1 = 'lobelia4cosmetics'
CHANNEL2 = 'CheMed123'

#create folders
today = datetime.today().strftime('%Y-%m-%d')
BASE_DIR = os.path.join("Data/Raw Data", today)
BASE_DIR = os.path.join(BASE_DIR,CHANNEL2)
IMAGE_DIR = os.path.join(BASE_DIR, "Images")
os.makedirs(IMAGE_DIR, exist_ok=True)

# Create a Telethon client
client = TelegramClient("session", API_ID, API_HASH)


async def main():
    await client.start(PHONE)

    messages = []

    async for message in client.iter_messages(CHANNEL2, limit=100):
        msg_data = {
            "id": message.id,
            "date": str(message.date),
            "sender_id": message.sender_id,
            "text": message.message,
        }

        # Check and download media
        if message.media:
            image_path = os.path.join(IMAGE_DIR, f"{message.id}.jpg")
            try:
                await message.download_media(file=image_path)
                msg_data["media"] = image_path
            except Exception as e:
                msg_data["media_error"] = str(e)

        messages.append(msg_data)

    # Save messages as JSON in the base directory
    json_path = os.path.join(BASE_DIR, "messages.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(messages)} messages to {json_path}")

with client:
    client.loop.run_until_complete(main())
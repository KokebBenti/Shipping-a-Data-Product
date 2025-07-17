# import
import pg8000.dbapi as pg
import os
import json
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/dell/PycharmProjects/Shipping-a-Data-Product/.env")

user=os.getenv('POSTGRES_USER')
password=os.getenv('POSTGRES_PASSWORD')
host="localhost"
port=5432
database=os.getenv('POSTGRES_DB')
raw1 = "Data/Raw Data/2025-07-15/lobelia4cosmetics/messages.json"
raw2 = "Data/Raw Data/2025-07-15/CheMed123/messages.json"

def load_json_to_postgres(json_path,channel_name):
    print(f"Loading JSON file: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        messages = json.load(f)
    conn = pg.Connection(user=user,password=password, host=host,port=port, database=database)
    cur = conn.cursor()
    # Create schema and table if not exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.messages (
            message_id INTEGER PRIMARY KEY,
            sender_id BIGINT,
            date TIMESTAMP,
            text TEXT,
            media_file TEXT,
            channel TEXT
        );
    """)

    # Insert each message
    for msg in messages:
        cur.execute("""
            INSERT INTO raw.messages (message_id, sender_id, date, text, media_file,channel)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id) DO NOTHING;
        """, (
            msg.get("id"),
            msg.get("sender_id"),
            msg.get("date"),
            msg.get("text"),
            msg.get("media"),
            channel_name
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(messages)} messages into raw.messages")

if __name__ == "__main__":
    load_json_to_postgres(raw1,"lobelia4cosmetics")
    load_json_to_postgres(raw2,'CheMed123')

from sqlalchemy import create_engine
engine=create_engine(f'postgresql+pg8000://{user}:{password}@{host}:{5432}/{database}')

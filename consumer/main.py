import os
from dotenv import load_dotenv
import json
from datetime import datetime
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# DB Configurations from ENV
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
BASE = declarative_base()

class CryptoMarketDataBase(BASE):
    __tablename__ = "crypto_market_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False)
    coin = Column(String(50), nullable=False)
    price_usd = Column(Float, nullable=False)
    source = Column(String(60), nullable=False)

BASE.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-market-data")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')), 
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-process'
)

print("Listening for messages...")

for message in consumer:
    raw_payload = message.value
    try:
        ingested_at = datetime.fromtimestamp(raw_payload['ingested_at'])
        source = raw_payload["source"]
        crypto_data = raw_payload['raw_data']

        for coin, prices in crypto_data.items():
            price = prices.get('usd')
            if price is not None:
                # Create a structured record
                record = CryptoMarketDataBase(
                    timestamp=ingested_at,
                    coin=coin,
                    price_usd=price,
                    source=source
                )
                session.add(record)
        session.commit()
        print(f"Processed and stored Silver records for {ingested_at}")
    
    except Exception as e:
        print(f"Error processing message: {e}")
        session.rollback()


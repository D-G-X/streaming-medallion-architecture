import os
from typing import Dict
from dotenv import load_dotenv
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError
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
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead-letter-queue")


class CoinPrice(BaseModel):
    usd: float 

class RawPayload(BaseModel):
    ingested_at: float 
    source: str
    raw_data: Dict[str, CoinPrice]

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')), 
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-process'
)

# We need a producer to send bad messages to the DLQ
dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Listening for messages...")

try:
    for message in consumer:
        raw_payload = message.value
        try:
            validated_data = RawPayload(**raw_payload)
            ingested_at = datetime.fromtimestamp(raw_payload['ingested_at'])
            source = raw_payload["source"]
            crypto_data = raw_payload['raw_data']

            for coin, prices in crypto_data.items():
                print(coin)
                price = prices.get('usd')
                if price is not None:
                    record = CryptoMarketDataBase(
                        timestamp=ingested_at,
                        coin=coin,
                        price_usd=price,
                        source=source
                    )
                    session.add(record)
            session.commit()
            print(f"Processed and stored records for {ingested_at}")
        
        except ValidationError as e:
                print(f"Invalid data detected! Routing to DLQ...")
                
                dlq_message = {
                    "error_type": "SchemaValidationError",
                    "error_details": e.errors(),
                    "original_payload": raw_payload
                }
                
                # Send to the Dead Letter Queue topic
                dlq_producer.send(DLQ_TOPIC, value=dlq_message)
                dlq_producer.flush()
                print(f"Sent to DLQ: {dlq_message}")
                session.rollback()
        
        except Exception as e:
            print(f"Error processing message: {e}")
            session.rollback()
except KeyboardInterrupt:
    print("Stopping consumer!!")
finally:
    print("Cleaning up connections...")
    consumer.close()
    dlq_producer.close()
    session.close()

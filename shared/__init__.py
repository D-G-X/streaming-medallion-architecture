import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-market-data")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead-letter-queue")

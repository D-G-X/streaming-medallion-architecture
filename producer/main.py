import asyncio
import json
import os
import time
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer

load_dotenv()

app = FastAPI(title="Market Data Producer")

# Configuration from environment
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "raw-market-data")
COINGECKO_API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

is_streaming = False
fetch_time = time.time()

async def fetch_and_publish():
    """Background task to fetch data and push to Kafka."""
    global is_streaming, fetch_time
    async with httpx.AsyncClient() as client:
        while is_streaming:
            try:
                response = await client.get(COINGECKO_API_URL)
                data = response.json()
                
                payload = {
                    "ingested_at": time.time(),
                    "source": "coingecko",
                    "raw_data": data
                }
                fetch_time = time.time()
                
                # Publish to Redpanda
                producer.send(TOPIC_NAME, payload)
                print(f"Published to {TOPIC_NAME}: {payload}")
                
            except Exception as e:
                print(f"Error fetching/publishing data: {e}")
            
            await asyncio.sleep(FETCH_INTERVAL)

@app.post("/start")
async def start_streaming(background_tasks: BackgroundTasks):
    """Endpoint to start the data stream."""
    global is_streaming
    if not is_streaming:
        is_streaming = True
        background_tasks.add_task(fetch_and_publish)
        return {"status": "Streaming started"}
    return {"status": "Already streaming"}

@app.get("/next_fetch_time")
async def next_fetch_time():
    """Endpoint to know when the next API call to fetch will occur."""
    global fetch_time, is_streaming
    next_fetch = fetch_time + FETCH_INTERVAL
    if not is_streaming:
        return {"status":"Not Streaming"}
    return {
        "status": "Streaming",
        "next_fetch_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(next_fetch)),
        "seconds_remaining": max(0, int(next_fetch - time.time()))
    }

@app.post("/stop")
async def stop_streaming():
    """Endpoint to stop the data stream."""
    global is_streaming
    is_streaming = False
    return {"status": "Streaming stopped"}
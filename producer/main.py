import asyncio
import json
import time
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer
from shared import KAFKA_BROKER, TOPIC_NAME
from producer.config import COINGECKO_API_URL, FETCH_INTERVAL, MAX_RETRIES, RETRY_DELAY

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    producer.flush()
    producer.close()
    print("Kafka producer closed.")

app = FastAPI(title="Market Data Producer", lifespan=lifespan)

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
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    response = await client.get(COINGECKO_API_URL)
                    response.raise_for_status()
                    data = response.json()
                    
                    payload = {
                        "ingested_at": time.time(),
                        "source": "coingecko",
                        "raw_data": data
                    }
                    fetch_time = time.time()
                    
                    # Publish to Redpanda
                    producer.send(TOPIC_NAME, payload)
                    producer.flush()
                    print(f"Published to {TOPIC_NAME}: {payload}")
                    break
                    
                except httpx.HTTPStatusError as e:
                    print(f"API error (attempt {attempt}/{MAX_RETRIES}): {e.response.status_code}")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
                except Exception as e:
                    print(f"Error (attempt {attempt}/{MAX_RETRIES}): {e}")
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY)
            
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
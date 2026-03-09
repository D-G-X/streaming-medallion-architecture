from kafka import KafkaProducer
import json

print("Connecting to Redpanda...")
producer = KafkaProducer(
    bootstrap_servers="localhost:19092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

poison_message = {
    "ingested_at": 1710000000.0,
    "source": "coingecko",
    "raw_data": {"bitcoin": {"wrong_key": 68000}}
}

print("Injecting poison message into raw-market-data topic...")
producer.send("raw-market-data", value=poison_message)
producer.flush()
print("Message sent successfully!")
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError
from consumer.db import engine, BASE, Session
from consumer.models import CryptoMarketData
from consumer.schemas import RawPayload
from shared import KAFKA_BROKER, TOPIC_NAME, DLQ_TOPIC

BASE.metadata.create_all(engine)
session = Session()

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')), 
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='consumer-process'
)

# A producer to send bad messages to the DLQ
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
                    record = CryptoMarketData(
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

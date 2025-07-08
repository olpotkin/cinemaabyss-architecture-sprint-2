from kafka import KafkaProducer, KafkaConsumer, errors as kafka_errors
import json
import threading
import time

TOPIC_MAP = {
    "user": "user-events",
    "payment": "payment-events",
    "movie": "movie-events"
}

def init_producer_with_retry(retries=5, delay=5):
    for i in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5
            )
            print("[Kafka] Producer initialized")
            return producer
        except kafka_errors.KafkaError as e:
            print(f"[Kafka Retry {i}/{retries}] Failed to initialize producer: {e}")
            time.sleep(delay)
    raise RuntimeError("Kafka connection failed after retries.")

producer = init_producer_with_retry()


def produce_event(event_type: str, payload: dict):
    topic = TOPIC_MAP.get(event_type)
    if not topic:
        print(f"[Error] Unknown event type: '{event_type}'")
        raise ValueError(f"Unknown event type: {event_type}")

    try:
        print(f"[Producer] Publishing to topic '{topic}': {payload}")
        future = producer.send(topic, value=payload)
        future.get(timeout=10)
        producer.flush()
    except kafka_errors.KafkaTimeoutError as e:
        print(f"[Kafka Timeout] Failed to send message: {e}")
    except kafka_errors.KafkaError as e:
        print(f"[Kafka Error] Failed to produce message: {e}")
    except Exception as e:
        print(f"[Error] Unexpected error in producer: {e}")


def consume_events():
    def _consume():
        try:
            consumer = KafkaConsumer(
                *TOPIC_MAP.values(),
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='event-consumer-group',
                consumer_timeout_ms=10000
            )
            print(f"[Consumer] Subscribed to topics: {', '.join(TOPIC_MAP.values())}")
        except kafka_errors.KafkaError as e:
            print(f"[Kafka Error] Failed to start consumer: {e}")
            return

        try:
            for msg in consumer:
                print(f"[Consumer] Received from '{msg.topic}': {msg.value}")
        except kafka_errors.KafkaError as e:
            print(f"[Kafka Error] Consumer error: {e}")
        except Exception as e:
            print(f"[Error] Unexpected error in consumer: {e}")

    thread = threading.Thread(target=_consume, daemon=True)
    thread.start()

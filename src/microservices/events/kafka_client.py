from confluent_kafka import Producer, Consumer, KafkaError
import threading
import logging

producer = Producer({'bootstrap.servers': 'kafka:9092'})

def produce_event(event_type: str, payload: dict):
    topic = f"{event_type}-topic"
    producer.produce(topic, str(payload))
    producer.flush()

def consume_events():
    def consume_loop():
        consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'cinemaabyss-events',
            'auto.offset.reset': 'earliest'
        })

        consumer.subscribe(['user-topic', 'payment-topic', 'movie-topic'])

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                continue
            print(f"Consumed event from topic {msg.topic()}: {msg.value().decode()}")

    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()

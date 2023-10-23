import json
import time
from datetime import datetime
from kafka import KafkaConsumer

# Kafka broker configuration
kafka_config = {
    'bootstrap_servers': 'localhost:9092',  # Change to your Kafka broker's address
    'group_id': 'unibo-group',
    'auto_offset_reset': 'latest',  # If it's 'earliest' it will start from the beginning of the topic
    'value_deserializer': lambda m: json.loads(m.decode('utf-8')) # To decode messages in utf-8
}

# Topic to subscribe to
TOPIC = 'test-topic'

def start_consumer():
    # Create a Kafka consumer instance
    consumer = KafkaConsumer(TOPIC, **kafka_config)

    # Continuously consume and print messages
    for message in consumer:
        current_time = datetime.now()
        timestamp = current_time.strftime("%Y-%m-%d-%H-%M-%S-%f")

        print(f"[{timestamp}] Received message: {message.value}")

if __name__ == "__main__":
    start_consumer()

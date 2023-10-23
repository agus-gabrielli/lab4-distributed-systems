import json
import time
from datetime import datetime
import random
from kafka import KafkaProducer

INTERVAL_SECONDS = 1 # Send a message with interval of X seconds

# Kafka broker configuration
kafka_config = {
    'bootstrap_servers': 'localhost:9092',  # The address of the Kafka broker
    'value_serializer': lambda v: json.dumps(v).encode('utf-8') # To encode messages in utf-8
}

# Topic to send messages to
TOPIC = 'test-topic'

# Sends a given JSON message from producer to the Kafka topic
def send_message(producer, message):
    producer.send(TOPIC, value=message)
    producer.flush() # Sends the message instantly, without waiting for the buffer to fill
    print(f"Message sent: {message}")

# Creates a random message including a timestamp
def create_message():
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d-%H-%M-%S-%f")

    return {
        'random_number_1': random.randint(1, 1000),
        'random_number_2': random.randint(1, 1000),
        'message_timestamp': timestamp # So as to know when it was produced
    }

def start_producer():
    # Create a Kafka producer instance
    producer = KafkaProducer(**kafka_config)

    # Main loop to send messages at regular intervals
    while True:
        message = create_message()

        send_message(producer, message)

        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    start_producer()
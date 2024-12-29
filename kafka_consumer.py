from confluent_kafka import Consumer
from pymongo import MongoClient
import json

# Kafka Consumer setup
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['test-topic'])

# MongoDB setup
client = MongoClient('localhost', 27017)  # Connect to MongoDB on localhost
db = client['real_time_data']  # Database name
collection = db['sensor_data']  # Collection name

# Consume and store messages in MongoDB
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll Kafka topic
        if msg is None:
            continue  # No new message, continue polling
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        # Deserialize the message value
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Storing: {data}")
        
        # Insert the message into MongoDB
        collection.insert_one(data)

finally:
    consumer.close()  

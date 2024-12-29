from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(10):
    data = {"temperature": 20 + i, "humidity": 50 - i}
    producer.produce('test-topic', key=str(i), value=json.dumps(data), callback=delivery_report)
    producer.flush()
    print(f"Sent: {data}")
    time.sleep(1)

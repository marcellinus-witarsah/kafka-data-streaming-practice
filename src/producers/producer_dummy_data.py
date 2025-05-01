import json
import time
from kafka import KafkaProducer


def json_serializer(data: dict) -> bytes:
    return json.dumps(data).encode('utf-8')

def main(localhost: str):
    producer = KafkaProducer(bootstrap_servers=localhost, value_serializer=json_serializer)
    topic = 'dummy_topic'
    
    for i in range(1000):
        # Dummy data to send
        data = {
            "event_number": i,
            "event_unix_time": time.time() * 1000
        }
        print(f"Sending data: {data}")
        # Convert data to JSON and send to Kafka
        producer.send(topic, data)
        
    producer.flush()
    producer.close()

if __name__ == "__main__":
    localhost = 'localhost:9092'
    main(localhost=localhost)
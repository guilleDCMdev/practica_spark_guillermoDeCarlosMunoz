from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def generate_message_with_errors():
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": random.choice([random.randint(1, 100), None]),
        "product_id": random.choice([fake.uuid4(), None, '']),
        "quantity_sold": random.choice([random.randint(1, 20), None]),
        "revenue": random.choice([round(random.uniform(100.0, 1000.0), 2), None, 'not a number'])
    }
    
    if random.random() < 0.1:
        message['timestamp'] = 'invalid_timestamp'

    return message

while True:
    message = generate_message_with_errors()
    producer.send('sales_stream', value=message)
    print(f"Sent message with possible errors: {message}")
    sleep(1)

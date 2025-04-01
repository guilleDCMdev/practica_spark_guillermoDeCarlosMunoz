from time import sleep
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

while True:
    message = {
        "timestamp": int(datetime.now().timestamp() * 1000),
        "store_id": random.randint(1, 100),
        "product_id": fake.uuid4(),  
        "quantity_sold": random.randint(1, 20),  
        "revenue": round(random.uniform(100.0, 1000.0), 2)  
    }
    producer.send('sales_stream', value=message)
    print(f"Sent message: {message}")
    sleep(1)

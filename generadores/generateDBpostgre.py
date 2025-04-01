import psycopg2
import random
import string
from faker import Faker
import os
from dotenv import load_dotenv

load_dotenv()

def getRandomString(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

def introduceAnomalies(value):
    anomalies = [None, '', 'NULL', 'ERROR']
    return random.choice(anomalies + [value]) if random.random() < 0.1 else value

def createTable():
    fake = Faker()
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="retail_db",
            user=os.getenv("DBUSER"),
            password=os.getenv("PASSWORD"),
            port=os.getenv("DBPORT")
        )
        createTableQuery = '''
        CREATE TABLE IF NOT EXISTS Stores (
            store_id SERIAL PRIMARY KEY,
            store_name VARCHAR(255),
            location VARCHAR(255),
            demographics VARCHAR(255)
        );'''
        
        with conn.cursor() as cur:
            cur.execute(createTableQuery)
        conn.commit()

        with conn.cursor() as cur:
            for _ in range(1000):
                storeName = introduceAnomalies(fake.company())
                location = introduceAnomalies(fake.city())
                demographics = introduceAnomalies(getRandomString(5))
                
                insertQuery = '''INSERT INTO Stores (store_name, location, demographics) VALUES (%s, %s, %s)'''
                cur.execute(insertQuery, (storeName, location, demographics))
            
            conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error: {error}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    createTable()
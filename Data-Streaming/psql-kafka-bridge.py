import psycopg2

from kafka import KafkaConsumer
from kafka import KafkaProducer

import time


# KAFKA Address
KAFKA_HOST = " 192.168.10.102:9092"


# PostgresSQL Settings
PS_DB_NAME  = "test"
PS_USERNAME = "postgres"
PS_PASSWORD = "1234"



def send_to_kafka():

    # Kafka Settings
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,api_version=(0,10,1))
    	
    # POSTGRESQL
    connection = psycopg2.connect(f"dbname={PS_DB_NAME} user={PS_USERNAME} password={PS_PASSWORD}")
    cursor = connection.cursor()
    
    data = cursor.execute("SELECT * FROM weather WHERE city = 'Oulu'").fetchall()
    print(data)


def fetch_from_kafka():

    # POSTGRESQL
    connection = psycopg2.connect(f"dbname={PS_DB_NAME} user={PS_USERNAME} password={PS_PASSWORD}")
    cursor = conn.cursor()
    
    # Kafka Settings
    kafka_consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST)
    
    for msg in kafka_consumer:
	    data = msg.value.decode()
	    cursor.execute(f"INSERT INTO weather (city, temp, date) VALUES ({data['city']}, {data['temp']}, {data['date']})")

	    cursor.commit()

	    time.sleep(0.1)


if __name__ == "__main__":
    send_to_kafka()


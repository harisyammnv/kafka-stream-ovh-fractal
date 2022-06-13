from kafka import KafkaProducer
from confluent_kafka.avro import AvroProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "dummy-credit-card-transactions"
KAFKA_BOOTSTRAP_SERVER_CONS = "141.95.96.132:9092"

if __name__ == "__main__":
    print("Simple Kafka Producer Application Started ...!!!")
    
    kafka_producer_obj = KafkaProducer(bootstrap_server = KAFKA_BOOTSTRAP_SERVER_CONS,
                                       value_serializer = lambda x: dumps(x).encode('utf-8'),
                                       acks="all")
    transaction_card_type = ["Visa", "MasterCard", "Maestro"]
    
    key = "test-key".encode("utf-8")
    
    message = None
    
    for i  in range(25):
        i = i+1
        message = {}
        print(f"Sending message {i} to the topic {KAFKA_TOPIC_NAME_CONS}")
        event_datetime = datetime.now()
        
        message["transaction_id"] = str(i)
        message["transaction_card_type"] = random.choice(transaction_card_type)
        message["transaction_amount"] = round(random.uniform(5.5, 555.5), 2)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H-%M-%S")
        print(f"Message to be sent: {message}")
        
        kafka_producer_obj.send(topic=KAFKA_TOPIC_NAME_CONS, key=key,
                                value=message)
        time.sleep(1)
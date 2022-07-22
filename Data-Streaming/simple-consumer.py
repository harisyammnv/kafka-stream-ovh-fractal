
from kafka import KafkaConsumer
from confluent_kafka.avro import AvroProducer
from datetime import datetime
import time
from json import dumps
import random


KAFKA_TOPIC_NAME_CONS= "my-topic-test3"
KAFKA_BOOTSTRAP_SERVER_CONS="kafka-bs.fractal-kafka.ovh:9094"
message= None
if __name__ == "__main__":
  print("Simple Kafka Consumer Application Started ...!!!")
  consumer= KafkaConsumer(KAFKA_TOPIC_NAME_CONS,
                          bootstrap_servers = KAFKA_BOOTSTRAP_SERVER_CONS,
                          auto_offset_reset='earliest',api_version=(0, 10, 1),
                          enable_auto_commit=True)

  
for i in consumer:
    
  print(i.value)
              

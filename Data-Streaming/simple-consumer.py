
from kafka import KafkaConsumer
from confluent_kafka.avro import AvroProducer
from datetime import datetime
import time
from json import dumps
import random


KAFKA_TOPIC_NAME_CONS= "my-topic-test1"
KAFKA_BOOTSTRAP_SERVER_CONS="141.95.96.119:9094"
message= None
if __name__ == "__main__":
  print("Simple Kafka Consumer Application Started ...!!!")
  consumer= KafkaConsumer(KAFKA_TOPIC_NAME_CONS,
                                      bootstrap_servers = KAFKA_BOOTSTRAP_SERVER_CONS,
                                                                  auto_offset_reset='earliest',api_version=(0, 10, 1),
                                                                 enable_auto_commit=True)

    #transaction_card_type = ["Visa", "MasterCard", "Maestro"]

    #key = "test-key".encode("utf-8")

for i in consumer:
    #print("%s:%d:%d:v=%s" %(i.topic))
  print(i.value)
  #print(f"Sending message {i} to the topic {KAFKA_TOPIC_NAME_CONS}")                                                                                                      event_datetime = datetime.now()

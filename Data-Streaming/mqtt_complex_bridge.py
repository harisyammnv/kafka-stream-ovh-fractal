
from kafka import KafkaProducer
import socket
import paho.mqtt.client as mqtt
from confluent_kafka import avro

from json import loads
from confluent_kafka.avro import AvroProducer
import csv

from time import sleep

# The Topic Name
TOPIC = "my-topic-test1"

# The address of Kafka server
KAFKA_HOST = "141.95.96.147:9094"

# Mqtt Address
MQTT_HOST = "localhost"

def load_avro_schema_from_file():
     key_schema = avro.load("schemas/taxi_ride_key.avsc")
     value_schema = avro.load("schemas/taxi_ride_value.avsc")

     return key_schema, value_schema




key_schema, value_schema = load_avro_schema_from_file()

producer_config = {
             "bootstrap.servers": "141.95.96.147:9094",
             "schema.registry.url": "http://141.95.96.148:8081",
         "acks": "1"
     }

producer = AvroProducer(producer_config,default_key_schema=key_schema, default_value_schema=value_schema)

 
     
     

# MQTT Settings
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
mqtt_client.connect(MQTT_HOST)


def on_message(client, userdata, message):
    i=0
    key={"vendorId": int(i)}
    i=i+1
    msg_payload = message.payload
    msg_payload = msg_payload.decode()
    print("Received MQTT message: ", msg_payload)
    producer.produce(topic=TOPIC, key=key ,value=loads(msg_payload))
    
    print("Send the message: " + msg_payload + f" to Kafka with topic {TOPIC}!")
    producer.flush()

    sleep(1)
mqtt_client.loop_start()
mqtt_client.subscribe("local_topic")

mqtt_client.on_message = on_message
sleep(100)
mqtt_client.loop_stop()




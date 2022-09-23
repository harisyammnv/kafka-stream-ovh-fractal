"""Summary:  In this file we are getting the data from mqtt client then 
   we will send this data on the kafka topic by the kafka producer"""
from confluent_kafka import Producer
from kafka import KafkaProducer
import socket
import paho.mqtt.client as mqtt
from confluent_kafka import avro
from json import loads
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
from pathlib import Path

# defined the kafka topics,bootstrep and Mqtt client to access the data from mqtt protocol.
TOPIC = "my-topic-test3"
KAFKA_BOOTSTRAP_SERVERS_CONS = "kafka-bs.fractal-kafka.ovh:9094"
MQTT_HOST = "localhost"
# Specifying Path
file_path = Path.home().joinpath("kafka-stream-ovh-fractal/Data-Streaming")
# loading the schema files for schema validation purpose.
def load_avro_schema_from_file():
    key_schema = avro.load(str(file_path) + "/" + "schemas/vehicle_ride_key.avsc")
    value_schema = avro.load(str(file_path) + "/" + "schemas/vehicle_ride_value.avsc")

    return key_schema, value_schema


key_schema, value_schema = load_avro_schema_from_file()
# producer Settings
producer_config = {
    "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
    "schema.registry.url": "http://152.228.251.146:8081",
    "acks": "1",
}
producer = AvroProducer(
    producer_config, default_key_schema=key_schema, default_value_schema=value_schema
)
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
# connecting the mqtt kafka produecr to kafka bridge .
mqtt_client.connect(MQTT_HOST)
# This function will used for producing the data on the kafka topics specify above.
def on_message(client, userdata, message):
    i = 0
    key = {"Time": float(i)}
    i = i + 1
    msg_payload = message.payload
    msg_payload = msg_payload.decode()
    print("Received MQTT message: ", msg_payload)
    producer.produce(topic=TOPIC, key=key, value=loads(msg_payload))
    print("Send the message: " + msg_payload + f" to Kafka with topic {TOPIC}!")
    producer.flush()


mqtt_client.loop_start()
mqtt_client.subscribe("local_topic")
mqtt_client.on_message = on_message
sleep(100)
mqtt_client.loop_stop()

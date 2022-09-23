import paho.mqtt.client as mqtt
from random import randint
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
import time
from json import dumps


# MQTT Address
HOST = "localhost"
# Mqtt Setting
mqtt_client = mqtt.Client("Random_Generator")
mqtt_client.connect(HOST)
# File contaiing the data that we processed further.
file = open("data/rides.csv")
csvreader = csv.reader(file)
header = next(csvreader)
# This loop will produce.
for row in csvreader:
    key = {"vendorId": int(row[0])}
    value = {
        "vendorId": int(row[0]),
        "passenger_count": int(row[3]),
        "trip_distance": float(row[4]),
        "payment_type": int(row[9]),
        "total_amount": float(row[16]),
    }
    # Dumping the data on the mqtt client and then this will connect with kafka topics
    mqtt_client.publish("local_topic", dumps(value))
    print("Send a message to MQTT: ", value, " to kafka")
    time.sleep(3)

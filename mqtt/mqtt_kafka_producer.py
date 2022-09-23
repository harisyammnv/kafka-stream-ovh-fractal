"""Summary: In this file the kafka produer producing  on the kafka topic then 
   we will connect this kafka topic data with kafka brodge by using the mqtt protocl."""
import paho.mqtt.client as mqtt
from random import randint
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
import time
from json import dumps


# MQTT Address
MQTT_HOST = "localhost"
# MQTT Settings
mqtt_client = mqtt.Client("Random_Generator")
mqtt_client.connect(MQTT_HOST)
# File containing the data that we processed further.
file = open("data/220129_Smart TMS_Cycles data_V4_2_processed.csv")
csvreader = csv.reader(file)
header = next(csvreader)
i = 0
# This loop will produce data.
for row in csvreader:
    key = {"Time": float(row[i + 8])}
    value = {
        "Time": float(row[i + 8]),
        "Latitude": float(row[i + 1]),
        "Longitude": float(row[i + 2]),
        "Distance": float(row[i + 3]),
        "Elevation": float(row[i + 4]),
    }
    # Dumping the data on the mqtt client and then this will connect with kafka topics
    mqtt_client.publish("local_topic", dumps(value))
    print("Send a message to MQTT: ", value, " to kafka")
    # taking sleep for 5 sec and then again restart all the things.
    time.sleep(5)

import paho.mqtt.client as mqtt
from random import randint
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
import time
from json import dumps


# MQTT Address
HOST = "localhost"

mqtt_client = mqtt.Client("Random_Generator")
mqtt_client.connect(HOST)  

file = open('data/220129_Smart TMS_Cycles data_V4_2_processed.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"BaseTime": float(row[8])}
    value = { 
              "BaseTime":  float(row[1]),
              "Latitude":  float(row[1]),
              "Longitude": float(row[1]),
              "Distance":  float(row[1]), 
              "Elevation": float(row[1])
    }
    
    mqtt_client.publish("local_topic",dumps(value))
    print("Send a message to MQTT: " ,value,  " to kafka")
    time.sleep(3)


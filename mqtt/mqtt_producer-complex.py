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
    key = {"BaseTime(s)": row[1]}
    value = {"BaseTime(s)": row[1],
              "Latitude": row[1]
             ,"Longitude": row[1], 
            "Distance (m)": row[1], 
            "Elevation (m)": row[1]}
    
    mqtt_client.publish("local_topic",dumps(value))
    print("Send a message to MQTT: " ,value,  " to kafka")
    time.sleep(3)


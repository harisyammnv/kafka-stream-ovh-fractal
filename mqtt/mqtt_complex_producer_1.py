import paho.mqtt.client as mqtt
from random import randint
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
import time
from json import dumps


# MQTT Address
HOST = "localhost"
#Mqtt Setting
mqtt_client = mqtt.Client("Random_Generator")
mqtt_client.connect(HOST)  
#File contaiing the data that we processed further.
file = open('data/220129_Smart TMS_Cycles data_V4_2_processed.csv')
csvreader = csv.reader(file)
header = next(csvreader)
#This loop will produce.
for row in csvreader:
    key = {"Time": float(row[8])}
    value = { 
            "Time":  float(row[1]),
            "T_Amb":  float(row[1]),
            "Phi_Amb": float(row[1]),
            "Road_Grad":  float(row[1]), 
            "Veh_Spd": float(row[1]),
            "Wind_Spd_Proj" : float(row[1]),
            "Solar_Rad" : float(row[1]),
            'DistanceChargeStation' : float(row[1]),
            'PowerConnector' : float(row[1])

    }
    #Dumping the data on the mqtt client and then this will connect with kafka topics 
    mqtt_client.publish("local_topic",dumps(value))
    print("Send a message to MQTT: " ,value,  " to kafka")
    time.sleep(5)


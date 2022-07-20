import paho.mqtt.client as mqtt

from random import randint
import time

# MQTT Address
HOST = "localhost"


mqtt_client = mqtt.Client("Random_Generator")
mqtt_client.connect(HOST)


while True:
    randNumber = randint(1, 1000)
    mqtt_client.publish("local_topic", randNumber)
    print("Send a message to MQTT: " + str(randNumber) + " to topic local_topic")
    
    time.sleep(3)



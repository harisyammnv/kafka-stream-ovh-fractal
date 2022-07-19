import paho.mqtt.client as paho
import sys
client=paho.Client()
if client.connect("localhost",port numnber,timeout variable)!=0:
    print("could not connect to MQTT Broker")
    sys.exit(-1)

client.publish("test/status","Hello world from paho-mqtt!",0)
client.disconnect()




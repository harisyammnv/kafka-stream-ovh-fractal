from kafka import KafkaProducer

import paho.mqtt.client as mqtt

import time

# The Topic Name
TOPIC = "my-topic-test3"

# The address of Kafka server
KAFKA_HOST = "141.95.96.27:9094"

# Mqtt Address
MQTT_HOST = "localhost"


# MQTT Settings
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
mqtt_client.connect(MQTT_HOST)


# Kafka Settings
kafka_producer = KafkaProducer(TOPIC,bootstrap_servers=KAFKA_HOST,api_version=(0,10,1))


def on_message(client, userdata, message):
    msg_payload = message.payload
    msg_payload = msg_payload.decode()
    print("Received MQTT message: ", msg_payload)
    kafka_producer.send(TOPIC, msg_payload.encode())
    print("Send the message: " + msg_payload + f" to Kafka with topic {TOPIC}!")

mqtt_client.loop_start()
mqtt_client.subscribe("local_topic")
mqtt_client.on_message = on_message
time.sleep(300)
mqtt_client.loop_end()


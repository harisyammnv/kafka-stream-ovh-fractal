
from confluent_kafka import Producer
from kafka import KafkaProducer
import socket
import paho.mqtt.client as mqtt
from confluent_kafka import avro
from json import loads
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
TOPIC = "my-topic-test1"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka-bs.fractal-kafka.ovh:9094'
MQTT_HOST = "localhost"

def load_avro_schema_from_file():
         key_schema = avro.load("/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/vehicle_ride_key.avsc")
         value_schema = avro.load("/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/vehicle_ride_value.avsc")

         return key_schema, value_schema

key_schema, value_schema = load_avro_schema_from_file()

producer_config = {
            "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "schema.registry.url": "http://152.228.251.146:8081",
            "acks": "1"
    }

producer = AvroProducer(producer_config,default_key_schema=key_schema, default_value_schema=value_schema)
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
mqtt_client.connect(MQTT_HOST)
file = open('/home/snehasuman/kafka-stream-ovh-fractal/Data-Streaming/data/220129_Smart TMS_Cycles data_V4_2_processed.csv')
csvreader = csv.reader(file)
csvreader=list(csvreader)
def on_message(client, userdata, message):
      i=0
      key={"Time": float(i)}
      i=i+1
      msg_payload = message.payload
      msg_payload = msg_payload.decode()
      print("Received MQTT message: ", msg_payload)
      producer.produce(topic=TOPIC, key=key ,value=loads(msg_payload))

      print("Send the message: " + msg_payload +f" to Kafka with topic {TOPIC}!")
      producer.flush()
 
mqtt_client.loop_start()
mqtt_client.subscribe("local_topic")
mqtt_client.on_message = on_message
sleep(300)
mqtt_client.loop_end()        
       

   



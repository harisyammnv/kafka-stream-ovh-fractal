'''Summary: In this file the kafka produer producing  on the kafka topic with avro schema files '''  
from kafka.producer import KafkaProducer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
from mqtt_complex_consumer_1 import *
#loading the schema files for schema validation purpose.
def load_avro_schema_from_file():
    key_schema = avro.load("schemas/vehicle_ride_key.avsc")
    value_schema = avro.load("schemas/vehicle_ride_value.avsc")
    return key_schema, value_schema

#This function  is used to validate the avro schema using the above files
def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
            "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh:8081",
            "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)
    #File containing the data that we will process further.
    file = read_messages.cfg
    #csvreader = csv.reader(file)
    header = next(r)
    
    for row in csvreader:
        #key = {"BaseTime(s)": row[1]}
        value = {"BaseTime": row[1],
              "Latitude": row[1]
             ,"Longitude": row[1], 
            "Distance": row[1], 
            "Elevation": row[1]}

        try:
            producer.produce(topic='my-topic-test3',  value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)

if __name__ == "__main__":
    send_record()
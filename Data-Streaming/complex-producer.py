'''Summary: In this file the kafka produer producing  on the kafka topic with avro schema files '''  
from kafka.producer import KafkaProducer
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep
from pathlib import Path

#Specifying path 
file_path=Path.home().joinpath('kafka-stream-ovh-fractal/mqtt')
#loading the schema files for schema validation purpose.
def load_avro_schema_from_file():  
    key_schema = avro.load(str(file_path)+'/'+'schemas/vehicle_ride_key.avsc')
    value_schema = avro.load(str(file_path)+'/'+'schemas/vehicle_ride_value.avsc')
    return key_schema, value_schema

#This function  is used to validate the avro schema using the above files
def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
            "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh:8081",
            "acks": "1"
    }

    producer = AvroProducer(producer_config,default_value_schema=value_schema)
    #File containing the data that we will process further.
    file = open(str(file_path)+'/'+'data/220129_Smart TMS_Cycles data_V4_2_processed.csv')
    csvreader = csv.reader(file)
    header = next(csvreader)
    i=0
    for row in csvreader:
        print(row)
        #key = {"BaseTime(s)": row[1]}
        value = {"Time": float(row[i+8]),
              "Latitude": float(row[i+1])
             ,"Longitude": float(row[i+3]), 
            "Distance": float(row[i+4]), 
            "Elevation": float(row[i+5])}
         
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
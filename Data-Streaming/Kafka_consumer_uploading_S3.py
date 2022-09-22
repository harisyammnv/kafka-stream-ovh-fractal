'''Summary: In this script we are consuming the kafka streaming using kafak consumer and 
   uploading it on the s3 objec storage of ovh cloud as in multiple parquet files.'''
from operator import index
from confluent_kafka.avro import AvroConsumer
import json
import toml
from pathlib import Path
from upload_service import DataIngestionService
'''This function is used to save the data on the ovh cloud, where the
   cfg:  having the credentials for s3 ovh object storage.
   data: list of json data coming from kafka topics 
   id:   Suffix for the parquet files which is saved in the ovh cloud '''
def save_data(cfg, data, id):
    data_uploader = DataIngestionService(cfg=cfg)
    encoded_data = json.dumps(data, indent=2).encode('utf-8')
    data_uploader.upload_binary(bucket_name="test-spark-container", 
                               filename=f'result-{id}.parquet', 
                               data=encoded_data)
#Defining the schema registry,bootstrap and topics of kafka.
def read_messages():
    consumer_config = {"bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh:8081",
             "group.id": "taxirides.avro.consumer.1",
             "auto.offset.reset": "earliest"}
    cfg = toml.load("/home/snehasuman/kafka-stream-ovh-fractal/Data-Processing/config.toml")
    
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["my-topic-test3"])
    data = []
    id = 0
    #consuming the kafka data and uploading it on the ovh cloud.
    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                data.append(message.value())
                if len(data) >= 10:
                   id+=1
                   save_data(cfg=cfg, data=data, id=id)
                   data.clear()
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()
    

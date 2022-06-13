from confluent_kafka.avro import AvroConsumer
import json
import toml
from pathlib import Path
from upload_service import DataIngestionService
from decouple import config

def save_data(data, id):
    with open(f"consumed_data/mydata-{id}.json", "w") as final:
        json.dump(data, final)

def read_messages():
    consumer_config = {"bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9092",
                       "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh",
                       "group.id": "taxirides.avro.consumer.1",
                       "auto.offset.reset": "earliest"}
    cfg = toml.load(Path.cwd().joinpath("config/config.toml"))
    data_uploader = DataIngestionService(cfg=cfg)
    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["dummy-taxi-rides"])
    data = []
    id = 0
    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                data.append(message.value())
                if len(data) > 100:
                    id+=1
                    save_data(data, id=id)
                    data.clear()
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    print(config('aws_access_key_id'))
    #read_messages()
    
from operator import index
from confluent_kafka.avro import AvroConsumer
import json
import toml
from pathlib import Path

# Defining the schema registry,bootstrep ad topics of kafka for schmema validation.
def read_messages():
    consumer_config = {
        "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
        "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh:8081",
        "group.id": "taxirides.avro.consumer.1",
        "auto.offset.reset": "earliest",
    }
    cfg = toml.load(Path.cwd().joinpath("config/config.toml"))

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["my-topic-test1"])
    data = []
    id = 0
    # consuming the kafka data and uploading it on the ovh cloud.
    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                data.append(message.value())

                print(
                    f"Successfully poll a record from "
                    f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                    f"message key: {message.key()} || message value: {message.value()}"
                )
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()

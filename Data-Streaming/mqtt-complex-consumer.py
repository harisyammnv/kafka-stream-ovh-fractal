from operator import index
from confluent_kafka.avro import AvroConsumer
from json import dumps
import toml
from pathlib import Path
from upload_service import DataIngestionService

KAFKA_HOST = "141.95.96.72:9094"
TOPIC = "my-topic-test1"
consumer = AvroConsumer(TOPIC,KAFKA_HOST)
  #consumer.subscribe(["my-topic-test1"])


#print("Message from topic: ", consumer.value())
while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                data.append(message.value())
                #if len(data) > 100:
                #    id+=1
                    #save_data(cfg=cfg, data=data, id=id)
                    #data.clear()
                print(message.value())
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")

        #     message = consumer.poll(5)
        # except Exception as e:
        #     print(f"Exception while trying to poll messages - {e}")
        # else:
        #     if message:
        #         data.append(message.value())
        #         print(f"Successfully poll a record from "
        #               f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
        #               f"message key: {message.key()} || message value: {message.value()}")
        #         consumer.commit()
        #     else:
        #         print("No new messages at this point. Try again later.")
if __name__ == "__main__":
    read_messages()
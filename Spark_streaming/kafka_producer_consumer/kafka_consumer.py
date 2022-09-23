from kafka import KafkaConsumer
from json import loads

# Defined the kafka topics and bootstrap server.
KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"
KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = "kafka-bs.fractal-kafka.ovh:9094"

if __name__ == "__main__":
    print("Kafka Consumer Application Started ... ")
    # Consumer Setting
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_NAME_CONS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
            value_deserializer=lambda x: loads(x.decode("utf-8")),
        )
        for message in consumer:
            # print(dir(message))
            # print(type(message))
            print("Key: ", message.key)
            message = message.value
            print("Message received: ", message)
    # If there is no data coming parallely from kafka producer then it will print below message.
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)

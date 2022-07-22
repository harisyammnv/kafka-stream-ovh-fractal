
from kafka import KafkaConsumer



# Address of the Kafka
HOST = "kafka-bs.fractal-kafka.ovh:9094"

consumer = KafkaConsumer("my-topic-test3",bootstrap_servers=HOST)


for i in consumer:
    print("Message from topic: ", i.value)
    
    

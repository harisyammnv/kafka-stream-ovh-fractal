
from confluent_kafka import Producer
from avro import schema
import avro.io
import io
import csv
import random
KAFKA_TOPIC_NAME_CONS = "my-topic-test1"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka-bs.fractal-kafka.ovh:9094'
from time import sleep
if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_config_obj = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS_CONS}
    kafka_producer_obj = Producer(**kafka_config_obj)

     # def load_avro_schema_from_file():
     #     key_schema = avro.load("schemas/taxi_ride_key.avsc")
     #     value_schema = avro.load("schemas/taxi_ride_value.avsc")

     #     return key_schema, value_schema


     # def send_record():
     #     key_schema, value_schema = load_avro_schema_from_file()

    # producer_config = {
    #         "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
    #         "schema.registry.url": "http://152.228.251.146:8081",
    #         "acks": "1"
    # }

    # producer = AvroProducer(producer_config)

    file = open('/home/snehasuman/kafka-stream-ovh-fractal/Data-Streaming/data/220129_Smart TMS_Cycles data_V4_2_processed.csv')
    csvreader = csv.reader(file)
    csvreader=list(csvreader)
    avro_schema_path = "/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/new.avsc" 
    avro_orders_schema = schema.parse(open(avro_schema_path).read())
    message_list = []
    message = None
    
    for i in range(1,500,1):
        message = {}        
        print("Preparing message: " + str(i))
        message["Time"] =str(csvreader[i+1][7])
        message["T_Amb"] = str(csvreader[i+1][0])
        message["Phi_Amb"] = str(csvreader[i+1][1])
        message["Road_Grad"] =str(csvreader[i+1][2])
        message["Veh_Spd"] = str(csvreader[i+1][3])
        message["Wind_Spd_Proj"] = str(csvreader[i+1][4])
        message["Solar_Rad"] =str(csvreader[i+1][5])
        message["DistanceChargeStation"] = str(csvreader[i+1][6])
        message["PowerConnector"] = str(csvreader[i+1][8])
        print("Message: ", message)
        message_writer = avro.io.DatumWriter(avro_orders_schema)
        message_bytes_writer = io.BytesIO()
        message_encoder = avro.io.BinaryEncoder(message_bytes_writer)
        message_writer.write(message,message_encoder)
        message_raw_bytes = message_bytes_writer.getvalue()
        
        kafka_producer_obj.produce(KAFKA_TOPIC_NAME_CONS, message_raw_bytes)
        sleep(1)  
       
    #kafka_producer_obj.flush()
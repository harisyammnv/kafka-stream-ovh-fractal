
from confluent_kafka import Producer
from avro import schema
import avro.io
import io
import csv
import random
#Defined the kafka topics and bootstrap server. 
KAFKA_TOPIC_NAME_CONS = "my-topic-test1"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kafka-bs.fractal-kafka.ovh:9094'
from time import sleep
if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    #producer Settings
    kafka_config_obj = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS_CONS}
    kafka_producer_obj = Producer(**kafka_config_obj)
    #File contaiing the data that we processed further.
    file = open('/home/snehasuman/kafka-stream-ovh-fractal/Data-Streaming/data/220129_Smart TMS_Cycles data_V4_2_processed.csv')
    csvreader = csv.reader(file)
    csvreader=list(csvreader)
    #loading the schema files for schema validation purpose.
    avro_schema_path = "/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/new.avsc" 
    avro_orders_schema = schema.parse(open(avro_schema_path).read())
    
    message_list = []
    message = None
    #This loop will produce data with avro.
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
       
    
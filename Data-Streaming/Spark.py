"""_summary_
This will stream messages and show on console. 
spark-submit streaming.py
"""
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.kafka:kafka-clients:2.8.1'
sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
#ssc = StreamingContext(sc,60)

kafkaStream = KafkaUtils.createStream(sc, 'kafka-bs.fractal-kafka.ovh:9094','spark-streaming' , {'my-topic-test1':1})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))

# import findspark
# if __name__ == "__main__":
#     spark = SparkSession \
#         .builder \
#         .appName("Multi Query Demo") \
#         .master("local[*]") \
#         .config("spark.streaming.stopGracefullyOnShutdown", "true") \
#         .getOrCreate()

#     kafka_source_df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "kafka-bs.fractal-kafka.ovh:9094") \
#         .option("subscribe", "my_topic_test3") \
#         .option("startingOffsets", "earliest") \
#         .load()

    
#     avroSchema = open('schemas/taxi_ride_value.avsc', mode='r').read()
   
    
    #value_df = kafka_source_df.select((from_avro(col("value"), avroSchema)).alias("value"))
    # print(avroSchema)
    
    # write_query =value_df.writeStream \
    #     .format("console") \
    #     .option("checkpointLocation", "../chk-point-dir") \
    #     .outputMode("append") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()
        

    # # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    # write_query.awaitTermination()
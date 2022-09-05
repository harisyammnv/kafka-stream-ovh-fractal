from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

import time
import os
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = 'kafka-bs.fractal-kafka.ovh:9094'
os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0 pyspark-shell"
if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") 
    spark.conf.set("parquet.enable.summary-metadata", "false")
    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()
    orders_df1 = orders_df.select("value", "timestamp")
    orders_schema_avro = open('/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/vehicle_ride_value.avsc', mode='r').read()

    #8,Wrist Band,MasterCard,137.13,2020-10-21 18:37:02,United Kingdom,London,www.datamaking.com
    orders_df2 = orders_df1\
        .select(from_avro(col("value"), orders_schema_avro)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = orders_df3 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .format("parquet")\
        .outputMode("append") \
        .option("checkpointLocation", "/home/snehasuman/kafka-stream-ovh-fractal/Data-Processing") \
        .option("path","/home/snehasuman/kafka-stream-ovh-fractal/Data-Processing/result2") \
        .option("truncate", "false")\
        .start()
    
    orders_agg_write_stream.awaitTermination()
    print("Stream Data Processing Application Completed.")


import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

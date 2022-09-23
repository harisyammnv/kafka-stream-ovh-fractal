from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import io
import toml
import time
import os

# Need to define the topics and bootstrep server to access the streaming data from kafka .
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = "kafka-bs.fractal-kafka.ovh:9094"
if __name__ == "__main__":
    config = toml.load("config.toml")
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    # Creating a spark session with S3 object of ovh cloud
    spark = (
        SparkSession.builder.appName("PySpark Structured Streaming")
        .config("spark.hadoop.fs.s3a.access.key", config["S3"].get("s3_access_key"))
        .config("spark.hadoop.fs.s3a.secret.key", config["S3"].get("s3_secret_key"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.gra.cloud.ovh.net")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    # Construct a streaming DataFrame that reads from test-topic
    orders_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic_name)
        .option("startingOffsets", "latest")
        .load()
    )

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()
    # Define a schema for the orders data
    orders_df1 = orders_df.select("value", "timestamp")
    orders_schema_avro = open("new.avsc", mode="r").read()
    orders_df2 = orders_df1.select(
        from_avro(col("value"), orders_schema_avro).alias("orders"), "timestamp"
    )

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = (
        orders_df3.writeStream.trigger(processingTime="5 seconds")
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", "s3a://test-timeseries-data/")
        .option("path", "s3a://test-timeseries-data/result")
        .start()
    )

    orders_agg_write_stream.awaitTermination()
    print("Stream Data Processing Application Completed.")
    spark.stop()

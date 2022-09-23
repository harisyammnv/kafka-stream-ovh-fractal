from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import time
import os

# Need to define the topics and bootstrap server to access the streaming data from kafka .
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = "kafka-bs.fractal-kafka.ovh:9094"
if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    # Creating a spark session
    spark = (
        SparkSession.builder.appName(
            "PySpark Structured Streaming with Kafka and Message Format as JSON"
        )
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("parquet.enable.summary-metadata", "false")
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
    orders_schema_avro = open(
        "/home/snehasuman/kafka-stream-ovh-fractal/mqtt/schemas/vehicle_ride_value.avsc",
        mode="r",
    ).read()
    orders_df2 = orders_df1.select(
        from_avro(col("value"), orders_schema_avro).alias("orders"), "timestamp"
    )

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # Write final result into console for debugging purpose and saving data on local.
    orders_agg_write_stream = (
        orders_df3.writeStream.trigger(processingTime="5 seconds")
        .format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/home/snehasuman/kafka-stream-ovh-fractal/Data-Processing",
        )
        .option(
            "path", "/home/snehasuman/kafka-stream-ovh-fractal/Data-Processing/result2"
        )
        .option("truncate", "false")
        .start()
    )

    orders_agg_write_stream.awaitTermination()
    print("Stream Data Processing Application Completed.")

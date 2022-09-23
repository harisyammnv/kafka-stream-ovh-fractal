from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import time
import os

# Need to define the topics and bootstrap server to access the streaming data from kafka .
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = "kafka-bs.fractal-kafka.ovh:9094"
# Specifying path
file_path = Path.home().joinpath("kafka-stream-ovh-fractal/mqtt")
if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    # Creating a spark session.
    spark = (
        SparkSession.builder.appName(
            "PySpark Structured Streaming with Kafka and Message Format as JSON"
        )
        .master("local[*]")
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

    orders_df1 = orders_df.select("value", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema_avro = open(
        str(file_path) + "/" + "schemas/vehicle_ride_value.avsc", mode="r"
    ).read()

    # 8,Wrist Band,MasterCard,137.13,2020-10-21 18:37:02,United Kingdom,London,www.datamaking.com
    orders_df2 = orders_df1.select(
        from_avro(col("value"), orders_schema_avro).alias("orders"), "timestamp"
    )

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # Simple aggregate - find total_order_amount by grouping country, city
    """orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
        .agg({'order_amount': 'sum'}) \
        .select("order_country_name", "order_city_name", col("sum(order_amount)") \
        .alias("total_order_amount"))

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()"""

    # Write final result into console for debugging purpose and saving the data in local folders.
    orders_agg_write_stream = (
        orders_df3.writeStream.trigger(processingTime="5 seconds")
        .format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/home/snehasuman/kafka-stream-ovh-fractal/Spark_streaming/pyspark_structured_streaming",
        )
        .option(
            "path",
            "/home/snehasuman/kafka-stream-ovh-fractal/Spark_streaming/pyspark_structured_streaming/part4.3/output",
        )
        .start()
    )

    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# Need to define the topics and bootstrap server to access the streaming data from kafka .
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = "kafka-bs.fractal-kafka.ovh:9094"

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

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema_string = (
        "order_id INT, order_product_name STRING, order_card_type STRING, order_amount DOUBLE, "
        + "order_datetime STRING, order_country_name STRING, order_city_name STRING, "
        + "order_ecommerce_website_name STRING"
    )

    # 8,Wrist Band,MasterCard,137.13,2020-10-21 18:37:02,United Kingdom,London,www.datamaking.com
    orders_df2 = orders_df1.select(
        from_csv(col("value"), orders_schema_string).alias("orders"), "timestamp"
    )

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    # Simple aggregate - find total_order_amount by grouping country, city
    orders_df4 = (
        orders_df3.groupBy("order_country_name", "order_city_name")
        .agg({"order_amount": "sum"})
        .select(
            "order_country_name",
            "order_city_name",
            col("sum(order_amount)").alias("total_order_amount"),
        )
    )

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = (
        orders_df4.writeStream.trigger(processingTime="5 seconds")
        .outputMode("update")
        .option("truncate", "false")
        .format("console")
        .start()
    )

    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")

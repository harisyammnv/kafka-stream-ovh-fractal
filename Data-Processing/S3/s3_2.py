from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import toml
import json
kafka_topic_name = "my-topic-test1"
kafka_bootstrap_servers = 'kafka-bs.fractal-kafka.ovh:9094'
if __name__ == "__main__":
    config = toml.load("config.toml")
    # create a SparkSession
    # We want to use the Swift S3 API. So we have to provide some attributes
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount") \
        .config("spark.hadoop.fs.s3a.access.key", "e6352ff9cf0e487cb49ee01400472811") \
        .config("spark.hadoop.fs.s3a.secret.key", "dd201d8844524421900fe72e44d9257a") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.endpoint", "s3.gra.cloud.ovh.net")\
        .getOrCreate()
    # lines = spark.read.parquet("s3a://testPythonScript/output.parquet").rdd.map(lambda r: r[0]) 
    # lines.saveAsparquetfile("s3a://testPythonScript/output.parquet")
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load("s3a://testPythonScript/wordcount_result.json")
         
    #orders_df.to_json("s3a://testPythonScript/wordcount_result.json", orient='records', compression='gzip') 
    orders_df.write.json("s3a://testPythonScript/wordcount_result.json")
    

    # s3_folder_path = "s3a://test-timeseries-data/*"
    # print(s3_folder_path)
    # df = spark.read.format('parquet').load(s3_folder_path)
    # print(df)
    # df.write.parquet("s3a://test-timeseries-data/output.parquet")
   
    
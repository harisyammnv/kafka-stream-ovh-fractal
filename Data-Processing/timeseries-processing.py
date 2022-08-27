from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import toml

if __name__ == "__main__":

    config = toml.load("config.toml")
    # create a SparkSession
    # We want to use the Swift S3 API. So we have to provide some attributes
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount") \
        .config("spark.hadoop.fs.s3a.access.key", config["S3"].get('s3_access_key')) \
        .config("spark.hadoop.fs.s3a.secret.key", config["S3"].get('s3_secret_key')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.endpoint", "s3.gra.cloud.ovh.net")\
        .getOrCreate()
        
    s3_folder_path = "s3a://test-timeseries-data/"
    print(s3_folder_path)
    df = spark.read.csv(s3_folder_path)
    df.show()
    

    # to calculate distance = speed [km/hr] * (time[s]/3600)
    #df.withColumn("distance_travelled", col("Speed") * (col("time")/3600))
    
    df.write.csv("s3a://test-timeseries-data/csv/final-drive-cycle1.csv")
    
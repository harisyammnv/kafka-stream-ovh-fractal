from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
import toml
if __name__ == "__main__":
    config = toml.load("config.toml")
    # create a SparkSession
    # We want to use the Swift S3 API. So we have to provide some attributes
    spark = SparkSession\
        .builder \
        .appName("PySpark Structured Streaming") \
        .config("spark.hadoop.fs.s3a.access.key", config["S3"].get('s3_access_key')) \
        .config("spark.hadoop.fs.s3a.secret.key", config["S3"].get('s3_secret_key')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.endpoint", "s3.gra.cloud.ovh.net")\
        .getOrCreate()


    # read the input file in Swift through the S3 API
    # create a rdd that contains lines of the input file
    s3_folder_path = "s3a://test-timeseries-data/"
    print(s3_folder_path)
    df = spark.read.csv(s3_folder_path)
    df.show()
    # lines = spark.read.csv("s3a://testPythonScript/")
    # lines.show() 
    # split lines, extract words and count the number of occurrences for each of them
    

    # store the result in the same (or another) bucket but in the same project
    # according to the attributes provided in SparkSession
    df.write.csv("s3a://test-timeseries-data/csv/final-drive-cycle2.csv")

    # print the result
    
    # very important: stop the current session

from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession
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

    # read the input file in Swift through the S3 API
    # create a rdd that contains lines of the input file
    lines = spark.read.text("s3a://test-data-wc/wordcount.txt").rdd.map(lambda r: r[0])

    # split lines, extract words and count the number of occurrences for each of them
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    # store the result in the same (or another) bucket but in the same project
    # according to the attributes provided in SparkSession
    counts.saveAsTextFile("s3a://test-data-wc/wordcount_result.txt")

    # print the result
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    # very important: stop the current session
    spark.stop()
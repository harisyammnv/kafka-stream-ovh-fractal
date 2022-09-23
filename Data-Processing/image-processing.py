import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.image import ImageSchema
from pyspark.ml.linalg import DenseVector, VectorUDT
import toml

# S3 credentials are stored in Config TOML
config = toml.load("config.toml")
# create a SparkSession
# We want to use the Swift S3 API. So we have to provide some attributes
spark = (
    SparkSession.builder.appName("PythonWordCount")
    .config("spark.hadoop.fs.s3a.access.key", config["S3"].get("s3_access_key"))
    .config("spark.hadoop.fs.s3a.secret.key", config["S3"].get("s3_secret_key"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.gra.cloud.ovh.net")
    .getOrCreate()
)

# Object Storage bucket where images are present
s3_url = "s3a://test-image-data-processing/*"
df = spark.read.format("image").load(s3_url)

img2vec = F.udf(lambda x: DenseVector(ImageSchema.toNDArray(x).flatten()), VectorUDT())

df = df.withColumn("vecs", img2vec("image"))
# object storage bucket where the output is saved
df.write.csv("s3a://test-data-wc/images_info.csv")

"""Summary: In this script we are uploading the data or the parquet file from s3 object of ovh cloud to lakefs cloud
   so after running the  kafka_consumer_uploading_ovh file u will need to run this file."""
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Need to define the S3 the Object storage of lakefs and ovh cloud
s3_lakefs = boto3.client(
    "s3",
    endpoint_url="http://fractal.ik-europe.eu/",
    aws_access_key_id="",
    aws_secret_access_key="",
)
s3_ovh = boto3.client(
    "s3",
    endpoint_url="https://s3.gra.cloud.ovh.net",
    aws_access_key_id="",
    aws_secret_access_key="",
)
s3_ovh_resource = boto3.resource(
    "s3",
    endpoint_url="https://s3.gra.cloud.ovh.net",
    aws_access_key_id="",
    aws_secret_access_key="",
)
bucket_name = "test-spark-container"
bucket = s3_ovh_resource.Bucket(bucket_name)
objects = s3_ovh.list_objects(Bucket=bucket_name)

# Accessing the bucket from S3 object storage of ovh cloud .
for obj in bucket.objects.all():
    filename = obj.key.rsplit("/")[-1]
    s3_ovh.download_file(bucket_name, obj.key, filename)
with open("file.parquet", "w") as f1:
    print(f1)
# Accessing the multiple parquet file and coverting it into one parquet file for uploading on the lakefs.
for object in objects["Contents"]:
    print(object["Key"])
    x = object["Key"]
    print(x)
    if x.endswith(".snappy.parquet"):
        with open("file.parquet", "a") as f1:
            with open(x, "rb") as f2:
                f1.write(str(f2.read()))
                f1.close()
"""opening the parquet file which is saved by the above code, 
   so opening the file and converting it into a valid parquet file for uploading on the lakefs and 
   i named it final.parquet"""
with open("file.parquet", "rb") as f1:
    df = pd.read_csv("file.parquet", sep=",", engine="python")
    table = pa.Table.from_pandas(df)
    print(table)
    pq.write_table(table, "final1.parquet")
    # Finally accessing the bucket and uploading the parquet file on the lakefs s3 object.
with open("final1.parquet", "rb") as f3:
    s3_lakefs.put_object(
        Body=f3, Bucket="fractal-uc2", Key="test123/result/final.parquet"
    )

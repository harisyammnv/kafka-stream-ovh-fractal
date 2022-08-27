import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

s3 = boto3.client('s3',
  endpoint_url='http://fractal.ik-europe.eu',
  aws_access_key_id='AKIAJ4MIFBBUBNPXCZCQ',
  aws_secret_access_key='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')

parquet_dir = Path("/home/snehasuman/kafka-stream-ovh-fractal/Spark_streaming/pyspark_structured_streaming/part4.3/result")
df=pq.ParquetDataset(parquet_dir)

print(df.read())
df = pq.read_table(source=parquet_dir).to_pandas()
print(type(df))
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_dir.parent.joinpath("final.parquet"))
with open('/home/snehasuman/kafka-stream-ovh-fractal/Spark_streaming/pyspark_structured_streaming/part4.3/final.parquet', 'rb') as f:
  s3.put_object(Body=f, Bucket='fractal-uc2', Key='test123/result/final.parquet')




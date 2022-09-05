import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

s3 = boto3.client('s3',
  endpoint_url='http://fractal.ik-europe.eu/',
  aws_access_key_id='',
  aws_secret_access_key='')
s3client1 = boto3.client('s3',
  endpoint_url='https://s3.gra.cloud.ovh.net',
  aws_access_key_id='',
  aws_secret_access_key='')
s3client2 = boto3.resource('s3',
  endpoint_url='https://s3.gra.cloud.ovh.net',
  aws_access_key_id='',
  aws_secret_access_key='')  
bucket_name="test-spark-container"
bucket = s3client2.Bucket(bucket_name)
objects=s3client1.list_objects(Bucket=bucket_name)
for obj in bucket.objects.all():
    filename = obj.key.rsplit('/')[-1]
    s3client1.download_file(bucket_name, obj.key, filename)
with open("file.parquet",'w') as f1:
  print(f1)
for object in objects['Contents']:
  print(object['Key'])
  x=object['Key']
  print(x)
  if(x.endswith('.snappy.parquet')):
    with open('file.parquet','a') as f1:
      with open(x,'rb') as f2:
        f1.write(str(f2.read()))
        f1.close()
with open('file.parquet','rb') as f1:
 df=pd.read_csv('file.parquet',sep=',',engine='python')
 table=pa.Table.from_pandas(df) 
 print(table)
 pq.write_table(table,'final1.parquet')
with open('final1.parquet','rb') as f3:  
 s3.put_object(Body=f3, Bucket='fractal-uc2', Key='test123/result/final.parquet') 
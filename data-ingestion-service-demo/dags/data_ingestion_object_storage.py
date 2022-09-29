import functools
import json
import logging
from datetime import datetime, timedelta
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
import os
from dotenv import load_dotenv
import string
import random
import pandas as pd
from airflow import DAG
import time
import boto3
from airflow.operators.python import PythonOperator


from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

default_args = {
    "owner": "fractal-uc2-avl",
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}
load_dotenv()

class UploadException(Exception):
    pass

consumer_logger = logging.getLogger("airflow")
def upload_binary(data) -> None:

    try:
        endpoint_url= "https://s3.gra.cloud.ovh.net"
        session = boto3.Session(aws_access_key_id=os.getenv('OVH_S3_ACCESS_KEY'), 
                                aws_secret_access_key=os.getenv('OVH_S3_SECRET_KEY'))
        s3_client = session.client("s3", 
                                   endpoint_url=endpoint_url, 
                                   region_name="gra")
        consumer_logger.info("Batch-upload started !!!")
        encoded_data = json.dumps(data, indent=2).encode("utf-8")
        data = json.loads(encoded_data)
        data_df = pd.DataFrame.from_records(data)
        table = pa.Table.from_pandas(data_df)
        writer = pa.BufferOutputStream()
        pq.write_table(table, writer)
        body = bytes(writer.getvalue())
        rand_val = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
        s3_client.put_object(Body=body,
                             Bucket='test-spark-container', 
                             Key=f'cache/dc-{rand_val}.parquet')
        consumer_logger.info(f"Batch-upload finished at- s3://test-spark-container/cache/dc-{rand_val}.parquet !!!")
    except Exception as ex: 
        raise UploadException(
            f"Could not put object in the bucket: test-spark-container because of {ex}"
        )

def upload_dataset():
    try:
        endpoint_url= "http://fractal.ik-europe.eu/"
        lakefs_client = boto3.client("s3",
                                    endpoint_url=endpoint_url,
                                    aws_access_key_id=os.getenv('LAKEFS_ACCESS_KEY'),
                                    aws_secret_access_key=os.getenv('LAKEFS_SECRET_KEY'))
        fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': "https://s3.gra.cloud.ovh.net"})
        dataset=pq.ParquetDataset(f's3://test-spark-container/cache', 
                                    filesystem=fs)
        df = dataset.read().to_pandas()

        table = pa.Table.from_pandas(df)
        writer = pa.BufferOutputStream()
        pq.write_table(table, writer)
        body = bytes(writer.getvalue())
        
        lakefs_client.put_object(Body=body, 
                                 Bucket="fractal-uc2", 
                                 Key=f"main/data_ingestion/drive-cycle-{int(time.time())}.parquet"
        )
    except Exception as ex:
        raise UploadException(
        f"Could not put object in the bucket: fractal-uc2 because of {ex}"
    )


def await_function(message):
    if json.loads(message.value()) == "Trigger DataSet Transformation":
        return f" Got the following message: {json.loads(message.value())}"

def producer_function():
    for i in range(1):
        yield (json.dumps(i), json.dumps("Trigger DataSet Transformation"))

consumer_logger = logging.getLogger("airflow")
def consumer_function(data):
    consumer_logger.info(f"Records: {data}")
    upload_binary(data=data)
    return

with DAG(
    "streaming-data-ingestion",
    default_args=default_args,
    description="Demo of Streaming Data Ingestion Job in FRACTAL Cloud",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["demo-streaming-data-ingestion"],
) as dag:
    
    t1 = ConsumeFromTopicOperator(
        task_id="consume-raw-data-ovh-upload",
        topics=["dummy-topic-test1"],
        apply_function="data_ingestion_object_storage.consumer_function",
        max_messages = 1000,
        max_batch_size=10,
        consumer_config={
            "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "group.id": "taxirides.avro.consumer.1",
            "schema.registry.url": "http://schemaregistry.fractal-kafka.ovh:8081",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        },
        commit_cadence="end_of_batch",
    )
    
    t1.doc_md = 'Consume from topic operator consumes the data from a given topic and stores batches of them in parquet files on OVH Object Storage'

    t2 = ProduceToTopicOperator(
        task_id="produce-information-raw-drive-cycle",
        topic="dummy-topic-test2",
        producer_function=producer_function,
        kafka_config={"bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094"},
    )

    t2.doc_md = 'This Task produces a message which will trigger the Data Transformation task which will read the raw data from OVH object storage'
    
    t3 = PythonOperator(
        task_id='transform-dataSet-upload',
        python_callable=upload_dataset
    )

    t3.doc_md = 'The task that is executed after the deferrable task returns for execution after the completion of a drive cycle'
       

    t1 >> t2 >> t3
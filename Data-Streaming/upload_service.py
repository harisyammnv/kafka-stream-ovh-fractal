import boto3
import botocore.config as boto_config
import time
import io
from pathlib import Path
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from decouple import config

class DataIngestionServiceException(Exception):
    pass

class DataIngestionService:
    def __init__(self, cfg) -> None:
        print("Initializing data ingestion service")
        session = boto3.Session(
            aws_access_key_id=config('aws_access_key_id,'),
            aws_secret_access_key=config('aws_secret_access_key')
        )
        self.s3_client = session.client(
            's3',
            endpoint_url=f"https://s3.{cfg['S3'].s3_region}.cloud.ovh.net/",
            region_name=cfg.s3_region
        )
        self.cfg = cfg

    def _check_bucket(self, bucket_name: str, create_on_check: bool = False) -> bool:
        available_bucket_names = (bucket['Name'] for bucket in self.s3_client.list_buckets()['Buckets'])
        if bucket_name in available_bucket_names:
            return True
        if create_on_check:
            location = {'LocationConstraint': self.cfg.s3_region}
            self.s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
            return True
        return False

    def upload_binary(self, bucket_name: str, filename: str, data: bytearray, retries: int = 1, rest: int = 5) -> None:
        self._check_bucket(bucket_name, create_on_check=True)
        print(f"Saving {filename} to bucket {bucket_name}.")
        for _ in range(retries):
            try:
                file_bytestream = io.BytesIO(data)
                self.s3_client.upload_fileobj(
                    Fileobj=file_bytestream,
                    Bucket=bucket_name,
                    Key =filename
                )
            except Exception as ex:
                exception = ex
                time.sleep(rest)
            else:
                print(f"Saved {filename}({len(data)}) to {bucket_name}.")
                return
        raise DataIngestionServiceException(f'Could not put json object in the bucket: {bucket_name} because of {exception}')

    def _get_filename(self, message: ConsumerRecord) -> str:
        name_extension = message.key.decode('UTF-8').rsplit('.', 1)
        if len(name_extension) == 2:
            name, extension = name_extension
            extension = f".{extension}"
        else:
            name, extension = name_extension[0], ''
        filename = f"{name}-{message.timestamp}{extension}"
        return filename
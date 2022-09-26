"""Summary: this script is used to upload the kafka data on the ovh cloud"""

import time
import io
from pathlib import Path
import os
from dotenv import load_dotenv
import boto3
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class UploadServiceException(Exception):
    pass


"""Class for defining the s3 object storage of ovh cloud ,u need specify the credentials for ovh cloud's bucket with region
   and this function will be called by kafka_consumer_uploading_ovh.py file"""


class UploadService:
    def __init__(self, cfg: dict) -> None:
        load_dotenv()
        self.cfg = cfg
        session = boto3.Session(aws_access_key_id=os.getenv('OVH_S3_ACCESS_KEY'), 
                                aws_secret_access_key=os.getenv('OVH_S3_SECRET_KEY'))
        self.s3_client = session.client("s3", 
                                        endpoint_url=self.cfg["S3"]["endpoint_url"], 
                                        region_name=self.cfg["S3"]["s3_region"])
        self.lakefs_client = boto3.client("s3",
                                          endpoint_url=self.cfg["LAKE-FS"]["endpoint_url"],
                                          aws_access_key_id="",
                                          aws_secret_access_key="")

    def _check_bucket(self, bucket_name: str, create_on_check: bool = False) -> bool:
        available_bucket_names = (
            bucket["Name"] for bucket in self.s3_client.list_buckets()["Buckets"]
        )
        if bucket_name in available_bucket_names:
            return True
        if create_on_check:
            location = self.cfg["S3"]["s3_region"]
            self.s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location
            )
            return True
        return False

    def upload_binary(
        self,
        bucket_name: str,
        filename: str,
        data: bytearray,
        retries: int = 1,
        rest: int = 5,
    ) -> None:
        self._check_bucket(bucket_name, create_on_check=True)
        print(f"Saving {filename} to bucket {bucket_name}.")
        for _ in range(retries):
            try:
                data = json.loads(data)
                data_df = pd.DataFrame.from_records(data)
                table = pa.Table.from_pandas(data_df)
                writer = pa.BufferOutputStream()
                pq.write_table(table, writer)
                body = bytes(writer.getvalue())
                # file_bytestream = io.BytesIO(data)
                # self.s3_client.upload_fileobj(
                #     Fileobj=file_bytestream, Bucket=bucket_name, Key=filename
                # )
                self.s3_client.put_object(Body=body, 
                                          Bucket=bucket_name, 
                                          Key=filename)
                
            except Exception as ex:
                exception = ex
                time.sleep(rest)
            else:
                print(f"Saved {filename}({len(data)}) to {bucket_name}.")
                return
        raise UploadServiceException(
            f"Could not put object in the bucket: {bucket_name} because of {exception}"
        )
    
    def upload_dataset(self, 
                       lake_fs_bucket: str,
                       local_file_path: Path,
                       filename: str):
        try:
            with open(local_file_path, "rb") as f:
                self.lakefs_client.put_object(Body=f, 
                                    Bucket=lake_fs_bucket, 
                                    Key=f"main/data_ingestion/{filename}"
                )
        except Exception as ex:
            raise UploadServiceException(
            f"Could not put object in the bucket: {lake_fs_bucket} because of {ex}"
        )
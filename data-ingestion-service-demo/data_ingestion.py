from confluent_kafka.avro import AvroConsumer
import structlog
import json
import toml
from pathlib import Path
from upload_service import UploadService


class DataIngestionServiceException(Exception):
    pass


class DataIngestionService:
    
    def __init__(self, 
                 config_path: Path) -> None:
        self.logger = structlog.get_logger()
        self.config_path = config_path
        self.cfg = self._load_config()
        
    def _load_config(self):
        try:
            self.logger.msg("Reading User defined config for MQTT Ingestion !!!")
            return toml.load(self.config_path)
        except Exception:
            raise DataIngestionServiceException("Config cannot be loaded. \
                Make sure the path is correct")
        
    
    def _setup_consumer(self):
        try:
            consumer_config = {
                "bootstrap.servers": self.cfg["OVH-KAFKA"]["kafka_bootstrap_servers"],
                "schema.registry.url": self.cfg["OVH-KAFKA"]["kafka_schema_registry_url"],
                "group.id": self.cfg["OVH-KAFKA"]["consumer_group"],
                "auto.offset.reset": self.cfg["OVH-KAFKA"]["offset_reset"],
            }
            return AvroConsumer(consumer_config)
        except Exception as ex:
            raise DataIngestionServiceException(f"Consumer cannot be configured because: {ex}")

    def _save_data_to_ovh(self, data, id, cache_num):
        try:
            data_uploader = UploadService(cfg=self.cfg)
            
            encoded_data = json.dumps(data, indent=2).encode("utf-8")
            self.logger.msg("Uploading Data to S3")
            data_uploader.upload_binary(
                bucket_name=self.cfg["S3"]["drive_cycle_bucket"],
                filename=f"cache-{cache_num}/result-{id}.parquet",
                data=encoded_data,
            )
        except Exception as ex:
            raise DataIngestionServiceException(f"Cannot Save data to Object storage because: {ex}")

    def consume_messages(self):
        try:
            self.consumer = self._setup_consumer()
            self.consumer.subscribe([self.cfg["OVH-KAFKA"]["kafka_topic"]])
            data = []
            id = 1
            cache_num = 1
            # consuming the kafka data and uploading it on the ovh cloud.
            while True:
                try:
                    message = self.consumer.poll(5)
                except Exception as e:
                    self.consumer.close()
                    raise DataIngestionServiceException(f"Exception while trying to poll messages - {e}")
                else:
                    if message:
                        data.append(message.value())
                        if len(data) >= self.cfg["OVH-KAFKA"]["data_cache_len"]:
                            self._save_data_to_ovh(data=data, id=id, cache_num= cache_num)
                            id += 1
                            data.clear()
                        self.logger.msg(
                            f"Successfully polled a record from "
                            f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                            f"message key: {message.key()} || message value: {message.value()}"
                        )
                        if id % self.cfg["OVH-KAFKA"]["cache_len"] == 0:
                            self.logger.msg("Trigger Data Transformation")
                            lakefs_uploader = UploadService(cfg=self.cfg)
                            lakefs_uploader.upload_dataset(lake_fs_bucket=self.cfg["LAKE-FS"]["dataset_bucket"],
                                                           key_path=f'cache-{cache_num}',
                                                           filename=f'cache-{cache_num}.parquet')
                            cache_num += 1
                            
                        self.consumer.commit()
                    else:
                        self.logger.msg("No new messages at this point. Try again later.")
        except Exception as ex:
            raise DataIngestionServiceException(f"Cannot consume messages because: {ex}")

if __name__ == "__main__":
    data_ingestion = DataIngestionService(config_path=Path.cwd().joinpath('config/config.toml'))
    data_ingestion.consume_messages()
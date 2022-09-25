"""Summary:  In this file we are getting the data from mqtt client then 
   we will send this data on the kafka topic by the kafka producer"""

import paho.mqtt.client as mqtt
from confluent_kafka import avro
from json import loads
from confluent_kafka.avro import AvroProducer
from time import sleep
from pathlib import Path
import structlog
import toml

class MQTTKafkaBridgeException(Exception):
    pass

class MQTTKafkaBridge:
    
    def __init__(self, avro_key_schema_path: Path,
                 avro_value_schema_path: Path,
                 config_path: Path,
                 client_name: str) -> None:
        self.logger = structlog.get_logger()
        self.avro_key_schema_path = avro_key_schema_path
        self.avro_value_schema_path = avro_value_schema_path
        self.config_path = config_path
        self.client_name = client_name
        self.cfg = self._load_config()
        self.key_counter = 0
    def _load_config(self):
        try:
            self.logger.msg("Reading User defined config for MQTT Ingestion !!!")
            return toml.load(self.config_path)
        except Exception:
            raise MQTTKafkaBridgeException("Config cannot be loaded. \
                             Make sure the path is correct")
    
    def _load_schema(self):
        # loading the schema files for schema validation purpose.
        self.key_schema = avro.load(str(self.avro_key_schema_path))
        self.value_schema = avro.load(str(self.avro_value_schema_path))
    
    def _setup_producer(self):
        # producer Settings
        self.producer_config = {
            "bootstrap.servers": self.cfg["OVH-KAFKA"]["kafka_bootstrap_servers"],
            "schema.registry.url": self.cfg["OVH-KAFKA"]["kafka_schema_registry"],
            "acks": "1",
        }
        return AvroProducer(self.producer_config, 
                                default_key_schema=self.key_schema,
                                default_value_schema=self.value_schema)
    def _configure_mqtt(self):
        try:
            self.logger.msg("Connecting to MQTT Client !!!")
            self.mqtt_client = mqtt.Client(self.client_name)
            self.mqtt_client.connect(self.cfg['MQTT']['mqtt_host'])
            self.logger.msg("MQTT Client connection established !!!")
        except Exception:
            raise MQTTKafkaBridgeException("MQTT Config cannot be loaded. \
                             Make sure the path is correct")

    def on_message(self, client, userdata, message):
        key = {"Time": float(self.key_counter)}
        self.key_counter = self.key_counter + 1
        msg_payload = message.payload
        msg_payload = msg_payload.decode()
        self.logger.msg(f"Received MQTT message at {key}")
        self.producer.produce(topic=self.cfg["OVH-KAFKA"]["kafka_topic"],
                              key=key, 
                              value=loads(msg_payload))
        self.logger.msg(f"Send the message: {loads(msg_payload)} to Kafka with topic {self.cfg['OVH-KAFKA']['kafka_topic']}!")
        self.producer.flush()

    def start_bridge(self):
        self._configure_mqtt()
        self._load_schema()
        self.producer = self._setup_producer()
        self.mqtt_client.loop_start()
        self.mqtt_client.subscribe(self.cfg["MQTT"]["mqtt_topic"])
        self.mqtt_client.on_message = self.on_message
        #sleep(100)
        #self.mqtt_client.loop_stop()
        
if __name__ == "__main__":
    mqtt_kafka = MQTTKafkaBridge(avro_key_schema_path=Path.cwd().joinpath('schemas/drive_cycle_key.avsc'),
                                 avro_value_schema_path=Path.cwd().joinpath('schemas/drive_cycle_value.avsc'), 
                                 config_path=Path.cwd().joinpath('config/config.toml'),
                                 client_name="BridgeMQTT2Kafka")
    mqtt_kafka.start_bridge()

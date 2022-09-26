"""Summary: In this file the data produced from a vehicle or sensor is sent to 
   the MQTT topic then data can be transferred to kafka topic data with 
   mqtt-kafka bridge by using the mqtt protocol."""

import paho.mqtt.client as mqtt
import time
import toml
import pandas as pd
from typing import Optional
from pathlib import Path
import structlog
import json


class EdgeToMQTTException(Exception):
    pass


class EdgeToMQTT:

    def __init__(self, config_path: Path,
                 client_name: Optional[str] = 'DriveCycle_Generator') -> None:
        self.config_path = config_path
        self.client_name = client_name
        self.logger = structlog.get_logger()
        self.cfg = self._load_config()

    def _load_config(self):
        try:
            self.logger.msg("Reading User defined config for MQTT Ingestion !!!")
            return toml.load(self.config_path)
        except Exception:
            raise EdgeToMQTTException("MQTT Config cannot be loaded. \
                             Make sure the path is correct")
    
    def _configure_mqtt(self):
        try:
            self.logger.msg("Connecting to MQTT Client !!!")
            self.mqtt_client = mqtt.Client(self.client_name)
            self.mqtt_client.connect(self.cfg['MQTT']['mqtt_host'])
            self.logger.msg("MQTT Client connection established !!!")
        except Exception:
            raise EdgeToMQTTException("MQTT Config cannot be loaded. \
                             Make sure the path is correct")
    
    def mqtt_publish(self):
        if Path.cwd().joinpath(self.cfg['MQTT']['data_file']).exists():
            data = pd.read_csv(Path.cwd().joinpath(self.cfg['MQTT']['data_file']))
            data = data.to_dict(orient='records')
            self._configure_mqtt()
            for entry in data:
                self.logger.msg(f"Sending to MQTT Topic - {self.cfg['MQTT']['mqtt_topic']}: {entry}")
                self.mqtt_client.publish(self.cfg['MQTT']['mqtt_topic'], json.dumps(entry))
                time.sleep(0.5)
                
        else:
            raise EdgeToMQTTException('Data File has to be existing')
        return


if __name__ == "__main__":
    edge_to_mqtt = EdgeToMQTT(config_path=Path.cwd().joinpath('config/config.toml'))
    edge_to_mqtt.mqtt_publish()

import functools
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2021, 7, 20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def producer_function():
    for i in range(2000):
        yield (json.dumps(i), json.dumps(i + 1))


consumer_logger = logging.getLogger("airflow")
def consumer_function(message, prefix=None):
    #key = json.loads(message.key())
    #value = json.loads(message.value())
    #consumer_logger.info(f"{prefix} {message.topic()} @ {message.offset()}; {key} : {value}")
    consumer_logger.info(message.decode('utf-8', errors='replace'))
    return

with DAG(
    "kafka-example-2",
    default_args=default_args,
    description="Examples of Kafka Operators",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # t1 = ProduceToTopicOperator(
    #     task_id="produce_to_topic",
    #     topic="dummy-topic-test1",
    #     producer_function="hello_kafka.producer_function",
    #     kafka_config={"bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094"},
    # )

    # t1.doc_md = 'Takes a series of messages from a generator function and publishes them to the `test_1` topic of our kafka cluster.'

    t2 = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["dummy-topic-test1"],
        apply_function="hello_kafka.consumer_function",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "kafka-bs.fractal-kafka.ovh:9094",
            "group.id": "taxirides.avro.consumer.1",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        },
        commit_cadence="end_of_batch",
    )
    t2
    # t1 >> t2
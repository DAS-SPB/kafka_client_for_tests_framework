import pytest

from kafka_client.config import KAFKA_CONFIG
from kafka_client.producer import KafkaProducer
from kafka_client.client import KafkaClient


@pytest.fixture(scope="session")
def kafka_client():
    producer = KafkaProducer(config=KAFKA_CONFIG)
    client = KafkaClient(producer=producer)

    yield client

    producer.close()

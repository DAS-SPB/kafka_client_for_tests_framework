import os
import pytest

from kafka_client.config import KafkaConfig
from kafka_client.producer import KafkaProducer
from kafka_client.client import KafkaClient


@pytest.fixture(scope="session")
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9092",
        ),
        security_protocol=os.getenv(
            "KAFKA_SECURITY_PROTOCOL",
            "PLAINTEXT",
        ),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
        sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
        sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
    )


@pytest.fixture(scope="session")
def kafka_client(kafka_config):
    producer = KafkaProducer(config=kafka_config)
    client = KafkaClient(producer=producer)

    yield client

    client.close()

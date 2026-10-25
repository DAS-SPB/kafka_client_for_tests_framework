import time

from dataclasses import dataclass
from threading import Event, Lock

from confluent_kafka import (
    KafkaError,
    KafkaException,
    Message,
    Producer,
)

from kafka_client.config import KafkaConfig


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    topic: str
    partition: int
    offset: int


class KafkaProducerError(RuntimeError):
    pass


class KafkaDeliveryError(KafkaProducerError):
    pass


class KafkaDeliveryTimeoutError(KafkaProducerError):
    pass


class KafkaProducerClosedError(KafkaProducerError):
    pass


class KafkaProducer:
    def __init__(self, config: KafkaConfig):
        self._producer = Producer(config.to_producer_config())

        self._lock = Lock()
        self._close_lock = Lock()

        self._closed = False
        self._drained = False

    def send(
            self,
            topic: str,
            value: bytes,
            *,
            key: bytes | None = None,
            headers: dict[str, bytes] | None = None,
            timeout: float = 10.0,
    ) -> DeliveryResult:
        deadline = time.monotonic() + timeout
        completed = Event()

        result: DeliveryResult | None = None
        delivery_error: KafkaError | None = None

        def on_delivery(
                error: KafkaError | None,
                message: Message,
        ) -> None:
            nonlocal result, delivery_error

            try:
                if error is not None:
                    delivery_error = error
                else:
                    result = DeliveryResult(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
            finally:
                completed.set()

        with self._lock:
            if self._closed:
                raise KafkaProducerClosedError(
                    "Kafka producer is closed"
                )

            try:
                self._producer.produce(
                    topic=topic,
                    value=value,
                    key=key,
                    headers=headers,
                    on_delivery=on_delivery,
                )
            except (KafkaException, BufferError) as exc:
                raise KafkaProducerError(
                    f"Failed to enqueue message to {topic}"
                ) from exc

        while not completed.is_set():
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                if completed.is_set():
                    break

                raise KafkaDeliveryTimeoutError(
                    f"Delivery confirmation timed out "
                    f"for topic {topic}"
                )

            try:
                self._producer.poll(min(remaining, 0.1))
            except KafkaException as exc:
                raise KafkaProducerError(
                    f"Failed while polling Kafka for {topic}"
                ) from exc

        if delivery_error is not None:
            raise KafkaDeliveryError(
                f"Delivery failed for topic {topic}: "
                f"{delivery_error}"
            )

        if result is None:
            raise KafkaProducerError(
                "Delivery callback returned no result"
            )

        return result

    def close(self, timeout: float = 10.0) -> None:
        with self._close_lock:
            with self._lock:
                self._closed = True

            if self._drained:
                return

            try:
                remaining = self._producer.flush(timeout)
            except KafkaException as exc:
                raise KafkaProducerError(
                    "Failed to flush Kafka producer"
                ) from exc

            if remaining:
                raise KafkaProducerError(
                    f"Producer has {remaining} pending messages"
                )

            self._drained = True

import json

from kafka_client.producer import KafkaProducer, DeliveryResult


class KafkaClient:
    def __init__(self, producer: KafkaProducer):
        self._producer = producer

    def write_message(
            self,
            topic: str,
            message: dict | str | bytes,
            *,
            key: str | bytes | None = None,
            headers: dict[str, str | bytes] | None = None,
            timeout: float = 10.0,
    ) -> DeliveryResult:
        payload = self._serialize_message(message)

        encoded_key = (
            key.encode("utf-8")
            if isinstance(key, str)
            else key
        )

        encoded_headers = (
            {
                (name, value.encode("utf-8") if isinstance(value, str) else value)
                for name, value in headers.items()
            }
            if headers is not None else None
        )

        return self._producer.send(
            topic=topic,
            value=payload,
            key=encoded_key,
            headers=encoded_headers,
            timeout=timeout,
        )

    @staticmethod
    def _serialize_message(
            message: dict | str | bytes,
    ) -> bytes:
        if isinstance(message, bytes):
            return message

        if isinstance(message, str):
            return message.encode("utf-8")

        if isinstance(message, dict):
            try:
                return json.dumps(
                    message,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "Failed to serialize message to JSON"
                ) from exc

        raise ValueError(
            f"Unsupported message type: {type(message)!r}"
        )

    def close(self, timeout: float = 10.0):
        self._producer.close(timeout=timeout)

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str = "pytest-kafka"

    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = field(
        default=None,
        repr=False,
    )

    message_timeout_ms: int = 30_000

    def to_producer_config(self) -> dict:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": self.message_timeout_ms,
        }

        if self.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            config.update(
                {
                    "sasl.mechanism": self.sasl_mechanism,
                    "sasl.username": self.sasl_username,
                    "sasl.password": self.sasl_password,
                }
            )

        return config

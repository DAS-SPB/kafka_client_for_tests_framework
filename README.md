# Kafka-клиент для pytest

Клиент для отправки сообщений в Kafka из pytest.
Поддерживает сообщения `dict`, `str`, `bytes`, ключи и headers.
После подтверждения доставки возвращает `topic`, `partition` и `offset`.

## Запуск

Требуются Python 3.12+, uv и Docker Compose.

```bash
uv sync
docker compose up -d kafka kafka-init
```

Добавьте тест в `tests/test_messages.py`, используя существующую фикстуру `kafka_client` из `tests/conftest.py`:

```python
def test_send_message(kafka_client):
    result = kafka_client.write_message(
        topic="tasks.created",
        message={"task_id": 1234, "type": "generate_report"},
        key="1234",
    )

    assert result.offset >= 0
```

Запустите тесты: `uv run pytest`.

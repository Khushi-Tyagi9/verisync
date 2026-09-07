"""Publishing normalized envelopes to the unified lifecycle topic.

The webhook path publishes synchronously (produce + flush) so it only returns
202 once Redpanda has acknowledged the write. At webhook-ingestion volume this
is fine and it keeps the "accepted means durably enqueued" contract simple.
"""

from __future__ import annotations

import json
from typing import Protocol, runtime_checkable


class PublishError(RuntimeError):
    """Raised when an envelope could not be durably enqueued."""


@runtime_checkable
class LifecyclePublisher(Protocol):
    def publish(self, *, order_id: str, envelope: dict) -> None: ...

    def close(self) -> None: ...


class KafkaLifecyclePublisher:
    def __init__(self, producer, topic: str, *, flush_timeout: float = 5.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout

    @classmethod
    def from_settings(cls, settings) -> "KafkaLifecyclePublisher":
        from confluent_kafka import Producer

        producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "verisync-ingestion",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
            }
        )
        return cls(producer, settings.lifecycle_topic)

    def publish(self, *, order_id: str, envelope: dict) -> None:
        payload = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
        errors: list = []

        def _on_delivery(err, _msg) -> None:
            if err is not None:
                errors.append(err)

        try:
            self._producer.produce(
                self._topic,
                key=order_id.encode("utf-8"),
                value=payload,
                headers=[("event_id", envelope["event_id"].encode("utf-8"))],
                on_delivery=_on_delivery,
            )
        except BufferError as exc:
            raise PublishError(f"producer queue full: {exc}") from exc
        except Exception as exc:  # confluent_kafka.KafkaException and friends
            raise PublishError(str(exc)) from exc

        remaining = self._producer.flush(self._flush_timeout)
        if remaining > 0:
            raise PublishError(
                f"{remaining} message(s) not acknowledged within {self._flush_timeout}s"
            )
        if errors:
            raise PublishError(f"delivery failed: {errors[0]}")

    def close(self) -> None:
        try:
            self._producer.flush(self._flush_timeout)
        except Exception:
            pass

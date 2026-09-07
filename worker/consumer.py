"""Kafka consumer for the reconciliation worker.

Connection shell + poll loop. Offsets are committed manually and only after a
successful Postgres write (``enable.auto.commit=False``); until the decision
write path lands in the next step this loop deliberately does not commit, so a
worker started now re-reads from its last committed offset on every restart
rather than silently advancing past unprocessed events.

Run it with:  python -m worker.consumer
"""

from __future__ import annotations

import json
import logging
import signal
from typing import Any, Optional

from .config import WorkerSettings, load_settings

log = logging.getLogger("verisync.worker")

# confluent_kafka error code for "reached end of partition"; informational, not
# a failure, and only delivered when explicitly enabled.
_PARTITION_EOF = -191


def build_consumer(settings: WorkerSettings):
    """A confluent_kafka Consumer wired for manual, post-write offset commits."""
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # Per-partition order already holds across rebalances; we never seek.
            "enable.partition.eof": False,
            "client.id": "verisync-worker",
        }
    )


def build_pool(settings: WorkerSettings):
    from psycopg_pool import ConnectionPool

    return ConnectionPool(
        settings.postgres_dsn, min_size=1, max_size=4, open=True, timeout=10
    )


def build_redis(settings: WorkerSettings):
    import redis

    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


class ReconciliationWorker:
    def __init__(
        self,
        *,
        consumer,
        pool,
        redis_client,
        settings: WorkerSettings,
    ) -> None:
        self._consumer = consumer
        self._pool = pool
        self._redis = redis_client
        self._settings = settings
        self._stopping = False

    @classmethod
    def from_settings(cls, settings: Optional[WorkerSettings] = None) -> "ReconciliationWorker":
        settings = settings or load_settings()
        return cls(
            consumer=build_consumer(settings),
            pool=build_pool(settings),
            redis_client=build_redis(settings),
            settings=settings,
        )

    def request_stop(self, *_signal_args: Any) -> None:
        """Idempotent; safe to call from a signal handler."""
        self._stopping = True

    def run(self) -> None:
        topic = self._settings.lifecycle_topic
        self._consumer.subscribe([topic])
        log.info(
            "worker up: group=%s topic=%s brokers=%s",
            self._settings.consumer_group,
            topic,
            self._settings.kafka_bootstrap_servers,
        )
        try:
            while not self._stopping:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() == _PARTITION_EOF:
                        continue
                    log.error("consume error: %s", err)
                    continue
                self._handle_message(msg)
        finally:
            self.close()

    def _handle_message(self, msg) -> bool:
        """Process one message. Returns True when the offset is safe to commit.

        The decision write path is added in the next step; for now this only
        parses the envelope and logs it, and never reports the offset as
        committable.
        """
        try:
            envelope = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError) as exc:
            # A poison record: nothing downstream can use it. Log loudly and
            # skip; the real dead-letter routing arrives with the worker's DB
            # path. Returning False leaves the offset uncommitted for now.
            log.error("undeserializable message at %s[%s]: %s", msg.topic(), msg.partition(), exc)
            return False

        log.info(
            "event %s order=%s type=%s source=%s",
            _get(envelope, "event_id"),
            _get(envelope, "order_id"),
            _get(envelope, "event_type"),
            _get(envelope, "source"),
        )
        return False

    def close(self) -> None:
        for name, obj, closer in (
            ("consumer", self._consumer, lambda o: o.close()),
            ("pool", self._pool, lambda o: o.close()),
            ("redis", self._redis, lambda o: o.close()),
        ):
            try:
                closer(obj)
            except Exception as exc:  # best-effort teardown
                log.warning("error closing %s: %s", name, exc)


def _get(envelope: Any, key: str) -> Any:
    return envelope.get(key) if isinstance(envelope, dict) else None


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    worker = ReconciliationWorker.from_settings()
    signal.signal(signal.SIGINT, worker.request_stop)
    signal.signal(signal.SIGTERM, worker.request_stop)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

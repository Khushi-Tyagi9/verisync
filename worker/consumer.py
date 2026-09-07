"""Kafka consumer for the reconciliation worker.

For each lifecycle event, in order per partition:

1. Deserialize the envelope and normalize it to a ``decision.Event``.
2. Fast pre-filter: skip if the Redis idempotency key already exists.
3. In one Postgres transaction: read current state (``orders_state`` +
   ``order_views``), run the pure decision logic, insert the ``audit_log`` row
   (``ON CONFLICT (event_id) DO NOTHING`` -- the authoritative idempotency
   layer), persist the projected view, and apply the pointer effect.
4. Only after that transaction commits: set the Redis pre-filter key, then
   manually commit the Kafka offset.

Offsets are never committed before the write (``enable.auto.commit=False``), so
a crash produces at-least-once redelivery that the idempotency layers absorb.

Run it with:  python -m worker.consumer
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from decision.engine import evaluate
from decision.models import ReconStatus

from .config import WorkerSettings, load_settings
from .idempotency import RedisIdempotencyFilter
from .normalize import envelope_to_event
from .state import PostgresStateStore

log = logging.getLogger("verisync.worker")

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
            "enable.partition.eof": False,
            "client.id": "verisync-worker",
        }
    )


def build_redis(settings: WorkerSettings):
    import redis

    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


class ReconciliationWorker:
    def __init__(
        self,
        *,
        consumer,
        store: PostgresStateStore,
        idempotency: RedisIdempotencyFilter,
        settings: WorkerSettings,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._idem = idempotency
        self._settings = settings
        self._recheck_delay = timedelta(seconds=settings.recheck_delay_seconds)
        self._stopping = False

    @classmethod
    def from_settings(
        cls, settings: Optional[WorkerSettings] = None
    ) -> "ReconciliationWorker":
        settings = settings or load_settings()
        return cls(
            consumer=build_consumer(settings),
            store=PostgresStateStore.from_settings(settings),
            idempotency=RedisIdempotencyFilter(
                build_redis(settings), ttl_seconds=settings.idempotency_ttl_seconds
            ),
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
                if self._handle_message(msg):
                    self._consumer.commit(message=msg, asynchronous=False)
        finally:
            self.close()

    def run_until_idle(self, *, idle_seconds: float = 3.0, max_seconds: float = 30.0) -> int:
        """Consume until no message arrives for ``idle_seconds`` (or after
        ``max_seconds`` total), then shut down cleanly. For the Phase 4
        checkpoint scripts and integration tests, not for production."""
        self._consumer.subscribe([self._settings.lifecycle_topic])
        processed = 0
        start = last_seen = time.monotonic()
        try:
            while not self._stopping:
                msg = self._consumer.poll(1.0)
                clock = time.monotonic()
                if clock - start > max_seconds:
                    break
                if msg is None:
                    if clock - last_seen > idle_seconds:
                        break
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() != _PARTITION_EOF:
                        log.error("consume error: %s", err)
                    continue
                last_seen = clock
                if self._handle_message(msg):
                    self._consumer.commit(message=msg, asynchronous=False)
                    processed += 1
        finally:
            self.close()
        return processed

    def _handle_message(self, msg) -> bool:
        """Process one message. Returns True when the offset is safe to commit.

        Unexpected errors propagate: the worker stops and, because the offset
        was not committed, the message is redelivered on restart.
        """
        try:
            envelope = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError) as exc:
            log.error(
                "undeserializable message at %s[%s]@%s: %s; skipping",
                msg.topic(), msg.partition(), msg.offset(), exc,
            )
            return True

        event = envelope_to_event(envelope)
        if event is None:
            log.error(
                "unusable envelope at %s[%s]@%s: %r; skipping",
                msg.topic(), msg.partition(), msg.offset(),
                _truncate(msg.value()),
            )
            return True

        if self._idem.seen(event.event_id):
            log.debug("pre-filter hit, skipping %s", event.event_id)
            return True

        now = datetime.now(timezone.utc)
        with self._store.connection() as conn:
            state = self._store.read_state(conn, event.order_id)
            current_status = (
                ReconStatus(state.row.status) if state.row is not None else None
            )
            evaluation = evaluate(
                event=event,
                view=state.view,
                current_status=current_status,
                now=now,
                recheck_delay=self._recheck_delay,
            )
            result = self._store.apply(
                conn,
                event=event,
                decision=evaluation.decision,
                projected_view=evaluation.view,
                prior_row=state.row,
            )

        self._idem.mark_seen(event.event_id)
        log.info(
            "event %s order=%s type=%s -> %s%s%s",
            event.event_id,
            event.order_id,
            event.event_type,
            evaluation.decision.action.value,
            "" if result.audit_written else " (replay, audit already present)",
            "" if result.pointer_moved or not result.audit_written else " (pointer unchanged)",
        )
        return True

    def close(self) -> None:
        for name, obj in (
            ("consumer", self._consumer),
            ("store", self._store),
            ("idempotency", self._idem),
        ):
            try:
                obj.close()
            except Exception as exc:
                log.warning("error closing %s: %s", name, exc)


def _truncate(value: Any, limit: int = 200) -> str:
    text = value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)
    return text if len(text) <= limit else text[:limit] + "..."


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

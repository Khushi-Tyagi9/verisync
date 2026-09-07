"""The simulated merchant service.

Consumes ``order-lifecycle-events`` (group ``merchant-sim``), and for each
Razorpay capture it has not seen before, assigns the order a behavior profile
and records it in ``merchant_orders``:

  immediate  -> emit 'pending' now, 'paid' a beat later
  lagging    -> emit 'pending' now, 'paid' after lag_seconds (past the recheck)
  stuck      -> emit 'pending' now, never 'paid'
  silent     -> emit nothing at all

A scheduler pass emits the delayed 'paid' events when their time comes. All
merchant events go back onto the same topic as ``source='merchant'``; the
simulator ignores non-Razorpay and non-capture messages, so it never reacts to
its own output.

Run it with:   python -m merchant_sim.simulator
Seed + run:    python -m merchant_sim.simulator --seed 20
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from ingestion.payload import build_envelope
from ingestion.publisher import KafkaLifecyclePublisher

from .config import MerchantSimSettings, load_settings
from .store import MerchantOrderStore

log = logging.getLogger("verisync.merchant_sim")

_PARTITION_EOF = -191
_CAPTURE_EVENT_TYPES = ("payment.captured", "order.paid")
_NO_PAID_PROFILES = frozenset({"stuck", "silent"})


def build_consumer(settings: MerchantSimSettings):
    from confluent_kafka import Consumer

    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
            "client.id": "verisync-merchant-sim",
        }
    )


def _dig(obj: Any, *path: str) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _capture_amount_currency(raw: Any) -> tuple[Optional[int], Optional[str]]:
    payload = raw.get("payload") if isinstance(raw, dict) else None
    for entity in ("payment", "order"):
        ent = _dig(payload, entity, "entity")
        if isinstance(ent, dict):
            amt = ent.get("amount")
            cur = ent.get("currency")
            return (
                amt if isinstance(amt, int) and not isinstance(amt, bool) else None,
                cur if isinstance(cur, str) and cur else None,
            )
    return None, None


class MerchantSimulator:
    def __init__(
        self,
        *,
        consumer,
        store: MerchantOrderStore,
        publisher,
        settings: MerchantSimSettings,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._publisher = publisher
        self._settings = settings
        self._rng = rng or random.Random()
        self._stopping = False
        self._last_scan = 0.0

    @classmethod
    def from_settings(cls, settings: Optional[MerchantSimSettings] = None) -> "MerchantSimulator":
        settings = settings or load_settings()
        return cls(
            consumer=build_consumer(settings),
            store=MerchantOrderStore.from_settings(settings),
            publisher=KafkaLifecyclePublisher.from_settings(settings),
            settings=settings,
        )

    def request_stop(self, *_a: Any) -> None:
        self._stopping = True

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        self._consumer.subscribe([self._settings.lifecycle_topic])
        log.info(
            "merchant sim up: group=%s topic=%s lag=%ss",
            self._settings.consumer_group,
            self._settings.lifecycle_topic,
            self._settings.lag_seconds,
        )
        try:
            while not self._stopping:
                msg = self._consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    if self._handle_message(msg):
                        self._consumer.commit(message=msg, asynchronous=False)
                elif msg is not None and msg.error().code() != _PARTITION_EOF:
                    log.error("consume error: %s", msg.error())
                self._maybe_scan()
        finally:
            self.close()

    def run_until_idle(self, *, idle_seconds: float = 3.0, max_seconds: float = 30.0) -> int:
        """Consume + scan until the topic goes quiet. For the checkpoint script."""
        self._consumer.subscribe([self._settings.lifecycle_topic])
        handled = 0
        start = last = time.monotonic()
        try:
            while not self._stopping:
                msg = self._consumer.poll(1.0)
                clock = time.monotonic()
                if clock - start > max_seconds:
                    break
                if msg is not None and msg.error() is None:
                    last = clock
                    if self._handle_message(msg):
                        self._consumer.commit(message=msg, asynchronous=False)
                        handled += 1
                elif msg is not None and msg.error().code() != _PARTITION_EOF:
                    log.error("consume error: %s", msg.error())
                did = self._maybe_scan(force=True)
                if did:
                    last = clock
                if msg is None and clock - last > idle_seconds:
                    break
        finally:
            self.close()
        return handled

    # ------------------------------------------------------------------ #
    def _handle_message(self, msg) -> bool:
        import json

        try:
            envelope = json.loads(msg.value())
        except (ValueError, TypeError):
            return True  # not ours to worry about

        if not isinstance(envelope, dict):
            return True
        if envelope.get("source") != "razorpay":
            return True
        if envelope.get("event_type") not in _CAPTURE_EVENT_TYPES:
            return True

        order_id = envelope.get("order_id")
        if not isinstance(order_id, str) or not order_id.strip():
            return True

        amount, currency = _capture_amount_currency(envelope.get("raw"))
        profile = self._pick_profile()
        now = datetime.now(timezone.utc)
        next_action_at = self._next_action_at(profile, now)

        with self._store.connection() as conn:
            created = self._store.create_if_absent(
                conn,
                order_id=order_id,
                amount=amount,
                currency=currency,
                profile=profile,
                next_action_at=next_action_at,
            )
        if not created:
            return True  # already tracking this order

        if profile != "silent":
            self._emit(order_id, "pending", amount, currency)
        log.info(
            "capture %s -> profile=%s%s",
            order_id,
            profile,
            "" if profile != "lagging" else f" (paid in {self._settings.lag_seconds}s)",
        )
        return True

    def _maybe_scan(self, *, force: bool = False) -> int:
        clock = time.monotonic()
        if not force and clock - self._last_scan < self._settings.scan_interval_seconds:
            return 0
        self._last_scan = clock

        now = datetime.now(timezone.utc)
        with self._store.connection() as conn:
            due = self._store.due_transitions(conn, now=now)
        fired = 0
        for d in due:
            with self._store.connection() as conn:
                claimed = self._store.mark_paid(conn, d.order_id)
            if claimed:
                self._emit(d.order_id, "paid", d.amount, d.currency)
                log.info("merchant %s -> paid", d.order_id)
                fired += 1
        return fired

    def _emit(self, order_id: str, status: str, amount, currency) -> None:
        raw = {
            "order_id": order_id,
            "status": status,
            "amount": amount,
            "currency": currency,
            "created_at": int(time.time()),
        }
        envelope = build_envelope(
            event_id=f"merch:{order_id}:{status}",
            event_type="order.updated",
            order_id=order_id,
            raw=raw,
            received_at=datetime.now(timezone.utc).isoformat(),
            source="merchant",
        )
        self._publisher.publish(order_id=order_id, envelope=envelope)

    def _pick_profile(self) -> str:
        if self._settings.force_profile:
            return self._settings.force_profile
        weights = self._settings.profile_weights
        names = list(weights)
        return self._rng.choices(names, weights=[weights[n] for n in names], k=1)[0]

    def _next_action_at(self, profile: str, now: datetime) -> Optional[datetime]:
        if profile in _NO_PAID_PROFILES:
            return None
        secs = (
            self._settings.immediate_seconds
            if profile == "immediate"
            else self._settings.lag_seconds
        )
        return now + timedelta(seconds=secs)

    def close(self) -> None:
        for name, obj in (
            ("consumer", self._consumer),
            ("store", self._store),
            ("publisher", self._publisher),
        ):
            try:
                obj.close()
            except Exception as exc:
                log.warning("error closing %s: %s", name, exc)


def emit_synthetic_captures(
    settings: MerchantSimSettings, count: int, *, amount: int = 50_000
) -> list[str]:
    """Produce `count` synthetic Razorpay payment.captured envelopes straight to
    the topic, so a checkpoint run needs neither the ingestion service nor a
    real webhook. Returns the order_ids."""
    publisher = KafkaLifecyclePublisher.from_settings(settings)
    order_ids: list[str] = []
    try:
        for _ in range(count):
            oid = f"order_SIM{uuid.uuid4().hex[:12]}"
            pay = {
                "id": f"pay_{uuid.uuid4().hex[:14]}",
                "entity": "payment",
                "amount": amount,
                "currency": "INR",
                "status": "captured",
                "order_id": oid,
                "method": "upi",
            }
            raw = {
                "entity": "event",
                "event": "payment.captured",
                "contains": ["payment"],
                "payload": {"payment": {"entity": pay}},
                "created_at": int(time.time()),
            }
            envelope = build_envelope(
                event_id=f"evt_{uuid.uuid4().hex[:16]}",
                event_type="payment.captured",
                order_id=oid,
                raw=raw,
                received_at=datetime.now(timezone.utc).isoformat(),
                source="razorpay",
            )
            publisher.publish(order_id=oid, envelope=envelope)
            order_ids.append(oid)
    finally:
        publisher.close()
    return order_ids


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--seed", type=int, default=0, metavar="N",
        help="first emit N synthetic Razorpay captures, then run",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="run until the topic is idle instead of forever (checkpoint mode)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    settings = load_settings()

    if args.seed > 0:
        ids = emit_synthetic_captures(settings, args.seed)
        log.info("seeded %d synthetic captures", len(ids))

    sim = MerchantSimulator.from_settings(settings)
    signal.signal(signal.SIGINT, sim.request_stop)
    signal.signal(signal.SIGTERM, sim.request_stop)
    if args.once:
        n = sim.run_until_idle()
        log.info("handled %d capture(s)", n)
    else:
        sim.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

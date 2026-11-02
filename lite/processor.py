"""Postgres-only reconciliation: the Kafka worker's job without Kafka or Redis.

Everything here is a thin driver over existing code:

* ``process_event``  -> ``worker.normalize`` + ``worker.reconcile`` (the same
  evaluate/apply path the Kafka consumer uses). Idempotency is the audit_log
  unique constraint alone.
* ``process_due_rechecks`` -> ``SweepService`` (``StuckProcessingSweep`` then
  ``PendingRecheckSweep``, which drives ``RecheckRunner.resolve_order``) with a
  delay queue that does nothing, so Postgres ``recheck_at`` is the only clock.
* ``process_due_merchant_transitions`` -> ``MerchantOrderStore`` due/mark_paid,
  feeding the merchant event straight into ``process_event`` instead of Kafka.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from merchant_sim.envelopes import merchant_envelope
from merchant_sim.store import MerchantOrderStore
from worker.config import WorkerSettings
from worker.normalize import envelope_to_event
from worker.reconcile import Reconciled, reconcile_event
from worker.recheck import RecheckRunner
from worker.state import PostgresStateStore
from worker.sweep import PendingRecheckSweep, StuckProcessingSweep, SweepService

log = logging.getLogger("verisync.lite")

_ARMED_RECHECKS = """
    SELECT order_id, recheck_at FROM orders_state
    WHERE order_id = ANY(%s) AND status = 'PENDING_RECHECK' AND recheck_at IS NOT NULL
"""


class NoopDelayQueue:
    """DelayQueue that holds nothing. orders_state.recheck_at in Postgres is the
    only record of when a recheck is due, and the pending sweep reads it."""

    def push(self, order_id: str, when: datetime, *, only_if_earlier: bool = True) -> None:
        return None

    def due(self, now: datetime, *, limit: int = 100) -> list[str]:
        return []

    def remove(self, order_id: str) -> None:
        return None

    def close(self) -> None:
        return None


@dataclass(frozen=True)
class RecheckCounts:
    reclaimed: int
    resolved: int


class LiteProcessor:
    def __init__(
        self,
        *,
        store: PostgresStateStore,
        merchant_store: MerchantOrderStore,
        settings: WorkerSettings,
        limit: int = 20,
    ) -> None:
        self._store = store
        self._merchant = merchant_store
        self._settings = settings
        self._recheck_delay = timedelta(seconds=settings.recheck_delay_seconds)
        self._limit = limit

    def process_event(self, envelope: dict, *, now: Optional[datetime] = None) -> Optional[Reconciled]:
        """Reconcile one lifecycle envelope. None if the envelope is unusable."""
        event = envelope_to_event(envelope)
        if event is None:
            log.error("unusable envelope dropped: %r", envelope.get("event_id") if isinstance(envelope, dict) else envelope)
            return None
        reconciled = reconcile_event(
            self._store,
            event,
            now=now or datetime.now(timezone.utc),
            recheck_delay=self._recheck_delay,
        )
        log.info(
            "event %s order=%s -> %s%s",
            event.event_id,
            event.order_id,
            reconciled.evaluation.decision.action.value,
            "" if reconciled.result.audit_written else " (replay, audit already present)",
        )
        return reconciled

    def process_due_rechecks(self, now: Optional[datetime] = None, limit: Optional[int] = None) -> RecheckCounts:
        """Reclaim stuck PROCESSING rows, then resolve every recheck whose
        recheck_at has passed (grace is 0 in this mode)."""
        now = now or datetime.now(timezone.utc)
        runner = RecheckRunner(
            store=self._store, delay_queue=NoopDelayQueue(), settings=self._settings
        )
        sweeps = SweepService(
            stuck=StuckProcessingSweep(store=self._store, settings=self._settings),
            pending=PendingRecheckSweep(
                store=self._store,
                runner=runner,
                settings=self._settings,
                limit=limit if limit is not None else self._limit,
            ),
            settings=self._settings,
        )
        # No sweeps.close(): it would close the shared store.
        reclaimed, resolved = sweeps.run_once(now)
        return RecheckCounts(reclaimed=reclaimed, resolved=resolved)

    def process_due_merchant_transitions(
        self,
        now: Optional[datetime] = None,
        limit: int = 20,
        *,
        before_rechecks: Optional[bool] = None,
    ) -> int:
        """Flip due merchant_orders to paid and reconcile the resulting merchant
        event directly. Returns how many fired.

        There is no clock running between ticks, so one tick can find both an
        order's paid transition and its recheck due. To process each order's
        events in the order they happened, the tick calls this twice around the
        recheck pass: ``before_rechecks=True`` takes transitions due no later
        than the order's armed recheck, ``False`` takes those due after it.
        None (the default) takes everything."""
        now = now or datetime.now(timezone.utc)
        with self._merchant.connection() as conn:
            due = self._merchant.due_transitions(conn, now=now, limit=limit)
        if before_rechecks is not None and due:
            with self._store.connection() as conn:
                armed = dict(
                    conn.execute(_ARMED_RECHECKS, ([d.order_id for d in due],)).fetchall()
                )
            due = [
                d
                for d in due
                if (
                    d.order_id not in armed
                    or d.next_action_at is None
                    or d.next_action_at <= armed[d.order_id]
                )
                == before_rechecks
            ]
        fired = 0
        for d in due:
            with self._merchant.connection() as conn:
                claimed = self._merchant.mark_paid(conn, d.order_id)
            if claimed:
                self.process_event(
                    merchant_envelope(d.order_id, "paid", d.amount, d.currency), now=now
                )
                fired += 1
        return fired

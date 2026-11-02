"""Order submission, the tick, and the live-orders listing for lite mode.

Locks are transaction-level (``pg_try_advisory_xact_lock`` /
``pg_advisory_xact_lock``) on purpose: the Neon pooler is PgBouncer in
transaction mode, where session-level advisory locks would leak onto whichever
server connection the pooler hands out next.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from merchant_sim.store import MerchantOrderStore

from .config import MAX_AMOUNT, MAX_ORDERS_PER_REQUEST, MIN_AMOUNT, LiteSettings
from .outcomes import classify
from .processor import LiteProcessor
from .scenarios import LIVE_PREFIX, SCENARIOS, build_plan, new_order_id, scenario_of

# Arbitrary, distinct 64-bit keys (ASCII of "verisync" variants).
TICK_LOCK_KEY = 0x76657269_73796E63
CAP_LOCK_KEY = 0x76657269_73796E64


class LiteError(ValueError):
    """A request the caller can fix; maps to HTTP 4xx."""


class CapExceeded(LiteError):
    """The global cap on live orders would be exceeded."""


@dataclass(frozen=True)
class TickResult:
    ran: bool                 # False: another instance held the lock
    reclaimed: int = 0
    rechecks_resolved: int = 0
    merchant_paid: int = 0


_COUNT_LIVE = "SELECT count(*) FROM merchant_orders WHERE starts_with(order_id, %s)"

_RECENT = """
    SELECT m.order_id, m.status, m.amount, m.created_at, s.status, s.retry_count,
           s.recheck_at,
           COALESCE((SELECT array_agg(r.reason ORDER BY r.id)
                     FROM review_queue r WHERE r.order_id = m.order_id), '{}')
    FROM merchant_orders m
    LEFT JOIN orders_state s ON s.order_id = m.order_id
    WHERE starts_with(m.order_id, %s)
    ORDER BY m.created_at DESC, m.order_id
    LIMIT %s
"""


def build_service(pool, lite_settings: Optional[LiteSettings] = None) -> "LiteService":
    """Wire the Postgres-only stack onto an existing psycopg pool."""
    from worker.state import PostgresStateStore

    from .config import load_lite_settings

    settings = lite_settings or load_lite_settings()
    store = PostgresStateStore(pool)
    merchant = MerchantOrderStore(pool)
    processor = LiteProcessor(
        store=store,
        merchant_store=merchant,
        settings=settings.worker,
        limit=settings.tick_limit,
    )
    return LiteService(
        pool=pool, processor=processor, merchant_store=merchant, settings=settings
    )


class LiteService:
    def __init__(
        self,
        *,
        pool,
        processor: LiteProcessor,
        merchant_store: MerchantOrderStore,
        settings: LiteSettings,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._pool = pool
        self._processor = processor
        self._merchant = merchant_store
        self._settings = settings
        self._clock = clock

    # ------------------------------------------------------------------ #
    def submit_orders(self, *, amount: int, scenario: str, count: int = 1) -> list[str]:
        if scenario not in SCENARIOS:
            raise LiteError(f"unknown scenario {scenario!r}; one of {sorted(SCENARIOS)}")
        if not isinstance(count, int) or not 1 <= count <= MAX_ORDERS_PER_REQUEST:
            raise LiteError(f"count must be between 1 and {MAX_ORDERS_PER_REQUEST}")
        if not isinstance(amount, int) or not MIN_AMOUNT <= amount <= MAX_AMOUNT:
            raise LiteError(f"amount must be between {MIN_AMOUNT} and {MAX_AMOUNT} (paise)")

        now = self._clock()
        plans: list[tuple[str, object]] = []
        # One short transaction: take the cap lock, count, create every
        # merchant_orders row. A concurrent request waits here, so the cap
        # cannot be overshot by racing submits.
        with self._pool.connection() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(%s)", (CAP_LOCK_KEY,))
            existing = conn.execute(_COUNT_LIVE, (LIVE_PREFIX,)).fetchone()[0]
            if existing + count > self._settings.max_live_orders:
                raise CapExceeded(
                    f"live order cap reached ({existing}/{self._settings.max_live_orders})"
                )
            for _ in range(count):
                order_id = new_order_id(scenario)
                plan = build_plan(
                    scenario,
                    order_id,
                    amount,
                    now=now,
                    immediate_seconds=self._settings.immediate_seconds,
                    lag_seconds=self._settings.lag_seconds,
                )
                self._merchant.create_if_absent(
                    conn,
                    order_id=order_id,
                    amount=amount,
                    currency="INR",
                    profile=plan.profile,
                    next_action_at=plan.next_action_at,
                )
                if plan.paid_now:
                    self._merchant.mark_paid(conn, order_id)
                plans.append((order_id, plan))

        for order_id, plan in plans:
            for envelope in plan.envelopes:
                self._processor.process_event(envelope, now=now)
        return [order_id for order_id, _ in plans]

    # ------------------------------------------------------------------ #
    def tick(self, now: Optional[datetime] = None, *, budget_seconds: float = 6.0) -> TickResult:
        """Run one bounded pass of the three processors, if no other instance is
        mid-tick. The lock lives exactly as long as this transaction."""
        now = now or self._clock()
        limit = self._settings.tick_limit
        deadline = time.monotonic() + budget_seconds
        with self._pool.connection() as lock_conn:
            got = lock_conn.execute(
                "SELECT pg_try_advisory_xact_lock(%s)", (TICK_LOCK_KEY,)
            ).fetchone()[0]
            if not got:
                return TickResult(ran=False)
            # Chronological per order: a paid transition due before the order's
            # recheck goes first, one due after it goes last (and finds the
            # order already resolved).
            paid = self._processor.process_due_merchant_transitions(
                now, limit, before_rechecks=True
            )
            rechecks = self._processor.process_due_rechecks(now, limit)
            if time.monotonic() < deadline:
                paid += self._processor.process_due_merchant_transitions(
                    now, limit, before_rechecks=False
                )
        return TickResult(
            ran=True,
            reclaimed=rechecks.reclaimed,
            rechecks_resolved=rechecks.resolved,
            merchant_paid=paid,
        )

    # ------------------------------------------------------------------ #
    def recent_orders(self, limit: int = 25) -> list[dict]:
        now = self._clock()
        with self._pool.connection() as conn:
            rows = conn.execute(_RECENT, (LIVE_PREFIX, limit)).fetchall()
        out = []
        for order_id, m_status, amount, created_at, status, retries, recheck_at, reasons in rows:
            key = scenario_of(order_id)
            spec = SCENARIOS.get(key) if key else None
            outcome = classify(status, reasons or [])
            out.append(
                {
                    "order_id": order_id,
                    "scenario": key,
                    "scenario_label": spec.label if spec else None,
                    "amount": amount,
                    "merchant_status": m_status,
                    "created_at": created_at.isoformat() if created_at else None,
                    "retry_count": retries,
                    "recheck_in_seconds": (
                        max(0, int((recheck_at - now).total_seconds()))
                        if recheck_at is not None and status == "PENDING_RECHECK"
                        else None
                    ),
                    "outcome": {
                        "tag": outcome.tag,
                        "label": outcome.label,
                        "tone": outcome.tone,
                        "terminal": outcome.terminal,
                    },
                    "expected_tag": spec.expected_tag if spec else None,
                    "why": spec.why if spec else None,
                }
            )
        return out

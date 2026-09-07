"""Postgres access for the simulator's own merchant_orders table."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

_INSERT_IF_ABSENT = """
    INSERT INTO merchant_orders
      (order_id, status, amount, currency, profile, next_action_at)
    VALUES (%(order_id)s, 'pending', %(amount)s, %(currency)s, %(profile)s, %(next_action_at)s)
    ON CONFLICT (order_id) DO NOTHING
    RETURNING order_id
"""

_DUE = """
    SELECT order_id, amount, currency
    FROM merchant_orders
    WHERE status = 'pending'
      AND next_action_at IS NOT NULL
      AND next_action_at <= %(now)s
    ORDER BY next_action_at
    LIMIT %(limit)s
"""

# Conditional so two scanner passes cannot both emit the paid event.
_MARK_PAID = """
    UPDATE merchant_orders
    SET status = 'paid', next_action_at = NULL, updated_at = NOW()
    WHERE order_id = %(order_id)s AND status = 'pending'
    RETURNING order_id
"""


@dataclass(frozen=True)
class DueOrder:
    order_id: str
    amount: Optional[int]
    currency: Optional[str]


class MerchantOrderStore:
    def __init__(self, pool) -> None:
        self._pool = pool

    @classmethod
    def from_settings(cls, settings) -> "MerchantOrderStore":
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(
            settings.postgres_dsn, min_size=1, max_size=4, open=True, timeout=10
        )
        return cls(pool)

    def connection(self):
        return self._pool.connection()

    def create_if_absent(
        self,
        conn,
        *,
        order_id: str,
        amount: Optional[int],
        currency: Optional[str],
        profile: str,
        next_action_at: Optional[datetime],
    ) -> bool:
        row = conn.execute(
            _INSERT_IF_ABSENT,
            {
                "order_id": order_id,
                "amount": amount,
                "currency": currency,
                "profile": profile,
                "next_action_at": next_action_at,
            },
        ).fetchone()
        return row is not None

    def due_transitions(self, conn, *, now: datetime, limit: int = 200) -> list[DueOrder]:
        rows = conn.execute(_DUE, {"now": now, "limit": limit}).fetchall()
        return [DueOrder(order_id=r[0], amount=r[1], currency=r[2]) for r in rows]

    def mark_paid(self, conn, order_id: str) -> bool:
        return conn.execute(_MARK_PAID, {"order_id": order_id}).fetchone() is not None

    def close(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass

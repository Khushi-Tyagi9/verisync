"""Recording out-of-scope events (no order_id present).

This is a quiet stat counter, not an anomaly channel. The INSERT is
ON CONFLICT (event_id) DO NOTHING so webhook retries of the same event do not
inflate the count.
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class OutOfScopeStore(Protocol):
    def record(self, *, event_id: str, event_type: Optional[str], payload: dict) -> None: ...

    def close(self) -> None: ...


class PostgresOutOfScopeStore:
    _INSERT = """
        INSERT INTO out_of_scope_events (event_id, event_type, reason, payload)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """

    def __init__(self, pool) -> None:
        self._pool = pool

    @classmethod
    def from_settings(cls, settings) -> "PostgresOutOfScopeStore":
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(
            settings.postgres_dsn, min_size=1, max_size=4, open=True, timeout=10
        )
        return cls(pool)

    def record(self, *, event_id: str, event_type: Optional[str], payload: dict) -> None:
        from psycopg.types.json import Json

        with self._pool.connection() as conn:
            conn.execute(self._INSERT, (event_id, event_type, "no_order_id", Json(payload)))

    def close(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass

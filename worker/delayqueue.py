"""Redis sorted-set delay queue for armed rechecks.

This is the *fast path* only, not the source of truth. The authoritative record
that a recheck is due is ``orders_state.recheck_at`` in Postgres; the Postgres
fallback sweep catches anything that never made it here (a crash between the
orders_state write and the push). Entries are scored by the recheck deadline as
a unix timestamp; the member is the ``order_id``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

_QUEUE_KEY = "recheck:queue"


@runtime_checkable
class DelayQueue(Protocol):
    def push(self, order_id: str, when: datetime, *, only_if_earlier: bool = True) -> None: ...

    def due(self, now: datetime, *, limit: int = 100) -> list[str]: ...

    def remove(self, order_id: str) -> None: ...


def _epoch(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class RedisDelayQueue:
    def __init__(self, redis_client, *, key: str = _QUEUE_KEY) -> None:
        self._redis = redis_client
        self._key = key

    def push(self, order_id: str, when: datetime, *, only_if_earlier: bool = True) -> None:
        """Schedule (or reschedule) a recheck.

        ``only_if_earlier`` (the default) uses ``ZADD ... LT`` so a repeat
        detection can only pull the deadline in, never push it out -- the same
        "earliest wins" rule ``LEAST()`` enforces in Postgres. The recheck
        runner passes ``only_if_earlier=False`` when it deliberately re-arms a
        fresh window after a recheck has already fired.
        """
        self._redis.zadd(self._key, {order_id: _epoch(when)}, lt=only_if_earlier)

    def due(self, now: datetime, *, limit: int = 100) -> list[str]:
        return list(
            self._redis.zrangebyscore(self._key, "-inf", _epoch(now), start=0, num=limit)
        )

    def remove(self, order_id: str) -> None:
        self._redis.zrem(self._key, order_id)

    def close(self) -> None:
        try:
            self._redis.close()
        except Exception:
            pass


class InMemoryDelayQueue:
    """For tests."""

    def __init__(self) -> None:
        self._scores: dict[str, float] = {}

    def push(self, order_id: str, when: datetime, *, only_if_earlier: bool = True) -> None:
        score = _epoch(when)
        if only_if_earlier and order_id in self._scores:
            self._scores[order_id] = min(self._scores[order_id], score)
        else:
            self._scores[order_id] = score

    def due(self, now: datetime, *, limit: int = 100) -> list[str]:
        cutoff = _epoch(now)
        ready = sorted((s, oid) for oid, s in self._scores.items() if s <= cutoff)
        return [oid for _, oid in ready[:limit]]

    def remove(self, order_id: str) -> None:
        self._scores.pop(order_id, None)

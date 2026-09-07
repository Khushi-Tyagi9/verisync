"""The fast pre-filter half of the worker's dual idempotency.

Authoritative dedup is the UNIQUE(event_id) constraint on audit_log; that insert
failing on conflict IS the guarantee. This Redis key is only a cheap early
rejection in front of it, with a 48h TTL, and is populated *after* the
authoritative Postgres write commits so a crash between the two can never make a
never-processed event look processed.

Key: ``idempotency:{event_id}`` (event_id is globally unique, matching the
audit_log constraint, so the source is not part of the key).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

_KEY_PREFIX = "idempotency:"


@runtime_checkable
class IdempotencyFilter(Protocol):
    def seen(self, event_id: str) -> bool: ...

    def mark_seen(self, event_id: str) -> None: ...


class RedisIdempotencyFilter:
    def __init__(self, redis_client, *, ttl_seconds: int) -> None:
        self._redis = redis_client
        self._ttl = ttl_seconds

    def seen(self, event_id: str) -> bool:
        try:
            return bool(self._redis.exists(_KEY_PREFIX + event_id))
        except Exception:
            # Redis unavailable: fall through to the authoritative layer rather
            # than blocking ingestion. A false "not seen" is safe here.
            return False

    def mark_seen(self, event_id: str) -> None:
        try:
            self._redis.set(_KEY_PREFIX + event_id, "1", ex=self._ttl)
        except Exception:
            # Losing the pre-filter entry only costs one redundant Postgres
            # round trip on a later replay; the unique constraint still holds.
            pass

    def close(self) -> None:
        try:
            self._redis.close()
        except Exception:
            pass


class InMemoryIdempotencyFilter:
    """For tests. No TTL semantics; just presence."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def seen(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        self._seen.add(event_id)

"""Postgres write path for the reconciliation worker.

Holds every SQL statement that touches ``orders_state``, ``order_views``,
``audit_log`` and ``review_queue``. The pure decision layer decides *what* to do;
this module is the only place that does it.

Design points enforced here:

* audit_log INSERT is ``ON CONFLICT (event_id) DO NOTHING``. A missing RETURNING
  row means a replay slipped past the Redis pre-filter and the authoritative
  layer caught it: the orders_state effect is then skipped entirely.
* The ARM_RECHECK upsert preserves the earliest deadline with ``LEAST()`` and
  never runs against a ``PROCESSING`` row. When a prior orders_state row was
  read, it is additionally guarded by ``updated_at <= last_seen_updated_at`` so
  a stale in-flight arm cannot resurrect a row another writer just resolved;
  when no prior row was read, that guard is omitted so a rebalance/zombie race
  still merges a legitimately earlier deadline instead of dropping it.
* Every terminal transition writes ``recheck_at = NULL`` in the same statement
  that moves the status, honoring ``Decision.clear_recheck_at`` so a later drift
  episode cannot inherit a stale deadline through ``LEAST()``.
* Terminal / natural-resolution updates from the consumer are conditional on the
  status the decision was based on and may legitimately match zero rows when a
  recheck claim raced ahead; that is reported, not treated as an error.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from decision.models import Action, AuditEntry, Decision, Event, OrderView, ReviewItem
from .viewcodec import deserialize_view, serialize_view

# --------------------------------------------------------------------------- #
# Read model
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OrderRow:
    status: str
    recheck_at: Optional[datetime]
    retry_count: int
    claim_token: Optional[str]
    updated_at: datetime


@dataclass(frozen=True)
class ReadState:
    row: Optional[OrderRow]           # orders_state, None until a pointer exists
    view: Optional[OrderView]         # rebuilt from order_views, None if absent


@dataclass(frozen=True)
class ApplyResult:
    audit_written: bool               # False => replay caught by unique constraint
    pointer_moved: bool               # False => conditional update no-op'd (race) or n/a


# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #
_SELECT_ORDER = """
    SELECT status, recheck_at, retry_count, claim_token, updated_at
    FROM orders_state WHERE order_id = %s
"""

_SELECT_VIEW = "SELECT view FROM order_views WHERE order_id = %s"

_UPSERT_VIEW = """
    INSERT INTO order_views (order_id, view, updated_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (order_id) DO UPDATE
    SET view = EXCLUDED.view, updated_at = NOW()
"""

_INSERT_AUDIT = """
    INSERT INTO audit_log
      (event_id, order_id, sub_entity_type, sub_entity_id, source, event_type,
       old_state, new_state, decision, direction, amount, currency, payload)
    VALUES
      (%(event_id)s, %(order_id)s, %(sub_entity_type)s, %(sub_entity_id)s,
       %(source)s, %(event_type)s, %(old_state)s, %(new_state)s,
       %(decision)s, %(direction)s, %(amount)s, %(currency)s, %(payload)s)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING id
"""

# No prior orders_state row was read: plain INSERT normally wins; the ON CONFLICT
# branch only fires under a rebalance/zombie race and merges unconditionally
# (nothing to resurrect, and a legitimately earlier deadline must be kept).
_ARM_RECHECK_FRESH = """
    INSERT INTO orders_state (order_id, status, recheck_at, updated_at)
    VALUES (%(order_id)s, 'PENDING_RECHECK', %(recheck_at)s, NOW())
    ON CONFLICT (order_id) DO UPDATE
    SET status     = 'PENDING_RECHECK',
        recheck_at = LEAST(orders_state.recheck_at, EXCLUDED.recheck_at),
        updated_at = NOW()
    WHERE orders_state.status <> 'PROCESSING'
    RETURNING order_id
"""

# A prior row was read: the ON CONFLICT branch always runs, guarded by the
# optimistic-concurrency check on the row this decision was based on.
_ARM_RECHECK_GUARDED = """
    INSERT INTO orders_state (order_id, status, recheck_at, updated_at)
    VALUES (%(order_id)s, 'PENDING_RECHECK', %(recheck_at)s, NOW())
    ON CONFLICT (order_id) DO UPDATE
    SET status     = 'PENDING_RECHECK',
        recheck_at = LEAST(orders_state.recheck_at, EXCLUDED.recheck_at),
        updated_at = NOW()
    WHERE orders_state.status <> 'PROCESSING'
      AND orders_state.updated_at <= %(last_seen_updated_at)s
    RETURNING order_id
"""

_ROUTE_DISPUTED = """
    INSERT INTO orders_state (order_id, status, recheck_at, retry_count, updated_at)
    VALUES (%(order_id)s, 'DISPUTED', NULL, 0, NOW())
    ON CONFLICT (order_id) DO UPDATE
    SET status = 'DISPUTED', recheck_at = NULL, updated_at = NOW()
    WHERE orders_state.status <> 'PROCESSING'
      AND orders_state.status <> 'DISPUTED'
    RETURNING order_id
"""

_RESOLVE_NATURALLY = """
    UPDATE orders_state
    SET status = 'RESOLVED_NATURALLY', recheck_at = NULL, updated_at = NOW()
    WHERE order_id = %(order_id)s AND status = 'PENDING_RECHECK'
    RETURNING order_id
"""

_ENQUEUE_REVIEW = """
    INSERT INTO review_queue (order_id, sub_entity_id, event_id, reason, details)
    VALUES (%(order_id)s, %(sub_entity_id)s, %(event_id)s, %(reason)s, %(details)s)
    ON CONFLICT (event_id) WHERE event_id IS NOT NULL DO NOTHING
"""

# Atomic claim: a fresh server-generated fencing token in the same statement
# that flips the status. 0 rows => not claimable (already claimed, resolved, or
# not actually pending).
_CLAIM_RECHECK = """
    UPDATE orders_state
    SET status = 'PROCESSING', claim_token = gen_random_uuid(), updated_at = NOW()
    WHERE order_id = %(order_id)s AND status = 'PENDING_RECHECK'
    RETURNING claim_token
"""

# Completion verifies the token, not just the status. 0 rows => the claim was
# stolen (reclaimed past the watchdog) and the caller must not write audit_log.
_COMPLETE_CLAIM = """
    UPDATE orders_state
    SET status = %(new_status)s, recheck_at = NULL, claim_token = NULL, updated_at = NOW()
    WHERE order_id = %(order_id)s AND status = 'PROCESSING' AND claim_token = %(token)s
    RETURNING order_id
"""

# Bounded re-arm from a held claim: increment retry_count in the same statement
# as the status flip and token clear, and route to DEAD_LETTER at the ceiling
# instead of re-arming forever. Same shape as the stuck-PROCESSING sweep's
# reclaim (worker/sweep.py) so the retry budget is shared.
_REARM_OR_DEADLETTER = """
    UPDATE orders_state
    SET status = CASE WHEN retry_count + 1 >= %(max_retries)s
                      THEN 'DEAD_LETTER' ELSE 'PENDING_RECHECK' END,
        retry_count = retry_count + 1,
        recheck_at = CASE WHEN retry_count + 1 >= %(max_retries)s
                          THEN NULL ELSE %(fresh_recheck_at)s END,
        claim_token = NULL,
        updated_at = NOW()
    WHERE order_id = %(order_id)s AND status = 'PROCESSING' AND claim_token = %(token)s
    RETURNING status, retry_count
"""


def _json(value):
    from psycopg.types.json import Json

    return Json(value)


class PostgresStateStore:
    def __init__(self, pool) -> None:
        self._pool = pool

    @classmethod
    def from_settings(cls, settings) -> "PostgresStateStore":
        from psycopg_pool import ConnectionPool

        pool = ConnectionPool(
            settings.postgres_dsn, min_size=1, max_size=4, open=True, timeout=10
        )
        return cls(pool)

    def connection(self):
        """A pooled connection context manager. psycopg commits on clean exit
        and rolls back if the block raises."""
        return self._pool.connection()

    # ------------------------------------------------------------------ #
    # Read
    # ------------------------------------------------------------------ #
    def read_state(self, conn, order_id: str) -> ReadState:
        rec = conn.execute(_SELECT_ORDER, (order_id,)).fetchone()
        row = (
            OrderRow(
                status=rec[0],
                recheck_at=rec[1],
                retry_count=rec[2],
                claim_token=str(rec[3]) if rec[3] is not None else None,
                updated_at=rec[4],
            )
            if rec is not None
            else None
        )
        vrec = conn.execute(_SELECT_VIEW, (order_id,)).fetchone()
        view = deserialize_view(vrec[0], order_id=order_id) if vrec is not None else None
        return ReadState(row=row, view=view)

    # ------------------------------------------------------------------ #
    # Write (one event, one transaction owned by the caller)
    # ------------------------------------------------------------------ #
    def apply(
        self,
        conn,
        *,
        event: Event,
        decision: Decision,
        projected_view: OrderView,
        prior_row: Optional[OrderRow],
    ) -> ApplyResult:
        if not self._insert_audit(conn, decision.audit, payload=event.raw):
            # Replay: authoritative layer rejected it. Leave order_views and the
            # pointer exactly as the first processing left them.
            return ApplyResult(audit_written=False, pointer_moved=False)

        conn.execute(_UPSERT_VIEW, (event.order_id, _json(serialize_view(projected_view))))

        pointer_moved = self._apply_pointer(conn, event, decision, prior_row)
        return ApplyResult(audit_written=True, pointer_moved=pointer_moved)

    # ------------------------------------------------------------------ #
    def _insert_audit(self, conn, audit: AuditEntry, *, payload) -> bool:
        params = {
            "event_id": audit.event_id,
            "order_id": audit.order_id,
            "sub_entity_type": audit.sub_entity_type,
            "sub_entity_id": audit.sub_entity_id,
            "source": audit.source.value,
            "event_type": audit.event_type,
            "old_state": audit.old_state,
            "new_state": audit.new_state,
            "decision": audit.decision,
            "direction": audit.direction,
            "amount": audit.amount,
            "currency": audit.currency,
            "payload": _json(payload) if payload is not None else None,
        }
        return conn.execute(_INSERT_AUDIT, params).fetchone() is not None

    def _apply_pointer(
        self,
        conn,
        event: Event,
        decision: Decision,
        prior_row: Optional[OrderRow],
    ) -> bool:
        action = decision.action

        if action is Action.ARM_RECHECK:
            if prior_row is None:
                res = conn.execute(
                    _ARM_RECHECK_FRESH,
                    {"order_id": event.order_id, "recheck_at": decision.recheck_at},
                ).fetchone()
            else:
                res = conn.execute(
                    _ARM_RECHECK_GUARDED,
                    {
                        "order_id": event.order_id,
                        "recheck_at": decision.recheck_at,
                        "last_seen_updated_at": prior_row.updated_at,
                    },
                ).fetchone()
            return res is not None

        if action is Action.ROUTE_DISPUTED:
            res = conn.execute(_ROUTE_DISPUTED, {"order_id": event.order_id}).fetchone()
            return res is not None

        if action is Action.MARK_RESOLVED_NATURALLY:
            res = conn.execute(_RESOLVE_NATURALLY, {"order_id": event.order_id}).fetchone()
            return res is not None

        if action is Action.FLAG_FOR_REVIEW and decision.review is not None:
            self._enqueue_review(conn, decision.review)
            return False

        # NO_OP, LOCKED_REPLAY_IGNORED, and APPLY_AUTOCORRECT (which only the
        # recheck-claim path produces, never the consumer) touch no pointer.
        return False

    def _enqueue_review(self, conn, review: ReviewItem) -> None:
        conn.execute(
            _ENQUEUE_REVIEW,
            {
                "order_id": review.order_id,
                "sub_entity_id": review.sub_entity_id,
                "event_id": review.event_id,
                "reason": review.reason,
                "details": _json(dict(review.details)) if review.details else None,
            },
        )

    # ------------------------------------------------------------------ #
    # Recheck claim / completion (used by worker/recheck.py and the sweeps)
    # ------------------------------------------------------------------ #
    def claim_recheck(self, conn, order_id: str) -> Optional[str]:
        """Atomically claim a PENDING_RECHECK row. Returns the fresh fencing
        token, or None if the row was not claimable."""
        rec = conn.execute(_CLAIM_RECHECK, {"order_id": order_id}).fetchone()
        return str(rec[0]) if rec is not None else None

    def complete_claim(self, conn, *, order_id: str, token: str, new_status: str) -> bool:
        """Token-verified terminal completion. False => claim was stolen; the
        caller must not have written audit_log in this transaction."""
        rec = conn.execute(
            _COMPLETE_CLAIM,
            {"order_id": order_id, "token": token, "new_status": new_status},
        ).fetchone()
        return rec is not None

    def rearm_or_deadletter(
        self,
        conn,
        *,
        order_id: str,
        token: str,
        fresh_recheck_at: datetime,
        max_retries: int,
    ) -> Optional[tuple[str, int]]:
        """Token-verified bounded re-arm. Returns (new_status, retry_count), or
        None if the claim was stolen."""
        rec = conn.execute(
            _REARM_OR_DEADLETTER,
            {
                "order_id": order_id,
                "token": token,
                "fresh_recheck_at": fresh_recheck_at,
                "max_retries": max_retries,
            },
        ).fetchone()
        return (rec[0], rec[1]) if rec is not None else None

    def insert_audit(self, conn, audit: AuditEntry, *, payload=None) -> bool:
        """Public wrapper: insert one audit_log row, ON CONFLICT DO NOTHING.
        False => the event_id already existed."""
        return self._insert_audit(conn, audit, payload=payload)

    def enqueue_review(self, conn, review: ReviewItem) -> None:
        self._enqueue_review(conn, review)

    def close(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass

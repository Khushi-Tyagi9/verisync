"""Read-only Postgres queries + pure shaping for the dashboard summary.

``assemble_summary`` is a pure function over already-fetched rows so it can be
unit-tested without a database; ``build_summary`` runs the SQL and calls it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Optional

# orders_state.status groupings (mirrors the CHECK constraint in db/schema.sql).
_RESOLVED = ("RESOLVED_AUTOCORRECTED", "RESOLVED_NATURALLY")
_IN_PROGRESS = ("PENDING_RECHECK", "PROCESSING")
_ALL_STATUSES = _RESOLVED + _IN_PROGRESS + ("DISPUTED", "DEAD_LETTER")


def assemble_summary(
    *,
    status_counts: dict[str, int],
    orders_tracked: int,
    orphan_pointers: int,
    review_open_by_reason: dict[str, int],
    review_recent: list[dict],
    out_of_scope_total: int,
    decision_counts: dict[str, int],
    activity: list[dict],
    generated_at: Optional[datetime] = None,
) -> dict:
    by_status = {s: int(status_counts.get(s, 0)) for s in _ALL_STATUSES}
    pointer_total = sum(by_status.values())
    resolved = sum(by_status[s] for s in _RESOLVED)
    in_progress = sum(by_status[s] for s in _IN_PROGRESS)
    review_open = sum(review_open_by_reason.values())

    return {
        "generated_at": (generated_at or datetime.now(timezone.utc)).isoformat(),
        "orders": {
            # Every order any event has been seen for.
            "tracked": int(orders_tracked),
            # Of those, how many the reconciliation pointer has an opinion on.
            "with_pointer": pointer_total,
            # ...and how many never needed a reconciliation action.
            "no_action_needed": max(int(orders_tracked) - pointer_total, 0),
            # An orders_state row with no order_views row. apply() writes both in
            # one transaction and the recheck/sweep paths only ever UPDATE
            # existing rows, so this is 0 in normal operation; a non-zero value
            # means something wrote orders_state outside the consumer.
            "orphan_pointers": int(orphan_pointers),
            "by_status": by_status,
            "auto_corrected": by_status["RESOLVED_AUTOCORRECTED"],
            "resolved_naturally": by_status["RESOLVED_NATURALLY"],
            "resolved_total": resolved,
            "disputed": by_status["DISPUTED"],
            "dead_letter": by_status["DEAD_LETTER"],
            "in_progress": in_progress,
        },
        # Actionable anomaly queue. Kept distinct from out_of_scope on purpose.
        "review_queue": {
            "open": review_open,
            "by_reason": dict(review_open_by_reason),
            "recent": review_recent,
        },
        # Quiet stat: routine order-less traffic, nothing to action.
        "out_of_scope": {"total": int(out_of_scope_total)},
        "decisions": dict(decision_counts),
        "activity": activity,
    }


def _counts(rows: Iterable[tuple]) -> dict[str, int]:
    return {str(k): int(v) for k, v in rows if k is not None}


def _iso(value) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


_SQL_STATUS_COUNTS = "SELECT status, count(*) FROM orders_state GROUP BY status"
_SQL_ORDERS_TRACKED = "SELECT count(*) FROM order_views"
_SQL_ORPHAN_POINTERS = """
    SELECT count(*) FROM orders_state s
    LEFT JOIN order_views v USING (order_id)
    WHERE v.order_id IS NULL
"""
_SQL_REVIEW_OPEN = (
    "SELECT reason, count(*) FROM review_queue WHERE status = 'OPEN' GROUP BY reason"
)
_SQL_REVIEW_RECENT = """
    SELECT order_id, reason, event_id, details, created_at
    FROM review_queue
    WHERE status = 'OPEN'
    ORDER BY created_at DESC, id DESC
    LIMIT 20
"""
_SQL_OOS_TOTAL = "SELECT count(*) FROM out_of_scope_events"
_SQL_DECISIONS = "SELECT decision, count(*) FROM audit_log GROUP BY decision"
_SQL_ACTIVITY = """
    SELECT created_at, order_id, source, event_type, decision, direction,
           old_state, new_state
    FROM audit_log
    ORDER BY id DESC
    LIMIT %(limit)s
"""


def build_summary(conn, *, activity_limit: int = 30) -> dict:
    status_counts = _counts(conn.execute(_SQL_STATUS_COUNTS).fetchall())
    orders_tracked = conn.execute(_SQL_ORDERS_TRACKED).fetchone()[0]
    orphan_pointers = conn.execute(_SQL_ORPHAN_POINTERS).fetchone()[0]
    review_open = _counts(conn.execute(_SQL_REVIEW_OPEN).fetchall())
    oos_total = conn.execute(_SQL_OOS_TOTAL).fetchone()[0]
    decision_counts = _counts(conn.execute(_SQL_DECISIONS).fetchall())

    review_recent = [
        {
            "order_id": r[0],
            "reason": r[1],
            "event_id": r[2],
            "details": r[3],
            "at": _iso(r[4]),
        }
        for r in conn.execute(_SQL_REVIEW_RECENT).fetchall()
    ]

    activity = [
        {
            "at": _iso(r[0]),
            "order_id": r[1],
            "source": r[2],
            "event_type": r[3],
            "decision": r[4],
            "direction": r[5],
            "old_state": r[6],
            "new_state": r[7],
        }
        for r in conn.execute(_SQL_ACTIVITY, {"limit": activity_limit}).fetchall()
    ]

    return assemble_summary(
        status_counts=status_counts,
        orders_tracked=orders_tracked,
        orphan_pointers=orphan_pointers,
        review_open_by_reason=review_open,
        review_recent=review_recent,
        out_of_scope_total=oos_total,
        decision_counts=decision_counts,
        activity=activity,
    )

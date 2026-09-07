"""Pure extraction helpers over a parsed Razorpay webhook body, plus the
normalized envelope we publish to Kafka.

No I/O. The envelope keeps the full raw body under `raw` so the worker can do
all semantic interpretation (status mapping, sub-entity IDs, amounts) itself.
"""

from __future__ import annotations

from typing import Any, Optional

ENVELOPE_SCHEMA = "verisync.lifecycle.v1"

# Where an order_id can legitimately appear across event families. Order
# matters only for readability; the first non-empty string wins.
_ORDER_ID_PATHS: tuple[tuple[str, ...], ...] = (
    ("payload", "payment", "entity", "order_id"),
    ("payload", "order", "entity", "id"),
    ("payload", "refund", "entity", "order_id"),
    ("payload", "settlement", "entity", "order_id"),
)


def _dig(obj: Any, path: tuple[str, ...]) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def extract_order_id(body: Any) -> Optional[str]:
    """Return the order_id if present and non-empty, else None.

    Razorpay sends `order_id: null` for order-less flows (Payment Links, some
    legacy paths) -- those correctly fall through to None here and get routed
    to out_of_scope_events by the caller.
    """
    if not isinstance(body, dict):
        return None
    for path in _ORDER_ID_PATHS:
        val = _dig(body, path)
        if isinstance(val, str) and val.strip():
            return val
    return None


def extract_event_type(body: Any) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    ev = body.get("event")
    return ev if isinstance(ev, str) and ev else None


def extract_occurred_at(body: Any) -> Optional[int]:
    """Razorpay `created_at` is a unix timestamp (seconds)."""
    if not isinstance(body, dict):
        return None
    ts = body.get("created_at")
    return ts if isinstance(ts, int) else None


def build_envelope(
    *,
    event_id: str,
    event_type: Optional[str],
    order_id: str,
    raw: dict,
    received_at: str,
    source: str = "razorpay",
) -> dict:
    return {
        "schema": ENVELOPE_SCHEMA,
        "event_id": event_id,
        "source": source,
        "event_type": event_type,
        "order_id": order_id,
        "occurred_at": extract_occurred_at(raw),
        "received_at": received_at,
        "raw": raw,
    }

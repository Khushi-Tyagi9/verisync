"""Envelope builders shared by the Kafka simulator and the lite processor.

Pure. They build the same ``verisync.lifecycle.v1`` envelopes ingestion would
publish, so downstream code cannot tell synthetic traffic from a webhook.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional

from ingestion.payload import build_envelope


def merchant_envelope(
    order_id: str, status: str, amount: Optional[int], currency: Optional[str]
) -> dict:
    raw = {
        "order_id": order_id,
        "status": status,
        "amount": amount,
        "currency": currency,
        "created_at": int(time.time()),
    }
    return build_envelope(
        event_id=f"merch:{order_id}:{status}",
        event_type="order.updated",
        order_id=order_id,
        raw=raw,
        received_at=datetime.now(timezone.utc).isoformat(),
        source="merchant",
    )


def razorpay_envelope(
    *,
    event_id: str,
    event_type: str,
    order_id: str,
    entity_key: str,
    entity: dict[str, Any],
) -> dict:
    """A Razorpay webhook-shaped envelope; `entity_key` is the payload member
    ("payment", "refund", ...) the entity sits under."""
    raw = {
        "entity": "event",
        "event": event_type,
        "contains": [entity_key],
        "payload": {entity_key: {"entity": entity}},
        "created_at": int(time.time()),
    }
    return build_envelope(
        event_id=event_id,
        event_type=event_type,
        order_id=order_id,
        raw=raw,
        received_at=datetime.now(timezone.utc).isoformat(),
        source="razorpay",
    )

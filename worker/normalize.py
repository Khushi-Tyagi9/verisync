"""Turn a Kafka lifecycle envelope into a normalized ``decision.Event``.

Pure. No I/O, no clients. The envelope carries the full raw Razorpay body under
``raw``; all semantic interpretation (status mapping, sub-entity IDs, amounts)
happens here so the decision layer only ever sees the normalized shape.

Envelope shape (produced by ``ingestion/payload.build_envelope``):

    {
      "schema": "verisync.lifecycle.v1",
      "event_id": "evt_...",
      "source": "razorpay" | "merchant",
      "event_type": "payment.captured" | "order.updated" | ...,
      "order_id": "order_...",
      "occurred_at": 1725710400 | null,      # unix seconds
      "received_at": "2026-09-07T12:00:00+00:00",
      "raw": { ...provider body... }
    }

Razorpay ``raw`` is the webhook body: ``raw.payload.<entity>.entity`` holds the
payment / refund / dispute / order object.

Merchant ``raw`` (contract the Phase 5 simulator is written to meet):

    {"order_id": "order_...", "status": "paid" | "pending" | ...,
     "amount": 50000, "currency": "INR", "occurred_at": 1725710400}
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from decision.models import (
    Event,
    MerchantOrderStatus,
    PaymentStatus,
    Source,
    SubEntityType,
)

_PAYMENT_STATUS = {s.value: s for s in PaymentStatus}
_MERCHANT_STATUS = {s.value: s for s in MerchantOrderStatus}

# raw.payload key -> the sub-entity type it represents. "payment" and "order"
# are the primary entity, not reversal sub-entities.
_SUB_ENTITY_KEYS = {
    "refund": SubEntityType.REFUND,
    "dispute": SubEntityType.DISPUTE,
}


def _dig(obj: Any, *path: str) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _coerce_dt(occurred_at: Any, received_at: Any) -> datetime:
    """Prefer the provider's own timestamp; fall back to ingestion receipt time;
    last resort is now(). Always tz-aware UTC."""
    if isinstance(occurred_at, (int, float)):
        return datetime.fromtimestamp(occurred_at, tz=timezone.utc)
    if isinstance(received_at, str) and received_at:
        try:
            dt = datetime.fromisoformat(received_at)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _payment_status(raw_status: Any) -> Optional[PaymentStatus]:
    return _PAYMENT_STATUS.get(raw_status) if isinstance(raw_status, str) else None


def _merchant_status(raw_status: Any) -> Optional[MerchantOrderStatus]:
    return _MERCHANT_STATUS.get(raw_status) if isinstance(raw_status, str) else None


def _int_or_none(val: Any) -> Optional[int]:
    return val if isinstance(val, int) and not isinstance(val, bool) else None


def _str_or_none(val: Any) -> Optional[str]:
    return val if isinstance(val, str) and val.strip() else None


def _razorpay_fields(raw: dict) -> dict:
    """Extract the payment/refund/dispute details from a Razorpay webhook body."""
    payload = raw.get("payload") if isinstance(raw, dict) else None
    payload = payload if isinstance(payload, dict) else {}

    fields: dict = {}

    payment = _dig(payload, "payment", "entity")
    if isinstance(payment, dict):
        fields["payment_status"] = _payment_status(payment.get("status"))
        fields["amount"] = _int_or_none(payment.get("amount"))
        fields["currency"] = _str_or_none(payment.get("currency"))

    for key, sub_type in _SUB_ENTITY_KEYS.items():
        entity = _dig(payload, key, "entity")
        if isinstance(entity, dict):
            fields["sub_entity_type"] = sub_type
            fields["sub_entity_id"] = _str_or_none(entity.get("id"))
            fields["sub_entity_status"] = _str_or_none(entity.get("status"))
            # A reversal's own amount/currency, when the payment block is absent.
            fields.setdefault("amount", _int_or_none(entity.get("amount")))
            fields.setdefault("currency", _str_or_none(entity.get("currency")))
            break

    order = _dig(payload, "order", "entity")
    if isinstance(order, dict):
        fields.setdefault("amount", _int_or_none(order.get("amount")))
        fields.setdefault("currency", _str_or_none(order.get("currency")))

    return fields


def _merchant_fields(raw: dict) -> dict:
    if not isinstance(raw, dict):
        return {}
    return {
        "merchant_status": _merchant_status(raw.get("status")),
        "amount": _int_or_none(raw.get("amount")),
        "currency": _str_or_none(raw.get("currency")),
    }


def envelope_to_event(envelope: Any) -> Optional[Event]:
    """Return a normalized ``Event``, or None if the envelope is unusable.

    Unusable means: not a dict, no ``event_id``, or no ``order_id``. The
    ingestion scope gate guarantees an ``order_id`` for everything it publishes,
    so a missing one here is a malformed message, not routine out-of-scope
    traffic.
    """
    if not isinstance(envelope, dict):
        return None

    event_id = _str_or_none(envelope.get("event_id"))
    order_id = _str_or_none(envelope.get("order_id"))
    if not event_id or not order_id:
        return None

    try:
        source = Source(envelope.get("source"))
    except ValueError:
        return None

    raw = envelope.get("raw")
    raw = raw if isinstance(raw, dict) else {}

    if source is Source.RAZORPAY:
        provider_fields = _razorpay_fields(raw)
    elif source is Source.MERCHANT:
        provider_fields = _merchant_fields(raw)
    else:  # Source.SYSTEM never arrives over Kafka
        return None

    return Event(
        event_id=event_id,
        source=source,
        event_type=_str_or_none(envelope.get("event_type")) or "",
        order_id=order_id,
        occurred_at=_coerce_dt(envelope.get("occurred_at"), envelope.get("received_at")),
        raw=raw,
        **provider_fields,
    )

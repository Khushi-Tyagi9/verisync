"""JSON <-> decision.OrderView, for persisting the per-order projection.

Pure. The worker stores the result in ``order_views.view`` (JSONB) after every
event and rebuilds the ``OrderView`` from it before evaluating the next one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from decision.models import MerchantOrderStatus, OrderView, PaymentStatus

_SCHEMA = 1


def _enum_value(member: Any) -> Optional[str]:
    return member.value if member is not None else None


def _dt_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


def serialize_view(view: OrderView) -> dict:
    return {
        "_schema": _SCHEMA,
        "order_id": view.order_id,
        "razorpay_status": _enum_value(view.razorpay_status),
        "razorpay_amount": view.razorpay_amount,
        "razorpay_currency": view.razorpay_currency,
        "razorpay_last_event_at": _dt_iso(view.razorpay_last_event_at),
        "merchant_status": _enum_value(view.merchant_status),
        "merchant_last_event_at": _dt_iso(view.merchant_last_event_at),
        "settled": view.settled,
        # frozensets are not JSON types; sorted lists keep the blob stable.
        "reversal_ids": sorted(view.reversal_ids),
        "seen_event_ids": sorted(view.seen_event_ids),
    }


def _payment_status(val: Any) -> Optional[PaymentStatus]:
    try:
        return PaymentStatus(val) if val is not None else None
    except ValueError:
        return None


def _merchant_status(val: Any) -> Optional[MerchantOrderStatus]:
    try:
        return MerchantOrderStatus(val) if val is not None else None
    except ValueError:
        return None


def _dt(val: Any) -> Optional[datetime]:
    if not isinstance(val, str) or not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except ValueError:
        return None


def deserialize_view(data: Any, *, order_id: str) -> Optional[OrderView]:
    """Rebuild an OrderView from a stored blob. Returns None if there is no
    usable blob, so the caller falls back to ``OrderView.empty``."""
    if not isinstance(data, dict):
        return None

    return OrderView(
        order_id=data.get("order_id") or order_id,
        razorpay_status=_payment_status(data.get("razorpay_status")),
        razorpay_amount=data.get("razorpay_amount"),
        razorpay_currency=data.get("razorpay_currency"),
        razorpay_last_event_at=_dt(data.get("razorpay_last_event_at")),
        merchant_status=_merchant_status(data.get("merchant_status")),
        merchant_last_event_at=_dt(data.get("merchant_last_event_at")),
        settled=bool(data.get("settled", False)),
        reversal_ids=frozenset(data.get("reversal_ids") or ()),
        seen_event_ids=frozenset(data.get("seen_event_ids") or ()),
    )

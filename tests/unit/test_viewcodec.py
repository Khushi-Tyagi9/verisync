"""OrderView <-> JSON round-trips. Pure, no infrastructure."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from decision.correlate import project
from decision.models import (
    Event,
    MerchantOrderStatus,
    OrderView,
    PaymentStatus,
    Source,
)
from worker.viewcodec import deserialize_view, serialize_view

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def _round_trip(view: OrderView) -> OrderView:
    blob = serialize_view(view)
    # Must survive an actual JSON encode/decode, not just dict copying.
    reloaded = json.loads(json.dumps(blob))
    out = deserialize_view(reloaded, order_id=view.order_id)
    assert out is not None
    return out


def test_empty_view_round_trips():
    original = OrderView.empty("order_ABC")
    out = _round_trip(original)
    assert out == original


def test_fully_populated_view_round_trips():
    original = OrderView(
        order_id="order_ABC",
        razorpay_status=PaymentStatus.CAPTURED,
        razorpay_amount=50000,
        razorpay_currency="INR",
        razorpay_last_event_at=NOW,
        merchant_status=MerchantOrderStatus.PENDING,
        merchant_last_event_at=NOW,
        settled=True,
        reversal_ids=frozenset({"rfnd_1", "rfnd_2"}),
        seen_event_ids=frozenset({"evt_1", "evt_2", "evt_3"}),
    )
    out = _round_trip(original)
    assert out == original


def test_round_trip_after_projecting_real_events():
    rp = Event(
        event_id="evt_1",
        source=Source.RAZORPAY,
        event_type="payment.captured",
        order_id="order_ABC",
        occurred_at=NOW,
        payment_status=PaymentStatus.CAPTURED,
        amount=50000,
        currency="INR",
    )
    merch = Event(
        event_id="evt_2",
        source=Source.MERCHANT,
        event_type="order.updated",
        order_id="order_ABC",
        occurred_at=NOW,
        merchant_status=MerchantOrderStatus.PENDING,
    )
    view = project(project(None, rp), merch)

    assert _round_trip(view) == view


def test_deserialize_tolerates_missing_and_unknown_fields():
    out = deserialize_view(
        {"order_id": "order_ABC", "razorpay_status": "not_a_real_status"},
        order_id="order_ABC",
    )
    assert out is not None
    assert out.order_id == "order_ABC"
    assert out.razorpay_status is None
    assert out.reversal_ids == frozenset()
    assert out.settled is False


def test_deserialize_none_returns_none():
    assert deserialize_view(None, order_id="order_ABC") is None
    assert deserialize_view("garbage", order_id="order_ABC") is None


def test_deserialize_uses_fallback_order_id_when_blob_lacks_one():
    out = deserialize_view({}, order_id="order_FALLBACK")
    assert out is not None
    assert out.order_id == "order_FALLBACK"

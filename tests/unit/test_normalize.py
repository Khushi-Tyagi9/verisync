"""Envelope -> normalized Event. Pure, no infrastructure."""

from __future__ import annotations

import time

from decision.models import (
    MerchantOrderStatus,
    PaymentStatus,
    Source,
    SubEntityType,
)
from worker.normalize import envelope_to_event


def _razorpay_envelope(**overrides):
    payment_entity = {
        "id": "pay_ABC123",
        "entity": "payment",
        "amount": 50000,
        "currency": "INR",
        "status": "captured",
        "order_id": "order_XYZ",
        "method": "upi",
    }
    env = {
        "schema": "verisync.lifecycle.v1",
        "event_id": "evt_0001",
        "source": "razorpay",
        "event_type": "payment.captured",
        "order_id": "order_XYZ",
        "occurred_at": 1_725_710_400,
        "received_at": "2026-09-07T12:00:00+00:00",
        "raw": {
            "entity": "event",
            "event": "payment.captured",
            "contains": ["payment"],
            "payload": {"payment": {"entity": payment_entity}},
            "created_at": 1_725_710_400,
        },
    }
    env.update(overrides)
    return env


def test_razorpay_payment_captured_maps_to_event():
    ev = envelope_to_event(_razorpay_envelope())

    assert ev is not None
    assert ev.event_id == "evt_0001"
    assert ev.source is Source.RAZORPAY
    assert ev.event_type == "payment.captured"
    assert ev.order_id == "order_XYZ"
    assert ev.payment_status is PaymentStatus.CAPTURED
    assert ev.amount == 50000
    assert ev.currency == "INR"
    assert ev.merchant_status is None
    assert ev.occurred_at.tzinfo is not None


def test_unknown_payment_status_becomes_none_not_an_error():
    env = _razorpay_envelope()
    env["raw"]["payload"]["payment"]["entity"]["status"] = "some_new_status"

    ev = envelope_to_event(env)

    assert ev is not None
    assert ev.payment_status is None


def test_refund_event_populates_sub_entity_and_is_reversal():
    env = _razorpay_envelope(
        event_id="evt_rfnd",
        event_type="refund.created",
    )
    env["raw"]["payload"] = {
        "refund": {
            "entity": {
                "id": "rfnd_777",
                "entity": "refund",
                "amount": 20000,
                "currency": "INR",
                "status": "processed",
            }
        }
    }

    ev = envelope_to_event(env)

    assert ev is not None
    assert ev.sub_entity_type is SubEntityType.REFUND
    assert ev.sub_entity_id == "rfnd_777"
    assert ev.sub_entity_status == "processed"
    assert ev.amount == 20000
    assert ev.is_reversal is True


def test_merchant_envelope_maps_status_and_amount():
    env = {
        "event_id": "evt_merch_1",
        "source": "merchant",
        "event_type": "order.updated",
        "order_id": "order_XYZ",
        "occurred_at": None,
        "received_at": "2026-09-07T12:05:00+00:00",
        "raw": {
            "order_id": "order_XYZ",
            "status": "paid",
            "amount": 50000,
            "currency": "INR",
        },
    }

    ev = envelope_to_event(env)

    assert ev is not None
    assert ev.source is Source.MERCHANT
    assert ev.merchant_status is MerchantOrderStatus.PAID
    assert ev.payment_status is None
    assert ev.amount == 50000


def test_occurred_at_falls_back_to_received_at_when_timestamp_missing():
    env = _razorpay_envelope(occurred_at=None)

    ev = envelope_to_event(env)

    assert ev is not None
    assert ev.occurred_at.isoformat() == "2026-09-07T12:00:00+00:00"


def test_missing_event_id_or_order_id_is_unusable():
    assert envelope_to_event(_razorpay_envelope(event_id=None)) is None
    assert envelope_to_event(_razorpay_envelope(event_id="  ")) is None
    assert envelope_to_event(_razorpay_envelope(order_id=None)) is None


def test_unknown_source_is_unusable():
    assert envelope_to_event(_razorpay_envelope(source="system")) is None
    assert envelope_to_event(_razorpay_envelope(source="paypal")) is None
    assert envelope_to_event("not a dict") is None


def test_raw_body_is_carried_through_for_the_audit_trail():
    env = _razorpay_envelope()
    ev = envelope_to_event(env)

    assert ev is not None
    assert ev.raw == env["raw"]


def test_epoch_timestamp_is_interpreted_as_utc_seconds():
    ts = int(time.time())
    ev = envelope_to_event(_razorpay_envelope(occurred_at=ts))

    assert ev is not None
    assert abs(ev.occurred_at.timestamp() - ts) < 1.0

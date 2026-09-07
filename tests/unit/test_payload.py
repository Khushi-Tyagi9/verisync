from __future__ import annotations

from ingestion.payload import (
    ENVELOPE_SCHEMA,
    build_envelope,
    extract_event_type,
    extract_occurred_at,
    extract_order_id,
)


def _payment_event(order_id):
    return {
        "entity": "event",
        "event": "payment.captured",
        "contains": ["payment"],
        "payload": {"payment": {"entity": {"id": "pay_1", "order_id": order_id, "amount": 50000}}},
        "created_at": 1_757_246_400,
    }


def test_extract_order_id_from_payment_entity():
    assert extract_order_id(_payment_event("order_ABC")) == "order_ABC"


def test_extract_order_id_from_order_entity():
    body = {"event": "order.paid", "payload": {"order": {"entity": {"id": "order_XYZ"}}}}
    assert extract_order_id(body) == "order_XYZ"


def test_extract_order_id_from_refund_entity():
    body = {
        "event": "refund.created",
        "payload": {"refund": {"entity": {"id": "rfnd_1", "order_id": "order_RF"}}},
    }
    assert extract_order_id(body) == "order_RF"


def test_extract_order_id_is_none_when_null_payment_link_case():
    assert extract_order_id(_payment_event(None)) is None


def test_extract_order_id_is_none_when_blank():
    assert extract_order_id(_payment_event("   ")) is None


def test_extract_order_id_is_none_when_payload_missing_or_wrong_shape():
    assert extract_order_id({}) is None
    assert extract_order_id({"payload": {"payment": {}}}) is None
    assert extract_order_id("not a dict") is None


def test_extract_event_type_and_occurred_at():
    body = _payment_event("order_ABC")
    assert extract_event_type(body) == "payment.captured"
    assert extract_occurred_at(body) == 1_757_246_400
    assert extract_event_type({}) is None
    assert extract_occurred_at({"created_at": "nope"}) is None


def test_build_envelope_shape():
    body = _payment_event("order_ABC")
    env = build_envelope(
        event_id="evt_99",
        event_type="payment.captured",
        order_id="order_ABC",
        raw=body,
        received_at="2026-09-07T12:00:00+00:00",
    )
    assert env["schema"] == ENVELOPE_SCHEMA
    assert env["source"] == "razorpay"
    assert env["event_id"] == "evt_99"
    assert env["order_id"] == "order_ABC"
    assert env["occurred_at"] == 1_757_246_400
    assert env["received_at"] == "2026-09-07T12:00:00+00:00"
    assert env["raw"] is body  # full body carried through for the worker

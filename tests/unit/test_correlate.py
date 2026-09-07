from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from decision.correlate import correlate, project
from decision.models import (
    CorrelationFailure,
    CorrelationStatus,
    MerchantOrderStatus,
    OrderView,
    PaymentStatus,
    Source,
    SubEntityType,
)


def test_project_folds_razorpay_capture_into_settled_view(razorpay_captured):
    view = project(None, razorpay_captured)

    assert view.order_id == "order_ABC123"
    assert view.razorpay_status is PaymentStatus.CAPTURED
    assert view.settled is True
    assert view.razorpay_amount == 50000
    assert view.razorpay_currency == "INR"
    assert razorpay_captured.event_id in view.seen_event_ids


def test_project_folds_merchant_event_without_touching_razorpay_side(merchant_pending):
    view = project(None, merchant_pending)

    assert view.merchant_status is MerchantOrderStatus.PENDING
    assert view.razorpay_status is None
    assert view.settled is False


def test_project_accumulates_seen_event_ids_across_events(razorpay_captured, merchant_paid):
    view = project(None, razorpay_captured)
    view = project(view, merchant_paid)

    assert view.seen_event_ids == {razorpay_captured.event_id, merchant_paid.event_id}
    assert view.razorpay_status is PaymentStatus.CAPTURED
    assert view.merchant_status is MerchantOrderStatus.PAID


def test_project_returns_new_view_and_never_mutates_input(razorpay_captured):
    original = OrderView.empty("order_ABC123")
    returned = project(original, razorpay_captured)

    assert returned is not original
    assert original.razorpay_status is None
    with pytest.raises(FrozenInstanceError):
        original.settled = True  # type: ignore[misc]


def test_project_records_reversal_sub_entity_id(make_event):
    ev = make_event(
        source=Source.RAZORPAY,
        event_type="refund.created",
        sub_entity_type=SubEntityType.REFUND,
        sub_entity_id="rfnd_001",
        amount=10000,
    )
    view = project(None, ev)

    assert "rfnd_001" in view.reversal_ids
    # a reversal amount must not be mistaken for the order amount
    assert view.razorpay_amount is None


def test_correlate_missing_order_id_fails(make_event):
    ev = make_event(order_id="   ")
    result = correlate(OrderView.empty(""), ev)

    assert result.status is CorrelationStatus.FAILED
    assert result.failure is CorrelationFailure.MISSING_ORDER_ID


def test_correlate_amount_mismatch_on_non_reversal_fails(make_event):
    view = OrderView(order_id="order_ABC123", razorpay_amount=50000, razorpay_currency="INR")
    ev = make_event(amount=49999)

    result = correlate(view, ev)

    assert result.status is CorrelationStatus.FAILED
    assert result.failure is CorrelationFailure.AMOUNT_MISMATCH


def test_correlate_amount_difference_on_reversal_is_allowed(make_event):
    view = OrderView(order_id="order_ABC123", razorpay_amount=50000, razorpay_currency="INR")
    ev = make_event(
        event_type="refund.created",
        sub_entity_type=SubEntityType.REFUND,
        sub_entity_id="rfnd_001",
        amount=10000,
    )

    result = correlate(view, ev)

    assert result.status is CorrelationStatus.MATCHED


def test_correlate_currency_mismatch_fails(make_event):
    view = OrderView(order_id="order_ABC123", razorpay_currency="INR")
    ev = make_event(currency="USD")

    result = correlate(view, ev)

    assert result.status is CorrelationStatus.FAILED
    assert result.failure is CorrelationFailure.CURRENCY_MISMATCH


def test_correlate_require_merchant_record_fails_when_absent(razorpay_captured):
    view = OrderView.empty("order_ABC123")

    result = correlate(view, razorpay_captured, require_merchant_record=True)

    assert result.status is CorrelationStatus.FAILED
    assert result.failure is CorrelationFailure.NO_MERCHANT_RECORD


def test_correlate_require_merchant_record_passes_when_present(razorpay_captured):
    view = OrderView(order_id="order_ABC123", merchant_status=MerchantOrderStatus.PENDING)

    result = correlate(view, razorpay_captured, require_merchant_record=True)

    assert result.status is CorrelationStatus.MATCHED


def test_correlate_clean_event_matches(razorpay_captured):
    result = correlate(OrderView.empty("order_ABC123"), razorpay_captured)

    assert result.ok is True
    assert result.failure is None
    assert razorpay_captured.event_id in result.view.seen_event_ids

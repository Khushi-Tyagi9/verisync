"""Correlation: fold events into a per-order view, and validate that an
incoming event is consistent with what we already know.

Pure. No I/O, no clients.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional

from .models import (
    CorrelationFailure,
    CorrelationResult,
    CorrelationStatus,
    Event,
    OrderView,
    PaymentStatus,
    Source,
    SubEntityType,
)

_CAPTURE_EVENTS = ("order.paid", "payment.captured")
_SETTLING_STATUSES = (PaymentStatus.CAPTURED, PaymentStatus.REFUNDED)


def project(view: Optional[OrderView], event: Event) -> OrderView:
    """Fold one event into the running per-order view and return a new view.

    Lenient by design: projection records what the event says, it does not
    judge whether the event is trustworthy. That judgement is `correlate()`.
    """
    if view is None:
        view = OrderView.empty(event.order_id)

    changes: dict = {"seen_event_ids": view.seen_event_ids | {event.event_id}}

    if event.source is Source.RAZORPAY:
        if event.payment_status is not None:
            changes["razorpay_status"] = event.payment_status
            if event.payment_status in _SETTLING_STATUSES:
                changes["settled"] = True
        if event.event_type in _CAPTURE_EVENTS:
            changes["razorpay_status"] = PaymentStatus.CAPTURED
            changes["settled"] = True
        if event.amount is not None and view.razorpay_amount is None and not event.is_reversal:
            changes["razorpay_amount"] = event.amount
        if event.currency is not None and view.razorpay_currency is None:
            changes["razorpay_currency"] = event.currency
        changes["razorpay_last_event_at"] = event.occurred_at

    elif event.source is Source.MERCHANT:
        if event.merchant_status is not None:
            changes["merchant_status"] = event.merchant_status
        changes["merchant_last_event_at"] = event.occurred_at

    if (
        event.sub_entity_type in (SubEntityType.REFUND, SubEntityType.DISPUTE)
        and event.sub_entity_id
    ):
        changes["reversal_ids"] = view.reversal_ids | {event.sub_entity_id}

    return replace(view, **changes)


def correlate(
    view: OrderView,
    event: Event,
    *,
    require_merchant_record: bool = False,
) -> CorrelationResult:
    """Validate `event` against `view`. Returns the projected view either way;
    the caller decides whether to trust it based on `.status`.

    `require_merchant_record` is set by the worker when a recheck fires: at that
    point a missing merchant-side record is a genuine anomaly (the merchant
    "never updated" case), not just lag.
    """
    projected = project(view, event)

    if not event.order_id or not event.order_id.strip():
        return CorrelationResult(
            CorrelationStatus.FAILED,
            projected,
            CorrelationFailure.MISSING_ORDER_ID,
            "event carries no order_id",
        )

    if (
        event.currency
        and view.razorpay_currency
        and event.currency.upper() != view.razorpay_currency.upper()
    ):
        return CorrelationResult(
            CorrelationStatus.FAILED,
            projected,
            CorrelationFailure.CURRENCY_MISMATCH,
            f"event currency {event.currency} != known {view.razorpay_currency}",
        )

    # A refund/dispute is legitimately a different (usually smaller) amount, so
    # only non-reversal events are held to the exact-amount check.
    if (
        event.amount is not None
        and view.razorpay_amount is not None
        and event.amount != view.razorpay_amount
        and not event.is_reversal
    ):
        return CorrelationResult(
            CorrelationStatus.FAILED,
            projected,
            CorrelationFailure.AMOUNT_MISMATCH,
            f"event amount {event.amount} != known {view.razorpay_amount}",
        )

    if require_merchant_record and projected.merchant_status is None:
        return CorrelationResult(
            CorrelationStatus.FAILED,
            projected,
            CorrelationFailure.NO_MERCHANT_RECORD,
            "no merchant-side record for this order at recheck time",
        )

    return CorrelationResult(CorrelationStatus.MATCHED, projected)

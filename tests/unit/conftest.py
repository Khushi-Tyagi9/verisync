"""Shared factories for the decision-logic unit tests.

No infrastructure. If anything in tests/unit/ needs Docker, it is in the wrong
folder.
"""

from __future__ import annotations

from datetime import datetime, timezone
from itertools import count

import pytest

from decision.models import (
    Event,
    MerchantOrderStatus,
    PaymentStatus,
    Source,
    SubEntityType,
)

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def now() -> datetime:
    return NOW


@pytest.fixture
def make_event():
    """Factory for normalized events with sane defaults and a unique event_id."""
    ids = count(1)

    def _make(
        *,
        source: Source = Source.RAZORPAY,
        event_type: str = "payment.captured",
        order_id: str = "order_ABC123",
        occurred_at: datetime = NOW,
        payment_status: PaymentStatus | None = None,
        merchant_status: MerchantOrderStatus | None = None,
        amount: int | None = 50000,
        currency: str | None = "INR",
        sub_entity_type: SubEntityType | None = None,
        sub_entity_id: str | None = None,
        sub_entity_status: str | None = None,
        event_id: str | None = None,
    ) -> Event:
        return Event(
            event_id=event_id or f"evt_{next(ids):04d}",
            source=source,
            event_type=event_type,
            order_id=order_id,
            occurred_at=occurred_at,
            payment_status=payment_status,
            merchant_status=merchant_status,
            amount=amount,
            currency=currency,
            sub_entity_type=sub_entity_type,
            sub_entity_id=sub_entity_id,
            sub_entity_status=sub_entity_status,
        )

    return _make


@pytest.fixture
def razorpay_captured(make_event):
    return make_event(
        source=Source.RAZORPAY,
        event_type="payment.captured",
        payment_status=PaymentStatus.CAPTURED,
    )


@pytest.fixture
def razorpay_failed(make_event):
    return make_event(
        source=Source.RAZORPAY,
        event_type="payment.failed",
        payment_status=PaymentStatus.FAILED,
    )


@pytest.fixture
def merchant_paid(make_event):
    return make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID,
        payment_status=None,
    )


@pytest.fixture
def merchant_pending(make_event):
    return make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PENDING,
        payment_status=None,
    )

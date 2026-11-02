from __future__ import annotations

from decision.compare import assess
from decision.models import DriftKind, MerchantOrderStatus, OrderView, PaymentStatus


def _view(**kw) -> OrderView:
    return OrderView(order_id="order_ABC123", **kw)


def test_captured_vs_pending_is_safe_drift():
    d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=MerchantOrderStatus.PENDING))
    assert d.kind is DriftKind.SAFE


def test_captured_vs_silent_merchant_is_safe_drift():
    d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=None))
    assert d.kind is DriftKind.SAFE


def test_captured_vs_paid_is_no_drift():
    d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=MerchantOrderStatus.PAID))
    assert d.kind is DriftKind.NONE


def test_failed_vs_paid_is_unsafe_drift():
    d = assess(_view(razorpay_status=PaymentStatus.FAILED, merchant_status=MerchantOrderStatus.PAID))
    assert d.kind is DriftKind.UNSAFE


def test_failed_vs_pending_is_no_drift():
    d = assess(_view(razorpay_status=PaymentStatus.FAILED, merchant_status=MerchantOrderStatus.PENDING))
    assert d.kind is DriftKind.NONE


def test_reversal_id_on_settled_order_is_reversal():
    d = assess(
        _view(
            razorpay_status=PaymentStatus.CAPTURED,
            merchant_status=MerchantOrderStatus.PAID,
            settled=True,
            reversal_ids=frozenset({"rfnd_001"}),
        )
    )
    assert d.kind is DriftKind.REVERSAL


def test_reversal_takes_precedence_over_a_would_be_safe_comparison():
    d = assess(
        _view(
            razorpay_status=PaymentStatus.CAPTURED,
            merchant_status=MerchantOrderStatus.PENDING,
            settled=True,
            reversal_ids=frozenset({"disp_001"}),
        )
    )
    assert d.kind is DriftKind.REVERSAL


def test_refunded_on_settled_order_is_reversal():
    d = assess(_view(razorpay_status=PaymentStatus.REFUNDED, settled=True))
    assert d.kind is DriftKind.REVERSAL


def test_authorized_only_is_indeterminate():
    d = assess(_view(razorpay_status=PaymentStatus.AUTHORIZED, merchant_status=MerchantOrderStatus.PENDING))
    assert d.kind is DriftKind.INDETERMINATE


def test_nothing_known_is_indeterminate():
    d = assess(_view())
    assert d.kind is DriftKind.INDETERMINATE


def test_assessment_carries_both_observed_statuses():
    d = assess(_view(razorpay_status=PaymentStatus.FAILED, merchant_status=MerchantOrderStatus.PAID))
    assert d.razorpay_status is PaymentStatus.FAILED
    assert d.merchant_status is MerchantOrderStatus.PAID


def test_captured_vs_cancelled_is_unsafe_not_safe():
    d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=MerchantOrderStatus.CANCELLED))
    assert d.kind is DriftKind.UNSAFE
    assert d.kind is not DriftKind.SAFE


def test_captured_vs_refunded_is_a_reversal_not_safe():
    d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=MerchantOrderStatus.REFUNDED))
    assert d.kind is DriftKind.REVERSAL
    assert d.kind is not DriftKind.SAFE


def test_captured_vs_failed_and_pending_are_still_safe_drift():
    for status in (MerchantOrderStatus.FAILED, MerchantOrderStatus.PENDING):
        d = assess(_view(razorpay_status=PaymentStatus.CAPTURED, merchant_status=status))
        assert d.kind is DriftKind.SAFE, status


def test_refund_without_a_seen_capture_against_merchant_paid_is_a_reversal():
    # order_1004 in the uploaded sample: the gateway export holds only the
    # refund row, the merchant shows paid.
    d = assess(_view(merchant_status=MerchantOrderStatus.PAID, reversal_ids=frozenset({"rfnd_1004"})))
    assert d.kind is DriftKind.REVERSAL


def test_refund_without_a_seen_capture_and_merchant_not_paid_is_not_a_reversal():
    for status in (None, MerchantOrderStatus.PENDING, MerchantOrderStatus.CANCELLED):
        d = assess(_view(merchant_status=status, reversal_ids=frozenset({"rfnd_1"})))
        assert d.kind is not DriftKind.REVERSAL, status

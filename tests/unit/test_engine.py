"""End-to-end (still infra-free) walks through correlate -> compare -> decide."""

from __future__ import annotations

from datetime import timedelta

from decision.engine import evaluate
from decision.models import (
    Action,
    DEFAULT_RECHECK_DELAY,
    DriftKind,
    MerchantOrderStatus,
    PaymentStatus,
    ReconStatus,
    Source,
    SubEntityType,
)


def test_merchant_lag_then_catch_up_full_lifecycle(make_event, now):
    # 1. Razorpay confirms capture; merchant side silent -> safe drift, recheck armed.
    rp = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    step1 = evaluate(event=rp, view=None, current_status=None, now=now)

    assert step1.decision.action is Action.ARM_RECHECK
    assert step1.decision.new_status is ReconStatus.PENDING_RECHECK
    assert step1.decision.recheck_at == now + DEFAULT_RECHECK_DELAY

    # 2. Recheck fires 10 min later, merchant STILL hasn't caught up -> autocorrect.
    later = now + DEFAULT_RECHECK_DELAY
    step2 = evaluate(
        event=rp,
        view=step1.view,
        current_status=ReconStatus.PROCESSING,
        now=later,
        require_merchant_record=True,
    )
    # merchant record genuinely absent at recheck -> treated as anomaly
    assert step2.decision.action is Action.FLAG_FOR_REVIEW
    assert step2.decision.review.reason == "no_merchant_record"

    # 3. Alternative: merchant DID send a (late) pending update before the recheck.
    merch = make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PENDING,
        payment_status=None,
    )
    view = evaluate(event=merch, view=step1.view, current_status=ReconStatus.PENDING_RECHECK, now=later).view
    step3 = evaluate(
        event=rp,
        view=view,
        current_status=ReconStatus.PROCESSING,
        now=later,
        require_merchant_record=True,
    )
    assert step3.decision.action is Action.APPLY_AUTOCORRECT
    assert step3.decision.new_status is ReconStatus.RESOLVED_AUTOCORRECTED


def test_merchant_caught_up_before_recheck_resolves_naturally(make_event, now):
    rp = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    s1 = evaluate(event=rp, view=None, current_status=None, now=now)

    merch_paid = make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID,
        payment_status=None,
    )
    s2 = evaluate(
        event=merch_paid,
        view=s1.view,
        current_status=ReconStatus.PENDING_RECHECK,
        now=now + timedelta(minutes=3),
    )

    assert s2.decision.action is Action.MARK_RESOLVED_NATURALLY
    assert s2.decision.new_status is ReconStatus.RESOLVED_NATURALLY
    assert s2.decision.clear_recheck_at is True


def test_razorpay_failed_but_merchant_paid_is_flagged_never_corrected(make_event, now):
    merch_paid = make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID,
        payment_status=None,
    )
    view = evaluate(event=merch_paid, view=None, current_status=None, now=now).view

    rp_failed = make_event(event_type="payment.failed", payment_status=PaymentStatus.FAILED)
    out = evaluate(event=rp_failed, view=view, current_status=None, now=now)

    assert out.decision.action is Action.FLAG_FOR_REVIEW
    assert out.decision.audit.direction == "UNSAFE"
    assert out.decision.new_status is None


def test_refund_after_settlement_routes_to_disputed(make_event, now):
    rp = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    merch = make_event(
        source=Source.MERCHANT,
        event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID,
        payment_status=None,
    )
    view = evaluate(event=rp, view=None, current_status=None, now=now).view
    view = evaluate(event=merch, view=view, current_status=None, now=now).view

    refund = make_event(
        event_type="refund.created",
        sub_entity_type=SubEntityType.REFUND,
        sub_entity_id="rfnd_777",
        amount=50000,
    )
    out = evaluate(event=refund, view=view, current_status=ReconStatus.RESOLVED_NATURALLY, now=now)

    assert out.decision.action is Action.ROUTE_DISPUTED
    assert out.decision.new_status is ReconStatus.DISPUTED


def test_event_without_order_id_routes_through_review_queue_path(make_event, now):
    # Defense-in-depth: the ingestion gate should have caught this. If it
    # didn't, the decision layer treats it as an anomaly, not routine traffic.
    out = evaluate(event=make_event(order_id="   "), view=None, current_status=None, now=now)

    assert out.decision.action is Action.FLAG_FOR_REVIEW
    assert out.decision.review is not None
    assert out.decision.review.reason == "missing_order_id"
    assert out.decision.new_status is None


def test_projected_view_is_returned_for_persistence(make_event, now):
    rp = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    out = evaluate(event=rp, view=None, current_status=None, now=now)

    assert out.view.razorpay_status is PaymentStatus.CAPTURED
    assert rp.event_id in out.view.seen_event_ids

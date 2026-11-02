"""An event that fails correlation must not leave any trace in the stored view
except its id (so replays still dedupe), and so can never drive a later
auto-correction."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from decision.engine import evaluate
from decision.models import (
    Action,
    MerchantOrderStatus,
    OrderView,
    PaymentStatus,
    ReconStatus,
    Source,
)

DELAY = timedelta(seconds=20)


def _walk(events, now, *, status=None, view=None):
    """Fold events through evaluate() the way the worker does: persist the
    returned view and any status change. Returns (decisions, views, status)."""
    decisions, views = [], []
    for ev in events:
        res = evaluate(event=ev, view=view, current_status=status, now=now, recheck_delay=DELAY)
        decisions.append(res.decision)
        views.append(res.view)
        view = res.view
        if res.decision.new_status is not None:
            status = res.decision.new_status
    return decisions, views, status


def test_rejected_capture_cannot_later_arm_an_autocorrect(make_event, now):
    failed = make_event(event_type="payment.failed", payment_status=PaymentStatus.FAILED, amount=50_000)
    captured = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED, amount=70_000)
    merchant = make_event(
        source=Source.MERCHANT, event_type="order.updated",
        merchant_status=MerchantOrderStatus.PENDING, amount=50_000,
    )

    decisions, views, status = _walk([failed, captured, merchant], now)

    assert decisions[1].action is Action.FLAG_FOR_REVIEW
    assert decisions[1].review.reason == "amount_mismatch"
    assert decisions[2].action is not Action.ARM_RECHECK
    assert decisions[2].recheck_at is None
    assert status is None                                  # no pointer, no recheck armed
    assert all(d.action is not Action.APPLY_AUTOCORRECT for d in decisions)
    assert views[2].razorpay_status is PaymentStatus.FAILED
    assert views[2].settled is False


def test_rejected_event_leaves_the_prior_view_plus_its_id_only(make_event, now):
    first = make_event(event_type="payment.failed", payment_status=PaymentStatus.FAILED, amount=50_000)
    bad = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED, amount=70_000)

    _, views, _ = _walk([first, bad], now)

    expected = dataclasses.replace(
        views[0], seen_event_ids=views[0].seen_event_ids | {bad.event_id}
    )
    assert views[1] == expected


def test_rejected_event_id_is_still_recorded_for_dedupe(make_event, now):
    first = make_event(event_type="payment.authorized", payment_status=PaymentStatus.AUTHORIZED, amount=50_000)
    bad = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED, amount=1)

    _, views, _ = _walk([first, bad], now)

    assert bad.event_id in views[1].seen_event_ids
    assert first.event_id in views[1].seen_event_ids


def test_rejected_event_on_an_empty_view_stores_only_its_id(make_event, now):
    # require_merchant_record on a view with no merchant record fails correlation.
    ev = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    res = evaluate(event=ev, view=None, current_status=None, now=now, recheck_delay=DELAY,
                   require_merchant_record=True)
    assert res.decision.review.reason == "no_merchant_record"
    assert res.view == dataclasses.replace(OrderView.empty(ev.order_id), seen_event_ids=frozenset({ev.event_id}))


def test_currency_mismatch_is_not_stored_and_cannot_arm_a_recheck(make_event, now):
    authorized = make_event(event_type="payment.authorized", payment_status=PaymentStatus.AUTHORIZED,
                            amount=50_000, currency="INR")
    wrong_currency = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED,
                                amount=50_000, currency="USD")
    merchant = make_event(
        source=Source.MERCHANT, event_type="order.updated",
        merchant_status=MerchantOrderStatus.PENDING, amount=50_000, currency="INR",
    )

    decisions, views, status = _walk([authorized, wrong_currency, merchant], now)

    assert decisions[1].action is Action.FLAG_FOR_REVIEW
    assert decisions[1].review.reason == "currency_mismatch"
    assert views[1].razorpay_status is PaymentStatus.AUTHORIZED
    assert views[1].razorpay_currency == "INR"
    assert views[1].settled is False
    assert decisions[2].action is not Action.ARM_RECHECK
    assert status is None


def test_merchant_event_with_a_mismatching_amount_does_not_set_merchant_status(make_event, now):
    captured = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED, amount=50_000)
    bad_merchant = make_event(
        source=Source.MERCHANT, event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID, amount=40_000,
    )

    decisions, views, status = _walk([captured, bad_merchant], now)

    assert decisions[1].action is Action.FLAG_FOR_REVIEW
    assert decisions[1].review.reason == "amount_mismatch"
    assert views[1].merchant_status is None
    assert views[1].merchant_last_event_at is None
    assert status is ReconStatus.PENDING_RECHECK          # still waiting on the real merchant record

    # At recheck the merchant record is still absent, so it is an anomaly, not
    # something to correct from the rejected event.
    later = now + DELAY
    recheck = make_event(source=Source.SYSTEM, event_type="recheck.fired")
    res = evaluate(event=recheck, view=views[1], current_status=ReconStatus.PROCESSING,
                   now=later, recheck_delay=DELAY, require_merchant_record=True)
    assert res.decision.action is Action.FLAG_FOR_REVIEW
    assert res.decision.review.reason == "no_merchant_record"


def test_merchant_event_with_a_mismatching_currency_is_not_stored(make_event, now):
    captured = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED, currency="INR")
    bad_merchant = make_event(
        source=Source.MERCHANT, event_type="order.updated",
        merchant_status=MerchantOrderStatus.PAID, currency="USD",
    )
    decisions, views, _ = _walk([captured, bad_merchant], now)
    assert decisions[1].review.reason == "currency_mismatch"
    assert views[1].merchant_status is None


def test_no_merchant_record_at_recheck_still_flags_and_leaves_the_view_alone(make_event, now):
    captured = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    decisions, views, status = _walk([captured], now)
    assert decisions[0].action is Action.ARM_RECHECK

    recheck = make_event(source=Source.SYSTEM, event_type="recheck.fired")
    res = evaluate(event=recheck, view=views[0], current_status=ReconStatus.PROCESSING,
                   now=now + DELAY, recheck_delay=DELAY, require_merchant_record=True)

    assert res.decision.action is Action.FLAG_FOR_REVIEW
    assert res.decision.review.reason == "no_merchant_record"
    assert res.view == dataclasses.replace(
        views[0], seen_event_ids=views[0].seen_event_ids | {recheck.event_id}
    )


def test_accepted_events_still_project_normally(make_event, now):
    captured = make_event(event_type="payment.captured", payment_status=PaymentStatus.CAPTURED)
    _, views, _ = _walk([captured], now)
    assert views[0].razorpay_status is PaymentStatus.CAPTURED
    assert views[0].settled is True
    assert views[0].razorpay_amount == 50_000

from __future__ import annotations

from datetime import timedelta

import pytest

from decision.actions import decide
from decision.models import (
    Action,
    CorrelationFailure,
    CorrelationResult,
    CorrelationStatus,
    DEFAULT_RECHECK_DELAY,
    DriftAssessment,
    DriftKind,
    OrderView,
    ReconStatus,
    Source,
    SubEntityType,
)


def _matched(order_id: str = "order_ABC123") -> CorrelationResult:
    return CorrelationResult(CorrelationStatus.MATCHED, OrderView.empty(order_id))


def _failed(reason: CorrelationFailure) -> CorrelationResult:
    return CorrelationResult(
        CorrelationStatus.FAILED, OrderView.empty("order_ABC123"), reason, "boom"
    )


def _drift(kind: DriftKind) -> DriftAssessment:
    return DriftAssessment(kind=kind, detail=kind.value)


# --------------------------------------------------------------------------- #
# Fresh detection
# --------------------------------------------------------------------------- #
def test_safe_drift_arms_a_recheck_window(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.SAFE),
        current_status=None,
        now=now,
    )

    assert d.action is Action.ARM_RECHECK
    assert d.new_status is ReconStatus.PENDING_RECHECK
    assert d.recheck_at == now + DEFAULT_RECHECK_DELAY
    assert d.audit.direction == "SAFE"
    assert d.review is None


def test_safe_drift_respects_a_custom_recheck_delay(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.SAFE),
        current_status=None,
        now=now,
        recheck_delay=timedelta(minutes=2),
    )
    assert d.recheck_at == now + timedelta(minutes=2)


def test_unsafe_drift_flags_for_review_and_never_auto_corrects(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.UNSAFE),
        current_status=None,
        now=now,
    )

    assert d.action is Action.FLAG_FOR_REVIEW
    assert d.new_status is None
    assert d.recheck_at is None
    assert d.review is not None
    assert d.review.reason == "unsafe_drift"
    assert d.audit.direction == "UNSAFE"


def test_reversal_routes_to_disputed_and_clears_recheck(make_event, now):
    d = decide(
        event=make_event(event_type="payment.dispute.created"),
        correlation=_matched(),
        drift=_drift(DriftKind.REVERSAL),
        current_status=None,
        now=now,
    )

    assert d.action is Action.ROUTE_DISPUTED
    assert d.new_status is ReconStatus.DISPUTED
    assert d.clear_recheck_at is True


def test_no_drift_on_a_fresh_order_is_a_noop(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.NONE),
        current_status=None,
        now=now,
    )
    assert d.action is Action.NO_OP
    assert d.new_status is None


def test_no_drift_while_pending_recheck_resolves_naturally(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.NONE),
        current_status=ReconStatus.PENDING_RECHECK,
        now=now,
    )
    assert d.action is Action.MARK_RESOLVED_NATURALLY
    assert d.new_status is ReconStatus.RESOLVED_NATURALLY
    assert d.clear_recheck_at is True


def test_correlation_failure_flags_for_review_even_if_drift_looks_safe(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_failed(CorrelationFailure.AMOUNT_MISMATCH),
        drift=_drift(DriftKind.SAFE),
        current_status=None,
        now=now,
    )

    assert d.action is Action.FLAG_FOR_REVIEW
    assert d.review.reason == "amount_mismatch"
    assert d.new_status is None


def test_missing_order_id_reaching_decision_layer_routes_to_review(make_event, now):
    # The Phase 3 ingestion gate handles the routine "no order_id" case quietly
    # (out_of_scope_events, pre-Kafka). If an order_id-less event still reaches
    # the decision layer, that is a pipeline anomaly and must go through the
    # same review_queue path as any other correlation failure -- never
    # out_of_scope_events, and never an auto-correction.
    d = decide(
        event=make_event(order_id=""),
        correlation=_failed(CorrelationFailure.MISSING_ORDER_ID),
        drift=_drift(DriftKind.SAFE),
        current_status=None,
        now=now,
    )

    assert d.action is Action.FLAG_FOR_REVIEW
    assert d.review is not None
    assert d.review.reason == "missing_order_id"
    assert d.new_status is None
    assert d.recheck_at is None


def test_no_merchant_record_failure_flags_for_review(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_failed(CorrelationFailure.NO_MERCHANT_RECORD),
        drift=_drift(DriftKind.SAFE),
        current_status=ReconStatus.PROCESSING,
        now=now,
    )
    assert d.action is Action.FLAG_FOR_REVIEW
    assert d.review.reason == "no_merchant_record"


# --------------------------------------------------------------------------- #
# Resolving a claimed recheck (row is PROCESSING)
# --------------------------------------------------------------------------- #
def test_processing_with_safe_drift_applies_the_autocorrection(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.SAFE),
        current_status=ReconStatus.PROCESSING,
        now=now,
    )
    assert d.action is Action.APPLY_AUTOCORRECT
    assert d.new_status is ReconStatus.RESOLVED_AUTOCORRECTED
    assert d.clear_recheck_at is True


def test_processing_with_no_drift_marks_resolved_naturally(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.NONE),
        current_status=ReconStatus.PROCESSING,
        now=now,
    )
    assert d.action is Action.MARK_RESOLVED_NATURALLY
    assert d.new_status is ReconStatus.RESOLVED_NATURALLY


def test_processing_with_ambiguous_drift_rearms_instead_of_guessing(make_event, now):
    d = decide(
        event=make_event(),
        correlation=_matched(),
        drift=_drift(DriftKind.UNSAFE),
        current_status=ReconStatus.PROCESSING,
        now=now,
    )
    assert d.action is Action.ARM_RECHECK
    assert d.new_status is ReconStatus.PENDING_RECHECK
    assert d.recheck_at == now + DEFAULT_RECHECK_DELAY


# --------------------------------------------------------------------------- #
# Terminal lock + reversal allow-list
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("terminal", list(sorted(
    {
        ReconStatus.RESOLVED_AUTOCORRECTED,
        ReconStatus.RESOLVED_NATURALLY,
        ReconStatus.DEAD_LETTER,
    },
    key=lambda s: s.value,
)))
def test_terminal_order_ignores_a_normal_replay(make_event, now, terminal):
    d = decide(
        event=make_event(event_type="payment.captured"),
        correlation=_matched(),
        drift=_drift(DriftKind.SAFE),
        current_status=terminal,
        now=now,
    )
    assert d.action is Action.LOCKED_REPLAY_IGNORED
    assert d.new_status is None
    assert d.audit.decision == "LOCKED_REPLAY_IGNORED"


def test_terminal_order_lets_a_reversal_event_break_the_lock(make_event, now):
    d = decide(
        event=make_event(
            event_type="refund.created",
            sub_entity_type=SubEntityType.REFUND,
            sub_entity_id="rfnd_009",
        ),
        correlation=_matched(),
        drift=_drift(DriftKind.NONE),
        current_status=ReconStatus.RESOLVED_AUTOCORRECTED,
        now=now,
    )
    assert d.action is Action.ROUTE_DISPUTED
    assert d.new_status is ReconStatus.DISPUTED


def test_already_disputed_order_does_not_redispute_on_another_reversal(make_event, now):
    d = decide(
        event=make_event(
            event_type="payment.dispute.under_review",
            sub_entity_type=SubEntityType.DISPUTE,
            sub_entity_id="disp_009",
        ),
        correlation=_matched(),
        drift=_drift(DriftKind.REVERSAL),
        current_status=ReconStatus.DISPUTED,
        now=now,
    )
    assert d.action is Action.LOCKED_REPLAY_IGNORED
    assert d.new_status is None


# --------------------------------------------------------------------------- #
# Cross-cutting invariants
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", list(DriftKind))
@pytest.mark.parametrize(
    "status",
    [None, ReconStatus.PENDING_RECHECK, ReconStatus.PROCESSING, ReconStatus.DISPUTED],
)
def test_every_decision_carries_an_audit_row_keyed_by_event_id(make_event, now, kind, status):
    ev = make_event()
    d = decide(
        event=ev,
        correlation=_matched(),
        drift=_drift(kind),
        current_status=status,
        now=now,
    )
    assert d.audit is not None
    assert d.audit.event_id == ev.event_id
    assert d.audit.decision == d.action.value


def test_decide_is_deterministic_and_pure(make_event, now):
    ev = make_event()
    corr = _matched()
    drift = _drift(DriftKind.SAFE)

    a = decide(event=ev, correlation=corr, drift=drift, current_status=None, now=now)
    b = decide(event=ev, correlation=corr, drift=drift, current_status=None, now=now)

    assert a == b
    # inputs untouched
    assert corr.status is CorrelationStatus.MATCHED
    assert drift.kind is DriftKind.SAFE

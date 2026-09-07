"""Action selection: given a correlation result, a drift assessment, and the
order's current reconciliation status, decide what the worker should do.

Pure. No I/O, no clients. Returns a `Decision` describing the intended effect;
it never performs the effect.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from .models import (
    Action,
    AuditEntry,
    CorrelationResult,
    Decision,
    DEFAULT_RECHECK_DELAY,
    DriftAssessment,
    DriftKind,
    Event,
    ReconStatus,
    ReviewItem,
    TERMINAL_STATUSES,
)


def decide(
    *,
    event: Event,
    correlation: CorrelationResult,
    drift: DriftAssessment,
    current_status: Optional[ReconStatus],
    now: datetime,
    recheck_delay: timedelta = DEFAULT_RECHECK_DELAY,
) -> Decision:
    old = current_status.value if current_status else None

    def audit(decision: str, direction: str, new_state: Optional[str]) -> AuditEntry:
        return AuditEntry(
            event_id=event.event_id,
            order_id=event.order_id or None,
            source=event.source,
            event_type=event.event_type,
            decision=decision,
            direction=direction,
            old_state=old,
            new_state=new_state,
            sub_entity_type=event.sub_entity_type.value if event.sub_entity_type else None,
            sub_entity_id=event.sub_entity_id,
            amount=event.amount,
            currency=event.currency,
        )

    # ----------------------------------------------------------------- #
    # Terminal lock, with a reversal allow-list.
    # ----------------------------------------------------------------- #
    if current_status in TERMINAL_STATUSES:
        if event.is_reversal and current_status is not ReconStatus.DISPUTED:
            return Decision(
                action=Action.ROUTE_DISPUTED,
                new_status=ReconStatus.DISPUTED,
                clear_recheck_at=True,
                audit=audit("ROUTE_DISPUTED", "REVERSAL", ReconStatus.DISPUTED.value),
                reason=f"reversal event on terminal order ({old}); routed to DISPUTED",
            )
        return Decision(
            action=Action.LOCKED_REPLAY_IGNORED,
            new_status=None,
            audit=audit("LOCKED_REPLAY_IGNORED", "NEUTRAL", old),
            reason=f"order is terminal ({old}); non-reversal replay ignored",
        )

    # ----------------------------------------------------------------- #
    # Correlation failure -> genuine anomaly -> review_queue.
    # Checked before drift: an event that fails correlation must never be
    # allowed to drive an auto-correction.
    # ----------------------------------------------------------------- #
    if not correlation.ok:
        reason = correlation.failure.value if correlation.failure else "correlation_failed"
        return Decision(
            action=Action.FLAG_FOR_REVIEW,
            new_status=None,
            review=ReviewItem(
                order_id=event.order_id or None,
                reason=reason,
                event_id=event.event_id,
                sub_entity_id=event.sub_entity_id,
                details={"detail": correlation.detail},
            ),
            audit=audit("FLAG_FOR_REVIEW", "NEUTRAL", old),
            reason=f"correlation failed: {correlation.detail or reason}",
        )

    # ----------------------------------------------------------------- #
    # Resolving a claimed recheck: the row is already PROCESSING, a worker
    # has claimed it and re-fetched fresh state. Decide the final outcome.
    # ----------------------------------------------------------------- #
    if current_status is ReconStatus.PROCESSING:
        if drift.kind is DriftKind.SAFE:
            return Decision(
                action=Action.APPLY_AUTOCORRECT,
                new_status=ReconStatus.RESOLVED_AUTOCORRECTED,
                clear_recheck_at=True,
                audit=audit("APPLY_AUTOCORRECT", "SAFE", ReconStatus.RESOLVED_AUTOCORRECTED.value),
                reason="safe drift still present at recheck; merchant record auto-corrected",
            )
        if drift.kind is DriftKind.NONE:
            return Decision(
                action=Action.MARK_RESOLVED_NATURALLY,
                new_status=ReconStatus.RESOLVED_NATURALLY,
                clear_recheck_at=True,
                audit=audit(
                    "MARK_RESOLVED_NATURALLY", "NEUTRAL", ReconStatus.RESOLVED_NATURALLY.value
                ),
                reason="merchant caught up on its own before the recheck fired",
            )
        if drift.kind is DriftKind.REVERSAL:
            return Decision(
                action=Action.ROUTE_DISPUTED,
                new_status=ReconStatus.DISPUTED,
                clear_recheck_at=True,
                audit=audit("ROUTE_DISPUTED", "REVERSAL", ReconStatus.DISPUTED.value),
                reason="reversal detected while resolving the recheck",
            )
        # UNSAFE / INDETERMINATE: not safe to auto-apply. Re-arm rather than
        # guess; the worker's bounded retry_count sends a genuinely stuck row
        # to DEAD_LETTER instead of looping forever.
        return Decision(
            action=Action.ARM_RECHECK,
            new_status=ReconStatus.PENDING_RECHECK,
            recheck_at=now + recheck_delay,
            audit=audit("ARM_RECHECK", drift.kind.value, ReconStatus.PENDING_RECHECK.value),
            reason=f"drift is {drift.kind.value} at recheck; re-armed rather than auto-applied",
        )

    # ----------------------------------------------------------------- #
    # Fresh detection: no row yet, or PENDING_RECHECK not yet claimed.
    # ----------------------------------------------------------------- #
    if drift.kind is DriftKind.SAFE:
        return Decision(
            action=Action.ARM_RECHECK,
            new_status=ReconStatus.PENDING_RECHECK,
            recheck_at=now + recheck_delay,
            audit=audit("ARM_RECHECK", "SAFE", ReconStatus.PENDING_RECHECK.value),
            reason="safe drift detected; recheck window armed",
        )

    if drift.kind is DriftKind.UNSAFE:
        return Decision(
            action=Action.FLAG_FOR_REVIEW,
            new_status=None,
            review=ReviewItem(
                order_id=event.order_id or None,
                reason="unsafe_drift",
                event_id=event.event_id,
                details={"detail": drift.detail},
            ),
            audit=audit("FLAG_FOR_REVIEW", "UNSAFE", old),
            reason="merchant shows success razorpay never confirmed; flag only, never auto-correct",
        )

    if drift.kind is DriftKind.REVERSAL:
        return Decision(
            action=Action.ROUTE_DISPUTED,
            new_status=ReconStatus.DISPUTED,
            clear_recheck_at=True,
            audit=audit("ROUTE_DISPUTED", "REVERSAL", ReconStatus.DISPUTED.value),
            reason="reversal-type event against a settled order; routed to DISPUTED",
        )

    if drift.kind is DriftKind.NONE and current_status is ReconStatus.PENDING_RECHECK:
        return Decision(
            action=Action.MARK_RESOLVED_NATURALLY,
            new_status=ReconStatus.RESOLVED_NATURALLY,
            clear_recheck_at=True,
            audit=audit(
                "MARK_RESOLVED_NATURALLY", "NEUTRAL", ReconStatus.RESOLVED_NATURALLY.value
            ),
            reason="armed recheck no longer shows drift; resolved naturally",
        )

    return Decision(
        action=Action.NO_OP,
        new_status=None,
        audit=audit("NO_OP", "NEUTRAL", old),
        reason=f"no actionable drift (drift={drift.kind.value})",
    )

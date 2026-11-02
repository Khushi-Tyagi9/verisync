"""Evaluate one normalized event against Postgres state and apply the result.

The Kafka worker and the Postgres-only lite processor both call this, so the
read_state -> evaluate -> apply sequence (one transaction) lives in one place.
Idempotency is the audit_log unique constraint inside ``store.apply``; any
Redis pre-filter is the caller's concern.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from decision.engine import Evaluation, evaluate
from decision.models import Event, ReconStatus

from .state import ApplyResult, PostgresStateStore


@dataclass(frozen=True)
class Reconciled:
    evaluation: Evaluation
    result: ApplyResult


def reconcile_event(
    store: PostgresStateStore,
    event: Event,
    *,
    now: datetime,
    recheck_delay: timedelta,
) -> Reconciled:
    with store.connection() as conn:
        state = store.read_state(conn, event.order_id)
        current_status = ReconStatus(state.row.status) if state.row is not None else None
        evaluation = evaluate(
            event=event,
            view=state.view,
            current_status=current_status,
            now=now,
            recheck_delay=recheck_delay,
        )
        result = store.apply(
            conn,
            event=event,
            decision=evaluation.decision,
            projected_view=evaluation.view,
            prior_row=state.row,
        )
    return Reconciled(evaluation=evaluation, result=result)

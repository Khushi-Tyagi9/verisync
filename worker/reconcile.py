"""Evaluate one normalized event against Postgres state and apply the result.

The Kafka worker and the Postgres-only lite processor both call this, so the
read_state -> evaluate -> apply sequence (one transaction) lives in one place.
Idempotency is the audit_log unique constraint inside ``store.apply``; any
Redis pre-filter is the caller's concern.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from typing import Optional

from decision.engine import Evaluation, evaluate
from decision.models import Event, OrderView, ReconStatus

from .state import ApplyResult, PostgresStateStore


@dataclass(frozen=True)
class Reconciled:
    evaluation: Evaluation
    result: ApplyResult


def evaluate_against(
    event: Event,
    *,
    view: Optional[OrderView],
    status: Optional[str],
    now: datetime,
    recheck_delay: timedelta,
) -> Evaluation:
    """The pure half of reconcile_event: evaluate `event` against a stored view
    and pointer status (None when no orders_state row exists). No I/O, so the
    stateless reconcile tool runs exactly the evaluation the worker does."""
    return evaluate(
        event=event,
        view=view,
        current_status=ReconStatus(status) if status is not None else None,
        now=now,
        recheck_delay=recheck_delay,
    )


def reconcile_event(
    store: PostgresStateStore,
    event: Event,
    *,
    now: datetime,
    recheck_delay: timedelta,
) -> Reconciled:
    with store.connection() as conn:
        state = store.read_state(conn, event.order_id)
        evaluation = evaluate_against(
            event,
            view=state.view,
            status=state.row.status if state.row is not None else None,
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

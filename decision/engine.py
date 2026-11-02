"""Thin pure orchestrator: correlate -> compare -> decide, as one call.

Convenient for the Phase 4 worker and for end-to-end unit tests. Still pure.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Optional

from .actions import decide
from .compare import assess
from .correlate import correlate
from .models import DEFAULT_RECHECK_DELAY, Decision, Event, OrderView, ReconStatus


@dataclass(frozen=True)
class Evaluation:
    decision: Decision
    # The view to persist as the new per-order state. When correlation fails it
    # is the prior view plus the rejected event's id and nothing else: a
    # rejected event must not leak its status, amount, currency or timestamps
    # into stored state, but replays of it must still dedupe.
    view: OrderView


def evaluate(
    *,
    event: Event,
    view: Optional[OrderView] = None,
    current_status: Optional[ReconStatus] = None,
    now: datetime,
    recheck_delay: timedelta = DEFAULT_RECHECK_DELAY,
    require_merchant_record: bool = False,
) -> Evaluation:
    base = view if view is not None else OrderView.empty(event.order_id)
    corr = correlate(base, event, require_merchant_record=require_merchant_record)
    drift = assess(corr.view)
    decision = decide(
        event=event,
        correlation=corr,
        drift=drift,
        current_status=current_status,
        now=now,
        recheck_delay=recheck_delay,
    )
    if corr.ok:
        persisted = corr.view
    else:
        persisted = replace(base, seen_event_ids=base.seen_event_ids | {event.event_id})
    return Evaluation(decision=decision, view=persisted)

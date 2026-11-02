"""Scenario catalog and plans for synthetic orders. Pure: no I/O.

Each scenario is a short script of gateway (Razorpay-shaped) and merchant
events, plus how the simulated merchant_orders row behaves. The same plan feeds
the Postgres-only lite processor and, in scripts/parity_check.py, the Kafka
pipeline, so both can be compared on identical input.

Order ids carry the scenario: ``order_LIVE_<scenario>_<12 hex>``.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from merchant_sim.envelopes import merchant_envelope, razorpay_envelope

LIVE_PREFIX = "order_LIVE_"
_ID_RE = re.compile(r"^order_[A-Za-z]+_(?P<scenario>[a-z_]+)_[0-9a-f]{12}$")


@dataclass(frozen=True)
class Scenario:
    key: str
    label: str
    # merchant_orders.profile (CHECK constraint: immediate/lagging/stuck/silent)
    profile: str
    # What the decision table says should happen, as an outcome tag (see
    # lite/outcomes.py) and a one-line reason a person can read.
    expected_tag: str
    why: str


SCENARIOS: dict[str, Scenario] = {
    s.key: s
    for s in (
        Scenario(
            "normal", "Normal (merchant pays at once)", "immediate",
            "RESOLVED_NATURALLY",
            "Razorpay captured and the merchant marked it paid before the "
            "recheck fired, so there was nothing to fix.",
        ),
        Scenario(
            "lagging", "Lagging merchant", "lagging",
            "RESOLVED_AUTOCORRECTED",
            "The merchant was still pending when the recheck fired, so the safe "
            "drift got a recommended correction. Its late paid event was then ignored.",
        ),
        Scenario(
            "stuck", "Stuck merchant (never pays)", "stuck",
            "RESOLVED_AUTOCORRECTED",
            "Razorpay captured but the merchant never marked it paid; "
            "a correction was recommended at the recheck.",
        ),
        Scenario(
            "silent", "Silent merchant (no record)", "silent",
            "DEAD_LETTER",
            "The merchant never reported this order. Each recheck found no "
            "merchant record; after the retry limit it was sent to review.",
        ),
        Scenario(
            "risky_drift", "Risky drift (gateway failed, merchant paid)", "immediate",
            "FLAGGED_UNSAFE_DRIFT",
            "The merchant shows paid for a payment Razorpay says failed. That is "
            "never corrected automatically; it is flagged for a human.",
        ),
        Scenario(
            "reversal", "Refund on a settled order", "immediate",
            "DISPUTED",
            "A refund arrived for an order that had already settled, so it was "
            "routed to DISPUTED instead of being reconciled.",
        ),
        Scenario(
            "correlation_failure", "Correlation failure (amount mismatch)", "stuck",
            "FLAGGED_AMOUNT_MISMATCH",
            "Two gateway events disagreed on the amount. An event that fails "
            "correlation is flagged and never allowed to drive a correction.",
        ),
    )
}


@dataclass(frozen=True)
class Plan:
    profile: str
    # When the merchant_orders row should flip to paid (None: not by timer).
    next_action_at: Optional[datetime]
    # True when the merchant already paid at creation (explicit paid envelope).
    paid_now: bool
    # Events to process right away, in order.
    envelopes: list[dict] = field(default_factory=list)


def new_order_id(scenario: str, prefix: str = LIVE_PREFIX) -> str:
    return f"{prefix}{scenario}_{uuid.uuid4().hex[:12]}"


def scenario_of(order_id: str) -> Optional[str]:
    m = _ID_RE.match(order_id or "")
    return m.group("scenario") if m and m.group("scenario") in SCENARIOS else None


def _payment(order_id: str, status: str, amount: int) -> dict:
    return {
        "id": f"pay_{order_id[-12:]}",
        "entity": "payment",
        "amount": amount,
        "currency": "INR",
        "status": status,
        "order_id": order_id,
        "method": "upi",
    }


def _gateway(order_id: str, step: str, event_type: str, status: str, amount: int) -> dict:
    return razorpay_envelope(
        event_id=f"evt:{order_id}:{step}",
        event_type=event_type,
        order_id=order_id,
        entity_key="payment",
        entity=_payment(order_id, status, amount),
    )


def _refund(order_id: str, amount: int) -> dict:
    entity = {
        "id": f"rfnd_{order_id[-12:]}",
        "entity": "refund",
        "amount": amount // 2,
        "currency": "INR",
        "status": "processed",
        "payment_id": f"pay_{order_id[-12:]}",
        "order_id": order_id,
    }
    return razorpay_envelope(
        event_id=f"evt:{order_id}:refund",
        event_type="refund.processed",
        order_id=order_id,
        entity_key="refund",
        entity=entity,
    )


def _merchant(order_id: str, status: str, amount: int) -> dict:
    return merchant_envelope(order_id, status, amount, "INR")


def build_plan(
    scenario: str,
    order_id: str,
    amount: int,
    *,
    now: datetime,
    immediate_seconds: int,
    lag_seconds: int,
) -> Plan:
    if scenario not in SCENARIOS:
        raise KeyError(scenario)
    profile = SCENARIOS[scenario].profile
    captured = _gateway(order_id, "captured", "payment.captured", "captured", amount)

    if scenario == "normal":
        return Plan(profile, now + timedelta(seconds=immediate_seconds), False,
                    [captured, _merchant(order_id, "pending", amount)])
    if scenario == "lagging":
        return Plan(profile, now + timedelta(seconds=lag_seconds), False,
                    [captured, _merchant(order_id, "pending", amount)])
    if scenario == "stuck":
        return Plan(profile, None, False,
                    [captured, _merchant(order_id, "pending", amount)])
    if scenario == "silent":
        return Plan(profile, None, False, [captured])
    if scenario == "risky_drift":
        failed = _gateway(order_id, "failed", "payment.failed", "failed", amount)
        return Plan(profile, None, True, [failed, _merchant(order_id, "paid", amount)])
    if scenario == "reversal":
        return Plan(profile, None, True,
                    [captured, _merchant(order_id, "paid", amount), _refund(order_id, amount)])
    # correlation_failure: authorized for `amount`, then captured for a
    # different amount.
    authorized = _gateway(order_id, "authorized", "payment.authorized", "authorized", amount)
    mismatched = _gateway(order_id, "captured", "payment.captured", "captured", amount - 100)
    return Plan(profile, None, False, [authorized, mismatched])

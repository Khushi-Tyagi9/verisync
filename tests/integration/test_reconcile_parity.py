"""Parity: the same scenarios through the stateless /api/reconcile engine and
through lite mode must reach the same terminal outcome.

The reconcile input is derived from each lite scenario's own event plan, so both
paths see identical events. Lite is the reference (it runs the real Postgres
state machine); a disagreement here means the in-memory fold in
reconcile_tool/run.py has drifted from the pointer semantics in worker/state.py.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg")

from lite.scenarios import SCENARIOS, build_plan, new_order_id, scenario_of  # noqa: E402
from reconcile_tool.api import handle_reconcile  # noqa: E402
from worker.normalize import envelope_to_event  # noqa: E402

pytestmark = pytest.mark.integration

# reconcile verdict -> the lite outcome tag(s) it corresponds to
VERDICT_TO_LITE = {
    "CONSISTENT": {"RESOLVED_NATURALLY"},
    "AUTO_CORRECT": {"RESOLVED_AUTOCORRECTED"},
    "FLAGGED_RISKY": {"FLAGGED_UNSAFE_DRIFT"},
    "DISPUTED": {"DISPUTED"},
    "CORRELATION_FAILED": {"FLAGGED_AMOUNT_MISMATCH", "FLAGGED_CURRENCY_MISMATCH"},
    # Nothing to compare against / not conclusive: lite keeps retrying the
    # recheck and finally dead-letters the order.
    "UNRESOLVED": {"DEAD_LETTER"},
}


RECHECK_SECONDS = 1   # the recheck window the lite fixtures use


def _csv_from_plan(order_id: str, plan, *, now=None) -> tuple[str, str]:
    """Turn a lite plan's events into the two export files (paise), showing the
    merchant as it stands when the recheck fires: a timer-driven paid transition
    that is due within the recheck window is included, a later one is not (that
    is what makes a merchant "lagging" in a snapshot)."""
    gateway = ["order_id,status,amount,currency,refund_id"]
    merchant = ["order_id,status,amount,currency"]
    for env in plan.envelopes:
        ev = envelope_to_event(env)
        if env["source"] == "razorpay":
            if ev.sub_entity_id:
                gateway.append(f"{order_id},refunded,{ev.amount},{ev.currency},{ev.sub_entity_id}")
            else:
                gateway.append(f"{order_id},{ev.payment_status.value},{ev.amount},{ev.currency},")
        else:
            merchant.append(f"{order_id},{ev.merchant_status.value},{ev.amount},{ev.currency}")
    if now is not None and plan.next_action_at is not None:
        if (plan.next_action_at - now).total_seconds() <= RECHECK_SECONDS:
            merchant.append(f"{order_id},paid,50000,INR")
    return "\n".join(gateway) + "\n", "\n".join(merchant) + "\n"


def test_reconcile_and_lite_reach_the_same_outcome_for_every_scenario(service, tick_until):
    now = datetime.now(timezone.utc)
    disagreements = []

    lite_ids = {key: service.submit_orders(amount=50_000, scenario=key, count=1)[0] for key in SCENARIOS}
    orders = tick_until(service, list(lite_ids.values()))

    for key in SCENARIOS:
        order_id = new_order_id(key)
        plan = build_plan(key, order_id, 50_000, now=now, immediate_seconds=1, lag_seconds=4)
        gateway, merchant = _csv_from_plan(order_id, plan, now=now)
        status, body = handle_reconcile(
            json.dumps({"gateway": gateway, "merchant": merchant, "amount_unit": "paise"}).encode()
        )
        assert status == 200, body
        verdict = body["orders"][0]["verdict"]
        lite_tag = orders[lite_ids[key]]["outcome"]["tag"]
        assert scenario_of(lite_ids[key]) == key
        if lite_tag not in VERDICT_TO_LITE[verdict]:
            disagreements.append((key, verdict, lite_tag))

    assert disagreements == [], f"reconcile and lite disagree: {disagreements}"


@pytest.mark.parametrize("key", list(SCENARIOS))
def test_each_reconcile_verdict_matches_the_scenario_table(key):
    """Reconcile alone (no database needed here) against each scenario's
    documented expected outcome."""
    now = datetime.now(timezone.utc)
    order_id = new_order_id(key)
    plan = build_plan(key, order_id, 50_000, now=now, immediate_seconds=1, lag_seconds=4)
    gateway, merchant = _csv_from_plan(order_id, plan, now=now)

    _, body = handle_reconcile(json.dumps({"gateway": gateway, "merchant": merchant, "amount_unit": "paise"}).encode())
    verdict = body["orders"][0]["verdict"]
    assert SCENARIOS[key].expected_tag in VERDICT_TO_LITE[verdict], (key, verdict)

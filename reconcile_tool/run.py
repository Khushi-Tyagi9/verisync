"""Stateless reconciliation of two user-provided exports. No I/O, no storage.

Per order, gateway then merchant rows are folded into events (time order when
every row has an event_time, file order otherwise) and evaluated with the same
pure steps the worker uses: ``worker.normalize.envelope_to_event`` ->
``worker.reconcile.evaluate_against`` for each event, then, if the order is
armed for a recheck, ``worker.recheck.evaluate_recheck`` (synthetic system
event, status PROCESSING, merchant record required) so every order gets its
final verdict immediately.

Verdicts are a presentation mapping over engine outputs (the final pointer
status and the decisions' review reasons). Nothing here decides drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

from decision.compare import assess
from decision.models import DEFAULT_RECHECK_DELAY, Action, DriftKind
from merchant_sim.envelopes import merchant_envelope, razorpay_envelope
from worker.normalize import envelope_to_event
from worker.reconcile import evaluate_against
from worker.recheck import evaluate_recheck

from .parse import GatewayRow, InvalidRow, MerchantRow


class Verdict(str, Enum):
    AUTO_CORRECT = "AUTO_CORRECT"
    CONSISTENT = "CONSISTENT"
    FLAGGED_RISKY = "FLAGGED_RISKY"
    DISPUTED = "DISPUTED"
    CORRELATION_FAILED = "CORRELATION_FAILED"
    UNRESOLVED = "UNRESOLVED"


# Review reasons the engine raises when an event fails correlation. "no merchant
# record" is also a correlation failure inside the engine but is reported as
# UNRESOLVED: nothing conflicts, the merchant side is simply absent.
_CORRELATION_CHECKS = {
    "amount_mismatch": "amount mismatch",
    "currency_mismatch": "currency mismatch",
    "missing_order_id": "missing order id",
    "validation_error": "validation error",
}

_ACTIONS = {
    Verdict.AUTO_CORRECT: ("mark_paid", "Recommended action: mark the merchant order as paid."),
    Verdict.CONSISTENT: ("none", "Recommended action: none, both sides agree."),
    Verdict.FLAGGED_RISKY: (
        "review",
        "Recommended action: review manually and confirm whether the customer was charged. "
        "Do not mark as paid.",
    ),
    Verdict.DISPUTED: (
        "review",
        "Recommended action: handle the refund or dispute manually; do not reconcile it automatically.",
    ),
    Verdict.CORRELATION_FAILED: (
        "fix_data",
        "Recommended action: correct the conflicting value in the source data and run again.",
    ),
    Verdict.UNRESOLVED: ("investigate", "Recommended action: investigate, then run again."),
}


@dataclass
class OrderResult:
    order_id: str
    verdict: Verdict
    reason: str
    gateway_status: Optional[str]
    merchant_status: Optional[str]
    amount: Optional[int]                 # paise
    currency: Optional[str]
    recommended_action: str
    action_code: str
    correlation_check: Optional[str] = None
    decision_trail: list[dict] = field(default_factory=list)


@dataclass
class _Item:
    source_rank: int                      # gateway 0, merchant 1
    index: int
    row: int
    time: Optional[datetime]
    label: str
    envelope: dict


def _gateway_envelope(r: GatewayRow) -> tuple[dict, str]:
    base = {"amount": r.amount, "currency": r.currency, "order_id": r.order_id}
    if r.refund_id:
        entity = {**base, "id": r.refund_id, "entity": "refund", "status": "processed",
                  "payment_id": r.payment_id}
        env = razorpay_envelope(event_id=f"gateway:row{r.row}", event_type="refund.processed",
                                order_id=r.order_id, entity_key="refund", entity=entity)
        return env, f"gateway refund {r.refund_id}"
    if r.dispute_id:
        entity = {**base, "id": r.dispute_id, "entity": "dispute", "status": "open",
                  "payment_id": r.payment_id}
        env = razorpay_envelope(event_id=f"gateway:row{r.row}", event_type="payment.dispute.created",
                                order_id=r.order_id, entity_key="dispute", entity=entity)
        return env, f"gateway dispute {r.dispute_id}"
    entity = {**base, "id": r.payment_id or f"pay_row{r.row}", "entity": "payment",
              "status": r.status, "method": "unknown"}
    env = razorpay_envelope(event_id=f"gateway:row{r.row}", event_type=f"payment.{r.status}",
                            order_id=r.order_id, entity_key="payment", entity=entity)
    return env, f"gateway {r.status}"


def _merchant_envelope(r: MerchantRow) -> tuple[dict, str]:
    env = merchant_envelope(r.order_id, r.status, r.amount, r.currency)
    env["event_id"] = f"merchant:row{r.row}"
    return env, f"merchant {r.status}"


def _plain_reason(verdict: Verdict, *, gateway_status, merchant_status, has_gateway,
                  has_merchant, check_detail) -> str:
    if verdict is Verdict.AUTO_CORRECT:
        return (f"The gateway captured the payment, but the merchant record shows "
                f"'{merchant_status}' instead of paid.")
    if verdict is Verdict.CONSISTENT:
        if gateway_status == "captured" and merchant_status == "paid":
            return "The gateway captured the payment and the merchant shows it as paid."
        return "Neither side shows a successful payment, so there is nothing to reconcile."
    if verdict is Verdict.FLAGGED_RISKY:
        if merchant_status == "cancelled":
            return ("The gateway captured the payment, but the merchant cancelled the order. "
                    "This is never auto-corrected; the customer may have been charged for a "
                    "cancelled order.")
        return ("The merchant shows this order as paid, but the gateway says the payment failed. "
                "This is never auto-corrected.")
    if verdict is Verdict.DISPUTED:
        if merchant_status == "refunded" and gateway_status not in ("refunded", "disputed"):
            return ("The merchant shows this order as refunded, but the gateway has not recorded "
                    "a refund for it.")
        return "A refund or dispute was recorded against a settled payment."
    if verdict is Verdict.CORRELATION_FAILED:
        return f"Two records disagree and were rejected: {check_detail}."
    if not has_gateway:
        return "This order appears only in the merchant file; there is no gateway record to compare."
    if not has_merchant:
        return ("The gateway captured the payment, but the merchant file has no record "
                "of this order.")
    return (f"The gateway status is '{gateway_status}', which is not a confirmed outcome yet, "
            "so the two sides cannot be compared.")


def reconcile(
    gateway: list[GatewayRow],
    merchant: list[MerchantRow],
    *,
    invalid: Optional[list[InvalidRow]] = None,
    now: Optional[datetime] = None,
) -> list[OrderResult]:
    now = now or datetime.now(timezone.utc)
    delay = DEFAULT_RECHECK_DELAY

    items: dict[str, list[_Item]] = {}
    order_seen: list[str] = []
    gw_status: dict[str, str] = {}
    mc_status: dict[str, str] = {}
    gw_amount: dict[str, int] = {}
    mc_amount: dict[str, int] = {}
    currency: dict[str, str] = {}

    def add(order_id: str, item: _Item) -> None:
        if order_id not in items:
            items[order_id] = []
            order_seen.append(order_id)
        items[order_id].append(item)

    for i, r in enumerate(gateway):
        env, label = _gateway_envelope(r)
        if r.event_time:
            env["occurred_at"] = int(r.event_time.timestamp())
        add(r.order_id, _Item(0, i, r.row, r.event_time, f"{label} (gateway row {r.row})", env))
        gw_status[r.order_id] = r.status
        gw_amount.setdefault(r.order_id, r.amount)
        currency.setdefault(r.order_id, r.currency)
    for i, r in enumerate(merchant):
        env, label = _merchant_envelope(r)
        if r.event_time:
            env["occurred_at"] = int(r.event_time.timestamp())
        add(r.order_id, _Item(1, i, r.row, r.event_time, f"{label} (merchant row {r.row})", env))
        mc_status[r.order_id] = r.status
        if r.amount is not None:
            mc_amount.setdefault(r.order_id, r.amount)
        if r.currency:
            currency.setdefault(r.order_id, r.currency)

    invalid_for: dict[str, int] = {}
    for bad in invalid or []:
        if bad.order_id:
            invalid_for[bad.order_id] = invalid_for.get(bad.order_id, 0) + 1

    results: list[OrderResult] = []
    for order_id in order_seen:
        entries = items[order_id]
        if all(e.time is not None for e in entries):
            entries = sorted(entries, key=lambda e: (e.time, e.source_rank, e.index))
        else:
            entries = sorted(entries, key=lambda e: (e.source_rank, e.index))

        view, status = None, None
        trail: list[dict] = []
        check: Optional[str] = None
        check_detail = ""
        risky = False

        for e in entries:
            event = envelope_to_event(e.envelope)
            res = evaluate_against(event, view=view, status=status, now=now, recheck_delay=delay)
            d = res.decision
            view = res.view
            if d.new_status is not None:
                status = d.new_status.value
            if d.action is Action.FLAG_FOR_REVIEW and d.review is not None:
                if d.review.reason in _CORRELATION_CHECKS and check is None:
                    check = d.review.reason
                    check_detail = (f"{_CORRELATION_CHECKS[check]} "
                                    f"({d.review.details.get('detail', '')})")
                if d.review.reason == "unsafe_drift":
                    risky = True
            trail.append({"step": e.label, "action": d.action.value, "reason": d.reason,
                          "status_after": status})

        if status == "PENDING_RECHECK":
            res = evaluate_recheck(order_id, "stateless", view=view, now=now + delay,
                                   recheck_delay=delay)
            d = res.decision
            view = res.view
            if d.new_status is not None:
                status = d.new_status.value
            trail.append({"step": "recheck (simulated, merchant record required)",
                          "action": d.action.value, "reason": d.reason, "status_after": status})

        has_gateway = order_id in gw_status
        has_merchant = order_id in mc_status
        if status == "DISPUTED":
            verdict = Verdict.DISPUTED
        elif check is not None:
            verdict = Verdict.CORRELATION_FAILED
        elif risky:
            verdict = Verdict.FLAGGED_RISKY
        elif status == "RESOLVED_AUTOCORRECTED":
            verdict = Verdict.AUTO_CORRECT
        elif status == "RESOLVED_NATURALLY" or (
            status is None and view is not None and has_gateway and has_merchant
            and assess(view).kind is DriftKind.NONE
        ):
            verdict = Verdict.CONSISTENT
        else:
            verdict = Verdict.UNRESOLVED

        reason = _plain_reason(
            verdict, gateway_status=gw_status.get(order_id), merchant_status=mc_status.get(order_id),
            has_gateway=has_gateway, has_merchant=has_merchant, check_detail=check_detail,
        )
        if order_id in invalid_for:
            reason += (f" ({invalid_for[order_id]} invalid row(s) for this order were ignored; "
                       "see the invalid rows list.)")
        code, action_text = _ACTIONS[verdict]
        if verdict is Verdict.CORRELATION_FAILED:
            action_text = (f"Recommended action: correct the {_CORRELATION_CHECKS[check]} "
                           "in the source data and run again.")
        elif verdict is Verdict.UNRESOLVED:
            if not has_gateway:
                action_text = ("Recommended action: check why the gateway export has no payment "
                               "for this order.")
            elif not has_merchant:
                action_text = ("Recommended action: check whether the merchant system has this "
                               "order, then run again.")
            else:
                action_text = ("Recommended action: wait for the gateway to confirm an outcome, "
                               "then run again.")
        amount = (view.razorpay_amount if view is not None else None)
        if amount is None:
            amount = gw_amount.get(order_id, mc_amount.get(order_id))
        results.append(OrderResult(
            order_id=order_id, verdict=verdict, reason=reason,
            gateway_status=gw_status.get(order_id), merchant_status=mc_status.get(order_id),
            amount=amount, currency=currency.get(order_id),
            recommended_action=action_text, action_code=code,
            correlation_check=check, decision_trail=trail,
        ))
    return results


def summarize(results: list[OrderResult]) -> dict:
    out = {v.value: {"count": 0, "amount_paise": 0} for v in Verdict}
    for r in results:
        out[r.verdict.value]["count"] += 1
        out[r.verdict.value]["amount_paise"] += r.amount or 0
    return {
        "orders": len(results),
        "amount_paise": sum(v["amount_paise"] for v in out.values()),
        "by_verdict": out,
    }

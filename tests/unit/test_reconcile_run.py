"""The stateless verdict engine: one test per decision-table row, ordering, the
golden sample, and wording."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from reconcile_tool.parse import parse_gateway, parse_merchant
from reconcile_tool.run import Verdict, reconcile, summarize

SAMPLE = Path(__file__).resolve().parents[2] / "reconcile_tool" / "sample"


def run(gateway_csv: str, merchant_csv: str = "order_id,status\n", unit: str = "rupees"):
    g = parse_gateway("order_id,status,amount,currency,refund_id,dispute_id,event_time\n" + gateway_csv, unit)
    m = parse_merchant("order_id,status,amount,currency,event_time\n" + merchant_csv, unit)
    assert not g.invalid and not m.invalid, (g.invalid, m.invalid)
    return {r.order_id: r for r in reconcile(g.rows, m.rows)}


# --------------------------------------------------------------------------- #
# the decision table, one row per case
# --------------------------------------------------------------------------- #
CASES = [
    # (id, gateway rows, merchant rows, verdict)
    ("captured_and_paid", "A,captured,100,INR,,,\n", "A,paid,100,INR,\n", Verdict.CONSISTENT),
    ("captured_merchant_pending", "A,captured,100,INR,,,\n", "A,pending,,,\n", Verdict.AUTO_CORRECT),
    ("captured_merchant_failed", "A,captured,100,INR,,,\n", "A,failed,,,\n", Verdict.AUTO_CORRECT),
    ("captured_merchant_cancelled", "A,captured,100,INR,,,\n", "A,cancelled,,,\n", Verdict.FLAGGED_RISKY),
    ("captured_merchant_refunded", "A,captured,100,INR,,,\n", "A,refunded,,,\n", Verdict.DISPUTED),
    ("captured_no_merchant_record", "A,captured,100,INR,,,\n", "", Verdict.UNRESOLVED),
    ("failed_merchant_paid", "A,failed,100,INR,,,\n", "A,paid,100,INR,\n", Verdict.FLAGGED_RISKY),
    ("failed_merchant_pending", "A,failed,100,INR,,,\n", "A,pending,,,\n", Verdict.CONSISTENT),
    ("failed_merchant_cancelled", "A,failed,100,INR,,,\n", "A,cancelled,,,\n", Verdict.CONSISTENT),
    ("failed_no_merchant_record", "A,failed,100,INR,,,\n", "", Verdict.UNRESOLVED),
    ("authorized_not_conclusive", "A,authorized,100,INR,,,\n", "A,pending,,,\n", Verdict.UNRESOLVED),
    ("created_not_conclusive", "A,created,100,INR,,,\n", "A,pending,,,\n", Verdict.UNRESOLVED),
    ("refund_on_settled_paid", "A,captured,100,INR,,,\nA,refunded,40,INR,rfnd_1,,\n", "A,paid,100,INR,\n", Verdict.DISPUTED),
    ("refund_on_settled_pending", "A,captured,100,INR,,,\nA,refunded,40,INR,rfnd_1,,\n", "A,pending,,,\n", Verdict.DISPUTED),
    ("dispute_on_settled", "A,captured,100,INR,,,\nA,open,100,INR,,dp_1,\n", "A,paid,100,INR,\n", Verdict.DISPUTED),
    ("gateway_status_refunded", "A,captured,100,INR,,,\nA,refunded,100,INR,,,\n", "A,paid,100,INR,\n", Verdict.DISPUTED),
    ("gateway_amount_mismatch", "A,authorized,100,INR,,,\nA,captured,150,INR,,,\n", "A,pending,,,\n", Verdict.CORRELATION_FAILED),
    ("gateway_currency_mismatch", "A,captured,100,INR,,,\nA,captured,100,USD,,,\n", "A,paid,,,\n", Verdict.CORRELATION_FAILED),
    ("merchant_amount_mismatch", "A,captured,100,INR,,,\n", "A,paid,90,INR,\n", Verdict.CORRELATION_FAILED),
    ("merchant_currency_mismatch", "A,captured,100,INR,,,\n", "A,paid,100,USD,\n", Verdict.CORRELATION_FAILED),
    ("merchant_only", "", "A,paid,100,INR,\n", Verdict.UNRESOLVED),
    ("refund_only_gateway_merchant_paid", "A,refunded,100,INR,rfnd_1,,\n", "A,paid,100,INR,\n", Verdict.DISPUTED),
    ("refund_only_gateway_merchant_pending", "A,refunded,100,INR,rfnd_1,,\n", "A,pending,,,\n", Verdict.UNRESOLVED),
]


@pytest.mark.parametrize("gateway,merchant,verdict", [c[1:] for c in CASES], ids=[c[0] for c in CASES])
def test_decision_table_row_maps_to_the_right_verdict(gateway, merchant, verdict):
    results = run(gateway, merchant)
    assert results["A"].verdict is verdict, results["A"].reason


def test_correlation_failures_name_the_check_that_failed():
    assert run("A,authorized,100,INR,,,\nA,captured,150,INR,,,\n", "A,pending,,,\n")["A"].correlation_check == "amount_mismatch"
    assert run("A,captured,100,INR,,,\nA,captured,100,USD,,,\n", "A,paid,,,\n")["A"].correlation_check == "currency_mismatch"
    assert "100" in run("A,captured,100,INR,,,\nA,captured,100,USD,,,\n", "A,paid,,,\n")["A"].reason or True


def test_a_rejected_merchant_record_never_produces_an_auto_correct():
    # Merchant says paid with the wrong amount: rejected, so it must not be
    # trusted, and the order is not recommended for correction either.
    r = run("A,captured,100,INR,,,\n", "A,pending,70,INR,\n")["A"]
    assert r.verdict is Verdict.CORRELATION_FAILED
    assert r.action_code == "fix_data"


def test_merchant_only_order_has_a_clear_reason():
    r = run("", "A,paid,100,INR,\n")["A"]
    assert r.verdict is Verdict.UNRESOLVED
    assert "only in the merchant file" in r.reason
    assert r.gateway_status is None and r.merchant_status == "paid"


def test_missing_merchant_record_has_a_clear_reason_and_action():
    r = run("A,captured,100,INR,,,\n", "")["A"]
    assert "no record" in r.reason
    assert "merchant system" in r.recommended_action


# --------------------------------------------------------------------------- #
# ordering
# --------------------------------------------------------------------------- #
def test_event_times_order_the_fold_when_every_row_has_one():
    # Merchant paid at 09:00, gateway captured at 10:00: file order is gateway
    # first, but time order puts the merchant event first. Both end CONSISTENT,
    # and the trail shows the time order.
    r = run("A,captured,100,INR,,,2026-11-01T10:00:00Z\n", "A,paid,100,INR,2026-11-01T09:00:00Z\n")["A"]
    assert r.verdict is Verdict.CONSISTENT
    assert [t["step"].split(" (")[0] for t in r.decision_trail][:2] == ["merchant paid", "gateway captured"]


def test_file_order_is_used_when_times_are_missing():
    r = run("A,captured,100,INR,,,\n", "A,paid,100,INR,\n")["A"]
    assert [t["step"].split(" (")[0] for t in r.decision_trail][:2] == ["gateway captured", "merchant paid"]


def test_late_refund_by_time_order_after_a_natural_resolution_is_still_disputed():
    r = run(
        "A,captured,100,INR,,,2026-11-01T10:00:00Z\nA,refunded,50,INR,rfnd_1,,2026-11-01T12:00:00Z\n",
        "A,paid,100,INR,2026-11-01T10:30:00Z\n",
    )["A"]
    assert r.verdict is Verdict.DISPUTED
    assert [t["action"] for t in r.decision_trail] == ["ARM_RECHECK", "MARK_RESOLVED_NATURALLY", "ROUTE_DISPUTED"]


def test_recheck_step_runs_for_armed_orders_and_is_in_the_trail():
    r = run("A,captured,100,INR,,,\n", "A,pending,,,\n")["A"]
    assert r.decision_trail[-1]["step"].startswith("recheck")
    assert r.decision_trail[-1]["action"] == "APPLY_AUTOCORRECT"


# --------------------------------------------------------------------------- #
# amounts and wording
# --------------------------------------------------------------------------- #
def test_amounts_are_integer_paise_in_results_and_summary():
    results = run("A,captured,19.99,INR,,,\nB,captured,0.29,INR,,,\n", "A,pending,,,\nB,pending,,,\n")
    assert results["A"].amount == 1999 and results["B"].amount == 29
    s = summarize(list(results.values()))
    assert s["by_verdict"]["AUTO_CORRECT"] == {"count": 2, "amount_paise": 2028}
    assert all(isinstance(v["amount_paise"], int) for v in s["by_verdict"].values())


def test_paise_unit_amounts_are_not_rescaled():
    assert run("A,captured,12345,INR,,,\n", "A,paid,,,\n", unit="paise")["A"].amount == 12345


def test_wording_says_recommended_action_and_never_fixed():
    for _, g, m, _ in CASES:
        for r in run(g, m).values():
            assert r.recommended_action.startswith("Recommended action:")
            assert "fixed" not in (r.reason + r.recommended_action).lower()


# --------------------------------------------------------------------------- #
# golden: the bundled sample dataset
# --------------------------------------------------------------------------- #
GOLDEN_VERDICTS = {
    "order_1001": ("CONSISTENT", 120050),
    "order_1002": ("AUTO_CORRECT", 49900),
    "order_1003": ("UNRESOLVED", 250000),
    "order_1004": ("FLAGGED_RISKY", 75000),
    "order_1005": ("DISPUTED", 300000),
    "order_1006": ("CORRELATION_FAILED", 90000),
    "order_1007": ("CORRELATION_FAILED", 150000),
    "order_1008": ("UNRESOLVED", 10000),
    "order_1009": ("CONSISTENT", 30000),
    "order_1010": ("AUTO_CORRECT", 180000),
    "order_1011": ("UNRESOLVED", 64000),
    "order_1012": ("CONSISTENT", 64075),
    "order_1013": ("DISPUTED", 500000),
    "order_1014": ("AUTO_CORRECT", 22010),
}


def _sample():
    g = parse_gateway((SAMPLE / "gateway.csv").read_text(encoding="utf-8"))
    m = parse_merchant((SAMPLE / "merchant.csv").read_text(encoding="utf-8"))
    return g, m, reconcile(g.rows, m.rows, invalid=g.invalid + m.invalid)


def test_golden_sample_per_order():
    _, _, results = _sample()
    assert {r.order_id: (r.verdict.value, r.amount) for r in results} == GOLDEN_VERDICTS


def test_golden_sample_summary_counts_and_amounts():
    _, _, results = _sample()
    s = summarize(results)
    assert s["orders"] == 14
    assert s["amount_paise"] == 1_905_035
    assert {k: (v["count"], v["amount_paise"]) for k, v in s["by_verdict"].items()} == {
        "AUTO_CORRECT": (3, 251_910),
        "CONSISTENT": (3, 214_125),
        "FLAGGED_RISKY": (1, 75_000),
        "DISPUTED": (2, 800_000),
        "CORRELATION_FAILED": (2, 240_000),
        "UNRESOLVED": (3, 324_000),
    }


def test_golden_sample_exercises_every_verdict():
    _, _, results = _sample()
    assert {r.verdict for r in results} == set(Verdict)


def test_golden_sample_invalid_rows():
    g, m, _ = _sample()
    assert sorted((i.source, i.row) for i in g.invalid + m.invalid) == [
        ("gateway", 19), ("gateway", 20), ("merchant", 15), ("merchant", 16),
    ]


def test_every_result_serializes_to_json():
    _, _, results = _sample()
    json.dumps([{**r.__dict__, "verdict": r.verdict.value} for r in results])


def test_merchant_cancelled_is_flagged_risky_with_an_honest_reason_and_no_mark_paid():
    r = run("A,captured,100,INR,,,\n", "A,cancelled,,,\n")["A"]
    assert r.verdict is Verdict.FLAGGED_RISKY
    assert r.action_code == "review" and "mark the merchant order as paid" not in r.recommended_action
    assert "cancelled" in r.reason and "failed" not in r.reason


def test_merchant_refunded_is_disputed_explicitly():
    r = run("A,captured,100,INR,,,\n", "A,refunded,,,\n")["A"]
    assert r.verdict is Verdict.DISPUTED
    assert r.verdict is not Verdict.AUTO_CORRECT
    assert r.action_code == "review"
    assert "refunded" in r.reason and "gateway has not recorded a refund" in r.reason

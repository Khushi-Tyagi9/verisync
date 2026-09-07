"""Drift comparison: given a correlated per-order view, classify the
relationship between Razorpay's confirmed status and the merchant's record.

Pure. No I/O, no clients.

Direction rules (carried unchanged from the original design):
  * Razorpay confirmed success, merchant hasn't caught up   -> SAFE   (auto-correctable)
  * Razorpay failed, merchant record shows success          -> UNSAFE (flag only, never auto-correct)
  * Reversal-type activity against a settled order          -> REVERSAL (route to DISPUTED)
"""

from __future__ import annotations

from .models import DriftAssessment, DriftKind, MerchantOrderStatus, OrderView, PaymentStatus


def assess(view: OrderView) -> DriftAssessment:
    rp = view.razorpay_status
    mc = view.merchant_status

    def out(kind: DriftKind, detail: str) -> DriftAssessment:
        return DriftAssessment(kind=kind, detail=detail, razorpay_status=rp, merchant_status=mc)

    # Reversal takes precedence over any lag comparison: a refund/dispute
    # sub-entity against an order that has already settled.
    if view.reversal_ids and (view.settled or rp in (PaymentStatus.CAPTURED, PaymentStatus.REFUNDED)):
        return out(DriftKind.REVERSAL, "reversal sub-entity against a settled order")

    if rp is PaymentStatus.REFUNDED:
        return out(
            DriftKind.REVERSAL if view.settled else DriftKind.NONE,
            "razorpay reports refunded",
        )

    # Only one side has reported anything conclusive yet.
    if rp is None or mc is None:
        if rp is PaymentStatus.CAPTURED and mc is None:
            # Razorpay has confirmed success and the merchant side is silent —
            # this is exactly the "merchant lags / never updates" drift.
            return out(DriftKind.SAFE, "razorpay captured, merchant record silent")
        return out(DriftKind.INDETERMINATE, "insufficient cross-side data")

    if rp is PaymentStatus.CAPTURED:
        if mc is MerchantOrderStatus.PAID:
            return out(DriftKind.NONE, "both sides show success")
        return out(DriftKind.SAFE, f"razorpay captured, merchant shows {mc.value}")

    if rp is PaymentStatus.FAILED:
        if mc is MerchantOrderStatus.PAID:
            return out(DriftKind.UNSAFE, "merchant shows paid, razorpay shows failed")
        return out(DriftKind.NONE, f"both sides non-success (merchant {mc.value})")

    # CREATED / AUTHORIZED: not a confirmed outcome yet.
    return out(DriftKind.INDETERMINATE, f"razorpay status {rp.value} is not terminal")

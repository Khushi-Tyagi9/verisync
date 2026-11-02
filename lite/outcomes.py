"""Turn an order's state into a readable outcome tag. Pure: no I/O."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

# pointer status -> (label, tone, terminal)
_POINTER_TAGS = {
    "RESOLVED_NATURALLY": ("Resolved naturally", "good", True),
    "RESOLVED_AUTOCORRECTED": ("Auto-corrected", "good", True),
    "DISPUTED": ("Disputed", "warn", True),
    "DEAD_LETTER": ("Dead-lettered", "bad", True),
    "PENDING_RECHECK": ("Waiting for recheck", "info", False),
    "PROCESSING": ("Rechecking", "info", False),
}
_REVIEW_TAGS = {
    "unsafe_drift": ("FLAGGED_UNSAFE_DRIFT", "Flagged: unsafe drift"),
    "amount_mismatch": ("FLAGGED_AMOUNT_MISMATCH", "Flagged: amount mismatch"),
    "currency_mismatch": ("FLAGGED_CURRENCY_MISMATCH", "Flagged: currency mismatch"),
}


@dataclass(frozen=True)
class Outcome:
    tag: str        # stable machine value, e.g. RESOLVED_AUTOCORRECTED
    label: str      # human label
    tone: str       # good | warn | bad | info | quiet
    terminal: bool


def classify(pointer_status: Optional[str], review_reasons: Iterable[str]) -> Outcome:
    """`pointer_status` is orders_state.status (None when no pointer exists);
    `review_reasons` are the order's review_queue reasons."""
    if pointer_status in _POINTER_TAGS:
        label, tone, terminal = _POINTER_TAGS[pointer_status]
        return Outcome(pointer_status, label, tone, terminal)
    reasons = list(review_reasons)
    for reason in reasons:
        if reason in _REVIEW_TAGS:
            tag, label = _REVIEW_TAGS[reason]
            return Outcome(tag, label, "bad", True)
    if reasons:
        return Outcome("FLAGGED_" + reasons[0].upper(), "Flagged: " + reasons[0], "bad", True)
    return Outcome("NO_ACTION", "No action needed", "quiet", False)

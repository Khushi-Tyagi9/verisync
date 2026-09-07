"""Pure domain model for the reconciliation decision logic.

Zero I/O. Zero third-party imports. Stdlib only.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Mapping, Optional


# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #
class Source(str, Enum):
    RAZORPAY = "razorpay"
    MERCHANT = "merchant"


class PaymentStatus(str, Enum):
    """Razorpay-side payment lifecycle. CAPTURED is the only *confirmed success*."""

    CREATED = "created"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    FAILED = "failed"
    REFUNDED = "refunded"


class MerchantOrderStatus(str, Enum):
    """Merchant-side order record. PAID is the merchant's success state."""

    PENDING = "pending"
    PAID = "paid"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class SubEntityType(str, Enum):
    PAYMENT = "payment"
    REFUND = "refund"
    DISPUTE = "dispute"


class ReconStatus(str, Enum):
    """Mirrors the CHECK constraint on orders_state.status in db/schema.sql."""

    PENDING_RECHECK = "PENDING_RECHECK"
    PROCESSING = "PROCESSING"
    RESOLVED_AUTOCORRECTED = "RESOLVED_AUTOCORRECTED"
    RESOLVED_NATURALLY = "RESOLVED_NATURALLY"
    DISPUTED = "DISPUTED"
    DEAD_LETTER = "DEAD_LETTER"


TERMINAL_STATUSES: frozenset = frozenset(
    {
        ReconStatus.RESOLVED_AUTOCORRECTED,
        ReconStatus.RESOLVED_NATURALLY,
        ReconStatus.DISPUTED,
        ReconStatus.DEAD_LETTER,
    }
)


class DriftKind(str, Enum):
    NONE = "NONE"                 # sides agree (or agree on non-success)
    SAFE = "SAFE"                 # razorpay confirmed success, merchant hasn't caught up
    UNSAFE = "UNSAFE"             # merchant shows success razorpay never confirmed
    REVERSAL = "REVERSAL"         # reversal-type event against a settled order
    INDETERMINATE = "INDETERMINATE"  # not enough cross-side data yet


class Action(str, Enum):
    NO_OP = "NO_OP"
    ARM_RECHECK = "ARM_RECHECK"
    APPLY_AUTOCORRECT = "APPLY_AUTOCORRECT"
    MARK_RESOLVED_NATURALLY = "MARK_RESOLVED_NATURALLY"
    FLAG_FOR_REVIEW = "FLAG_FOR_REVIEW"
    ROUTE_DISPUTED = "ROUTE_DISPUTED"
    LOCKED_REPLAY_IGNORED = "LOCKED_REPLAY_IGNORED"


class CorrelationStatus(str, Enum):
    MATCHED = "MATCHED"
    FAILED = "FAILED"


class CorrelationFailure(str, Enum):
    MISSING_ORDER_ID = "missing_order_id"
    AMOUNT_MISMATCH = "amount_mismatch"
    CURRENCY_MISMATCH = "currency_mismatch"
    NO_MERCHANT_RECORD = "no_merchant_record"
    VALIDATION_ERROR = "validation_error"


# Reversal-type events. Against a settled order these route to DISPUTED, and
# they are the only events permitted to break the terminal lock.
REVERSAL_EVENT_TYPES: frozenset = frozenset(
    {
        "refund.created",
        "refund.processed",
        "refund.failed",
        "order.refunded",
        "payment.dispute.created",
        "payment.dispute.under_review",
        "payment.dispute.won",
        "payment.dispute.lost",
        "payment.dispute.closed",
    }
)

DEFAULT_RECHECK_DELAY = timedelta(minutes=10)


# --------------------------------------------------------------------------- #
# Value objects
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Event:
    """A normalized lifecycle event. Both Razorpay and merchant events use this
    shape once the ingestion layer has parsed them."""

    event_id: str
    source: Source
    event_type: str
    order_id: str
    occurred_at: datetime
    payment_status: Optional[PaymentStatus] = None
    merchant_status: Optional[MerchantOrderStatus] = None
    amount: Optional[int] = None          # minor units (paise)
    currency: Optional[str] = None
    sub_entity_type: Optional[SubEntityType] = None
    sub_entity_id: Optional[str] = None
    sub_entity_status: Optional[str] = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_reversal(self) -> bool:
        if self.event_type in REVERSAL_EVENT_TYPES:
            return True
        return self.sub_entity_type in (SubEntityType.REFUND, SubEntityType.DISPUTE)


@dataclass(frozen=True)
class OrderView:
    """Per-order state built up incrementally as events arrive. Immutable;
    `project()` returns a new view rather than mutating."""

    order_id: str
    razorpay_status: Optional[PaymentStatus] = None
    razorpay_amount: Optional[int] = None
    razorpay_currency: Optional[str] = None
    razorpay_last_event_at: Optional[datetime] = None
    merchant_status: Optional[MerchantOrderStatus] = None
    merchant_last_event_at: Optional[datetime] = None
    settled: bool = False
    reversal_ids: frozenset = frozenset()      # refund_id / dispute_id values seen
    seen_event_ids: frozenset = frozenset()

    @staticmethod
    def empty(order_id: str) -> "OrderView":
        return OrderView(order_id=order_id)


@dataclass(frozen=True)
class CorrelationResult:
    status: CorrelationStatus
    view: OrderView
    failure: Optional[CorrelationFailure] = None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status is CorrelationStatus.MATCHED


@dataclass(frozen=True)
class DriftAssessment:
    kind: DriftKind
    detail: str = ""
    razorpay_status: Optional[PaymentStatus] = None
    merchant_status: Optional[MerchantOrderStatus] = None


@dataclass(frozen=True)
class AuditEntry:
    """One row destined for audit_log. `event_id` is the authoritative
    idempotency key — every decision produces exactly one of these."""

    event_id: str
    order_id: Optional[str]
    source: Source
    event_type: str
    decision: str
    direction: str
    old_state: Optional[str] = None
    new_state: Optional[str] = None
    sub_entity_type: Optional[str] = None
    sub_entity_id: Optional[str] = None
    amount: Optional[int] = None
    currency: Optional[str] = None


@dataclass(frozen=True)
class ReviewItem:
    """One row destined for review_queue (genuine anomaly, needs a human)."""

    order_id: Optional[str]
    reason: str
    event_id: Optional[str] = None
    sub_entity_id: Optional[str] = None
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    """The full output of the decision logic for one event. Side-effect-free:
    it describes what the worker should do, it does not do it."""

    action: Action
    audit: AuditEntry
    reason: str
    new_status: Optional[ReconStatus] = None   # None => leave orders_state.status untouched
    recheck_at: Optional[datetime] = None      # set only for ARM_RECHECK
    clear_recheck_at: bool = False             # True when reaching a terminal resolved state
    review: Optional[ReviewItem] = None        # set for FLAG_FOR_REVIEW

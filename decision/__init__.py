"""verisync decision logic — pure, I/O-free, unit-testable without infrastructure.

Nothing in this package may import a database client, a Kafka client, an HTTP
client, or anything else that touches the network or disk. That constraint is
what lets the whole decision layer be tested in milliseconds.
"""

from .actions import decide
from .compare import assess
from .correlate import correlate, project
from .engine import Evaluation, evaluate
from .models import (
    Action,
    AuditEntry,
    CorrelationFailure,
    CorrelationResult,
    CorrelationStatus,
    Decision,
    DEFAULT_RECHECK_DELAY,
    DriftAssessment,
    DriftKind,
    Event,
    MerchantOrderStatus,
    OrderView,
    PaymentStatus,
    ReconStatus,
    REVERSAL_EVENT_TYPES,
    ReviewItem,
    Source,
    SubEntityType,
    TERMINAL_STATUSES,
)

__all__ = [
    "Action",
    "AuditEntry",
    "CorrelationFailure",
    "CorrelationResult",
    "CorrelationStatus",
    "Decision",
    "DEFAULT_RECHECK_DELAY",
    "DriftAssessment",
    "DriftKind",
    "Evaluation",
    "Event",
    "MerchantOrderStatus",
    "OrderView",
    "PaymentStatus",
    "ReconStatus",
    "REVERSAL_EVENT_TYPES",
    "ReviewItem",
    "Source",
    "SubEntityType",
    "TERMINAL_STATUSES",
    "assess",
    "correlate",
    "decide",
    "evaluate",
    "project",
]

"""Request handling for POST /api/reconcile. Pure: bytes in, (status, dict) out.

Nothing is stored and nothing is logged here. Error messages never echo the
request body; only invalid-row reasons repeat a cell value back to the caller
who sent it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .parse import MAX_ROWS, InputError, parse_gateway, parse_merchant
from .run import reconcile, summarize

MAX_BODY_BYTES = 2 * 1024 * 1024
AMOUNT_UNITS = ("rupees", "paise")
NOTICE = (
    "These are recommended actions only. verisync does not change any merchant system."
)
_SAMPLE_DIR = Path(__file__).parent / "sample"


def sample_data() -> dict:
    return {
        "gateway": (_SAMPLE_DIR / "gateway.csv").read_text(encoding="utf-8"),
        "merchant": (_SAMPLE_DIR / "merchant.csv").read_text(encoding="utf-8"),
    }


def _error(status: int, message: str, **extra: Any) -> tuple[int, dict]:
    return status, {"error": message, **extra}


def handle_reconcile(body: bytes) -> tuple[int, dict]:
    if len(body) > MAX_BODY_BYTES:
        return _error(413, f"Request body is larger than {MAX_BODY_BYTES // (1024 * 1024)} MB.")
    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return _error(422, "Request body must be JSON: {gateway, merchant, amount_unit}.")
    if not isinstance(payload, dict):
        return _error(422, "Request body must be a JSON object: {gateway, merchant, amount_unit}.")

    unit = payload.get("amount_unit", "rupees")
    if unit not in AMOUNT_UNITS:
        return _error(422, "amount_unit must be \"rupees\" or \"paise\".", field="amount_unit")
    for name in ("gateway", "merchant"):
        if not isinstance(payload.get(name), str):
            return _error(422, f"{name} must be CSV or JSON text.", field=name)

    try:
        gateway = parse_gateway(payload["gateway"], unit)
    except InputError as exc:
        return _error(422, str(exc), field="gateway")
    try:
        merchant = parse_merchant(payload["merchant"], unit)
    except InputError as exc:
        return _error(422, str(exc), field="merchant")

    invalid = gateway.invalid + merchant.invalid
    results = reconcile(gateway.rows, merchant.rows, invalid=invalid)
    return 200, {
        "notice": NOTICE,
        "amount_unit": unit,
        "limits": {"max_rows_per_file": MAX_ROWS, "max_body_bytes": MAX_BODY_BYTES},
        "input": {
            "gateway_rows": len(gateway.rows),
            "merchant_rows": len(merchant.rows),
            "invalid_rows": len(invalid),
        },
        "summary": summarize(results),
        "orders": [
            {
                "order_id": r.order_id,
                "verdict": r.verdict.value,
                "reason": r.reason,
                "gateway_status": r.gateway_status,
                "merchant_status": r.merchant_status,
                "amount": r.amount,
                "currency": r.currency,
                "recommended_action": r.recommended_action,
                "action_code": r.action_code,
                "correlation_check": r.correlation_check,
                "decision_trail": r.decision_trail,
            }
            for r in results
        ],
        "invalid_rows": [
            {"source": i.source, "row": i.row, "order_id": i.order_id, "reasons": list(i.reasons)}
            for i in invalid
        ],
    }

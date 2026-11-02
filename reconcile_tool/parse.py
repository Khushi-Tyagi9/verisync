"""Parse user-provided gateway and merchant exports. Pure: no I/O, no logging.

Accepts CSV (stdlib ``csv``) or JSON text. Headers are matched case- and
punctuation-insensitively with aliases ("Order ID", "razorpay_order_id", ...),
statuses accept common synonyms ("success" -> captured), and amounts become
integer paise using ``decimal.Decimal`` (never floats). A row that cannot be
used is reported with its row number and reasons, never dropped silently.

Row numbers: CSV rows count lines including the header (the header is row 1, the
first data row is row 2), matching what a spreadsheet shows. JSON rows are the
1-based position in the array.
"""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

MAX_ROWS = 5_000

GATEWAY_STATUSES = ("created", "authorized", "captured", "failed", "refunded")
MERCHANT_STATUSES = ("pending", "paid", "failed", "cancelled", "refunded")


class InputError(ValueError):
    """The whole input is unusable (bad format, missing columns, too many rows)."""


@dataclass(frozen=True)
class GatewayRow:
    row: int
    order_id: str
    status: str                       # a gateway status, or "refunded"/"disputed" for reversal rows
    amount: int                       # paise
    currency: str
    payment_id: Optional[str] = None
    refund_id: Optional[str] = None
    dispute_id: Optional[str] = None
    event_time: Optional[datetime] = None


@dataclass(frozen=True)
class MerchantRow:
    row: int
    order_id: str
    status: str
    amount: Optional[int] = None      # paise
    currency: Optional[str] = None
    event_time: Optional[datetime] = None


@dataclass(frozen=True)
class InvalidRow:
    source: str                       # "gateway" | "merchant"
    row: int
    reasons: tuple[str, ...]
    order_id: Optional[str] = None


@dataclass
class ParseResult:
    rows: list = field(default_factory=list)
    invalid: list[InvalidRow] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# headers
# --------------------------------------------------------------------------- #
_ALIASES = {
    "order_id": ("order_id", "orderid", "order", "order_no", "order_number", "order_ref",
                 "razorpay_order_id", "rzp_order_id", "gateway_order_id", "merchant_order_id"),
    "status": ("status", "payment_status", "order_status", "gateway_status",
               "merchant_status", "state"),
    "amount": ("amount", "amount_paid", "payment_amount", "order_total", "total", "value"),
    "currency": ("currency", "currency_code", "ccy"),
    "payment_id": ("payment_id", "razorpay_payment_id", "pay_id"),
    "refund_id": ("refund_id", "razorpay_refund_id"),
    "dispute_id": ("dispute_id", "razorpay_dispute_id"),
    "event_time": ("event_time", "event_at", "timestamp", "time", "created_at", "date"),
}


def _norm_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(key).strip().lower()).strip("_")


def _canonical_headers(keys: list[str]) -> dict[str, str]:
    """canonical name -> the actual header present, first alias wins."""
    normalized = {_norm_key(k): k for k in keys if k is not None}
    found = {}
    for canon, aliases in _ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                found[canon] = normalized[alias]
                break
    return found


# --------------------------------------------------------------------------- #
# statuses
# --------------------------------------------------------------------------- #
def _synonyms(canonical: str, *words: str) -> dict[str, str]:
    return {w: canonical for w in words}


_GATEWAY_SYNONYMS = {
    **_synonyms("captured", "captured", "capture", "success", "succeeded", "successful",
                "paid", "complete", "completed", "settled"),
    **_synonyms("authorized", "authorized", "authorised", "authorization", "auth"),
    **_synonyms("created", "created", "pending", "initiated", "new", "open", "attempted"),
    **_synonyms("failed", "failed", "fail", "failure", "declined", "error", "rejected"),
    **_synonyms("refunded", "refunded", "refund", "reversed", "partially_refunded"),
}
# Merchant "processing"/"completed" are WooCommerce-style paid states.
_MERCHANT_SYNONYMS = {
    **_synonyms("paid", "paid", "success", "succeeded", "successful", "captured",
                "complete", "completed", "processing", "fulfilled"),
    **_synonyms("pending", "pending", "pending_payment", "on_hold", "hold", "created",
                "awaiting_payment", "unpaid", "new", "open"),
    **_synonyms("failed", "failed", "fail", "failure", "declined", "error"),
    **_synonyms("cancelled", "cancelled", "canceled", "cancel", "void", "voided"),
    **_synonyms("refunded", "refunded", "refund", "reversed"),
}


def _status(raw: Any, table: dict[str, str]) -> Optional[str]:
    return table.get(_norm_key(raw)) if raw is not None else None


# --------------------------------------------------------------------------- #
# amounts, currency, time
# --------------------------------------------------------------------------- #
_THOUSANDS = re.compile(r"^\d{1,3}(,\d{3})+(\.\d+)?$")
_PLAIN = re.compile(r"^\d+(\.\d+)?$")
_NOISE = re.compile(r"^(?:inr|rs\.?|usd|eur|gbp|₹|\$|€|£)\s*|\s*(?:inr|rs\.?)$", re.IGNORECASE)


def to_paise(value: Any, unit: str) -> int:
    """Exact conversion to integer paise. Raises ValueError with a reason."""
    if isinstance(value, bool):
        raise ValueError("amount must be a number")
    if isinstance(value, (int, Decimal)):
        dec = Decimal(value)
    elif isinstance(value, float):
        # Only reachable if a caller bypassed parse_float; use its shortest repr.
        dec = Decimal(repr(value))
    else:
        text = _NOISE.sub("", str(value).strip()).strip()
        text = text.replace(" ", "").replace(" ", "")
        if _THOUSANDS.match(text):
            text = text.replace(",", "")
        if not _PLAIN.match(text):
            raise ValueError(f"amount {str(value).strip()!r} is not a plain number")
        try:
            dec = Decimal(text)
        except InvalidOperation:
            raise ValueError(f"amount {str(value).strip()!r} is not a number")
    if not dec.is_finite():
        raise ValueError("amount must be a finite number")
    if dec < 0:
        raise ValueError("amount must not be negative")
    paise = dec * 100 if unit == "rupees" else dec
    if paise != paise.to_integral_value():
        if unit == "rupees":
            raise ValueError(f"amount {dec} has more than 2 decimal places (cannot be exact paise)")
        raise ValueError(f"amount {dec} is not a whole number of paise")
    return int(paise)


_CURRENCY = re.compile(r"^[A-Za-z]{3}$")


def _currency(raw: Any) -> str:
    text = str(raw).strip()
    if not _CURRENCY.match(text):
        raise ValueError(f"currency {text!r} is not a 3-letter code")
    return text.upper()


def _event_time(raw: Any) -> datetime:
    if isinstance(raw, (int, Decimal)) and not isinstance(raw, bool):
        return _epoch(Decimal(raw))
    text = str(raw).strip()
    if re.fullmatch(r"\d+(\.\d+)?", text):
        return _epoch(Decimal(text))
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        raise ValueError(f"event_time {text!r} is not an ISO 8601 time or epoch seconds")
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _epoch(value: Decimal) -> datetime:
    seconds = value / 1000 if value > Decimal("100000000000") else value
    try:
        return datetime.fromtimestamp(float(seconds), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise ValueError("event_time is out of range")


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _text(v: Any) -> Optional[str]:
    return None if _blank(v) else str(v).strip()


# --------------------------------------------------------------------------- #
# reading text into raw rows
# --------------------------------------------------------------------------- #
def _raw_rows(text: str, source: str) -> tuple[list[tuple[int, dict]], list[str]]:
    """Return ([(row_number, {header: value})...], headers)."""
    if not isinstance(text, str):
        raise InputError(f"{source} data must be text")
    text = text.lstrip("﻿")
    stripped = text.lstrip()
    if not stripped:
        raise InputError(f"{source} data is empty")

    if stripped[0] in "[{":
        try:
            data = json.loads(stripped, parse_float=Decimal, parse_int=int)
        except json.JSONDecodeError as exc:
            raise InputError(f"{source} JSON is not valid: {exc.msg} (line {exc.lineno})")
        if isinstance(data, dict):
            for key in ("rows", "data", "orders", "payments", "items", "records"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
            else:
                data = [data]
        if not isinstance(data, list):
            raise InputError(f"{source} JSON must be an array of objects")
        rows, headers = [], []
        for i, item in enumerate(data, start=1):
            if len(rows) >= MAX_ROWS + 1:
                break
            if isinstance(item, dict):
                for k in item:
                    if k not in headers:
                        headers.append(k)
                rows.append((i, item))
            else:
                rows.append((i, {"__invalid__": item}))
        return rows, headers

    first_line = stripped.splitlines()[0]
    delimiter = max((",", ";", "\t", "|"), key=first_line.count)
    if first_line.count(delimiter) == 0:
        delimiter = ","
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter)
    headers: list[str] = []
    rows = []
    for line_no, record in enumerate(reader, start=1):
        if line_no == 1 or not headers:
            if any(c.strip() for c in record):
                headers = [c.strip() for c in record]
            continue
        if not any(c.strip() for c in record):
            continue
        if len(rows) >= MAX_ROWS + 1:
            break
        rec = dict(zip(headers, record + [""] * (len(headers) - len(record))))
        rows.append((reader.line_num, rec))
    if not headers:
        raise InputError(f"{source} data has no header row")
    return rows, headers


# --------------------------------------------------------------------------- #
# gateway / merchant
# --------------------------------------------------------------------------- #
def _require(headers: list[str], mapping: dict[str, str], needed: tuple[str, ...], source: str) -> None:
    missing = [c for c in needed if c not in mapping]
    if missing:
        raise InputError(
            f"{source} data is missing required column(s): {', '.join(missing)}. "
            f"Found columns: {', '.join(str(h) for h in headers) or 'none'}"
        )


def parse_gateway(text: str, amount_unit: str = "rupees") -> ParseResult:
    raw_rows, headers = _raw_rows(text, "gateway")
    if len(raw_rows) > MAX_ROWS:
        raise InputError(f"gateway data has more than {MAX_ROWS} rows")
    cols = _canonical_headers(headers)
    _require(headers, cols, ("order_id", "status", "amount"), "gateway")
    out = ParseResult()
    for number, rec in raw_rows:
        reasons: list[str] = []
        if "__invalid__" in rec:
            out.invalid.append(InvalidRow("gateway", number, ("row is not an object",)))
            continue

        def get(canon):
            return rec.get(cols[canon]) if canon in cols else None

        order_id = _text(get("order_id"))
        if not order_id:
            reasons.append("order_id is missing")
        refund_id, dispute_id = _text(get("refund_id")), _text(get("dispute_id"))
        reversal = bool(refund_id or dispute_id)

        status = None
        raw_status = get("status")
        if reversal:
            status = "disputed" if dispute_id and not refund_id else "refunded"
        elif _blank(raw_status):
            reasons.append("status is missing")
        else:
            status = _status(raw_status, _GATEWAY_SYNONYMS)
            if status is None:
                reasons.append(
                    f"status {str(raw_status).strip()!r} is not recognized "
                    f"(expected one of {', '.join(GATEWAY_STATUSES)} or a common synonym)"
                )

        amount = None
        if _blank(get("amount")):
            reasons.append("amount is missing")
        else:
            try:
                amount = to_paise(get("amount"), amount_unit)
            except ValueError as exc:
                reasons.append(str(exc))

        currency = "INR"
        if "currency" in cols and not _blank(get("currency")):
            try:
                currency = _currency(get("currency"))
            except ValueError as exc:
                reasons.append(str(exc))

        event_time = None
        if "event_time" in cols and not _blank(get("event_time")):
            try:
                event_time = _event_time(get("event_time"))
            except ValueError as exc:
                reasons.append(str(exc))

        if reasons:
            out.invalid.append(InvalidRow("gateway", number, tuple(reasons), order_id))
            continue
        out.rows.append(
            GatewayRow(number, order_id, status, amount, currency,
                       _text(get("payment_id")), refund_id, dispute_id, event_time)
        )
    return out


def parse_merchant(text: str, amount_unit: str = "rupees") -> ParseResult:
    raw_rows, headers = _raw_rows(text, "merchant")
    if len(raw_rows) > MAX_ROWS:
        raise InputError(f"merchant data has more than {MAX_ROWS} rows")
    cols = _canonical_headers(headers)
    _require(headers, cols, ("order_id", "status"), "merchant")
    out = ParseResult()
    for number, rec in raw_rows:
        reasons: list[str] = []
        if "__invalid__" in rec:
            out.invalid.append(InvalidRow("merchant", number, ("row is not an object",)))
            continue

        def get(canon):
            return rec.get(cols[canon]) if canon in cols else None

        order_id = _text(get("order_id"))
        if not order_id:
            reasons.append("order_id is missing")

        status = None
        if _blank(get("status")):
            reasons.append("status is missing")
        else:
            status = _status(get("status"), _MERCHANT_SYNONYMS)
            if status is None:
                reasons.append(
                    f"status {str(get('status')).strip()!r} is not recognized "
                    f"(expected one of {', '.join(MERCHANT_STATUSES)} or a common synonym)"
                )

        amount = None
        if "amount" in cols and not _blank(get("amount")):
            try:
                amount = to_paise(get("amount"), amount_unit)
            except ValueError as exc:
                reasons.append(str(exc))

        currency = None
        if "currency" in cols and not _blank(get("currency")):
            try:
                currency = _currency(get("currency"))
            except ValueError as exc:
                reasons.append(str(exc))

        event_time = None
        if "event_time" in cols and not _blank(get("event_time")):
            try:
                event_time = _event_time(get("event_time"))
            except ValueError as exc:
                reasons.append(str(exc))

        if reasons:
            out.invalid.append(InvalidRow("merchant", number, tuple(reasons), order_id))
            continue
        out.rows.append(MerchantRow(number, order_id, status, amount, currency, event_time))
    return out

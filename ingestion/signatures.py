"""Razorpay webhook signature verification.

Pure, stdlib only. The one rule that matters: the HMAC is computed over the
exact raw bytes Razorpay sent. Never parse-then-reserialize before verifying.
"""

from __future__ import annotations

import hashlib
import hmac


def compute_signature(raw_body: bytes, secret: str) -> str:
    """HMAC-SHA256(raw_body, secret) as a lowercase hex digest."""
    return hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()


def verify_signature(raw_body: bytes, provided_signature: str, secret: str) -> bool:
    """Constant-time comparison of the expected signature against the header."""
    if not provided_signature or not secret:
        return False
    expected = compute_signature(raw_body, secret)
    return hmac.compare_digest(expected, provided_signature.strip())

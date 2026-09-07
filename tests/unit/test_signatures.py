from __future__ import annotations

from ingestion.signatures import compute_signature, verify_signature

SECRET = "local_dev_webhook_secret"
BODY = b'{"event":"payment.captured","payload":{}}'


def test_compute_signature_is_deterministic_hex_sha256():
    a = compute_signature(BODY, SECRET)
    b = compute_signature(BODY, SECRET)
    assert a == b
    assert len(a) == 64
    assert int(a, 16) >= 0  # valid hex


def test_verify_accepts_a_correct_signature():
    sig = compute_signature(BODY, SECRET)
    assert verify_signature(BODY, sig, SECRET) is True


def test_verify_tolerates_surrounding_whitespace_in_header():
    sig = compute_signature(BODY, SECRET)
    assert verify_signature(BODY, f"  {sig}\n", SECRET) is True


def test_verify_rejects_wrong_secret():
    sig = compute_signature(BODY, "some_other_secret")
    assert verify_signature(BODY, sig, SECRET) is False


def test_verify_rejects_tampered_body():
    sig = compute_signature(BODY, SECRET)
    assert verify_signature(BODY + b" ", sig, SECRET) is False


def test_verify_rejects_empty_or_missing_signature():
    assert verify_signature(BODY, "", SECRET) is False
    assert verify_signature(BODY, "   ", SECRET) is False


def test_verify_rejects_when_secret_is_empty():
    assert verify_signature(BODY, compute_signature(BODY, ""), "") is False

"""Hand-construct a signed Razorpay-style webhook and POST it to the local
ingestion endpoint. Serves the Phase 3 checkpoint.

Examples:
    python scripts/send_test_webhook.py                     # signed, in-scope -> 202
    python scripts/send_test_webhook.py --bad-sig           # mis-signed      -> 401
    python scripts/send_test_webhook.py --no-order-id       # out of scope    -> 200
    python scripts/send_test_webhook.py --event payment.failed --status failed
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.request
import uuid

from dotenv import load_dotenv


def build_body(*, event: str, status: str, order_id: str | None, amount: int) -> dict:
    payment_entity = {
        "id": f"pay_{uuid.uuid4().hex[:14]}",
        "entity": "payment",
        "amount": amount,
        "currency": "INR",
        "status": status,
        "order_id": order_id,
        "method": "upi",
    }
    return {
        "entity": "event",
        "account_id": "acc_LOCALTEST",
        "event": event,
        "contains": ["payment"],
        "payload": {"payment": {"entity": payment_entity}},
        "created_at": int(time.time()),
    }


def main() -> int:
    load_dotenv(".env")
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000/webhooks/razorpay")
    parser.add_argument("--event", default="payment.captured")
    parser.add_argument("--status", default="captured")
    parser.add_argument("--amount", type=int, default=50000)
    parser.add_argument("--order-id", default=f"order_{uuid.uuid4().hex[:14]}")
    parser.add_argument("--no-order-id", action="store_true", help="omit order_id (out of scope)")
    parser.add_argument("--bad-sig", action="store_true", help="send a deliberately wrong signature")
    parser.add_argument("--secret", default=os.environ.get("RAZORPAY_WEBHOOK_SECRET"))
    args = parser.parse_args()

    if not args.secret:
        parser.error("no webhook secret: set RAZORPAY_WEBHOOK_SECRET in .env or pass --secret")

    order_id = None if args.no_order_id else args.order_id
    body = build_body(event=args.event, status=args.status, order_id=order_id, amount=args.amount)
    raw = json.dumps(body).encode("utf-8")

    signature = hmac.new(args.secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    if args.bad_sig:
        signature = "0" * len(signature)

    req = urllib.request.Request(
        args.url,
        data=raw,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Razorpay-Signature": signature,
            "X-Razorpay-Event-Id": f"evt_{uuid.uuid4().hex[:16]}",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(resp.status, resp.read().decode())
            return 0
    except urllib.error.HTTPError as exc:
        print(exc.code, exc.read().decode())
        return 0 if args.bad_sig and exc.code == 401 else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Kill the recheck worker mid-claim, run the sweep, and check the order is
recovered exactly once.

Needs the compose stack up (Postgres on the host port from .env, Redis too)
and the repo venv:

    .venv/Scripts/python.exe scripts/chaos_kill_test.py

What it does:
  1. Inserts a fresh order (id prefix order_CHAOS) as PENDING_RECHECK with a
     view that has a clear terminal outcome (Razorpay captured, merchant
     still pending).
  2. Spawns a child process that claims it via RecheckRunner.resolve_order and
     dies with os._exit(137) right after the claim commits, before completion.
  3. Asserts the row is stuck in PROCESSING holding the dead claim_token.
  4. Runs the sweep (stuck timeout and grace forced to 0 so no waiting), twice.
  5. Asserts: exactly one terminal audit row for the order, the row is in a
     terminal status, and no orders_state row holds the dead claim_token.

The order and its audit rows stay in the database afterwards (audit_log is
append-only), so the dashboard's order count goes up by one per run.

Exit codes: 0 all assertions held, 1 an assertion failed, 2 setup failure.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

TERMINAL_DECISIONS = ("APPLY_AUTOCORRECT", "MARK_RESOLVED_NATURALLY", "ROUTE_DISPUTED")
TERMINAL_STATUSES = ("RESOLVED_AUTOCORRECTED", "RESOLVED_NATURALLY", "DISPUTED")


def run_child(order_id: str) -> None:
    """Claim the order, then die before the claim is completed."""
    from worker.recheck import RecheckRunner

    runner = RecheckRunner.from_settings()

    def die_after_claim(*_a, **_kw):
        # Reached only once claim_recheck has committed PROCESSING + token.
        os._exit(137)

    runner._store.read_state = die_after_claim
    runner.resolve_order(order_id, datetime.now(timezone.utc))
    # Surviving to here means the kill hook never fired.
    sys.exit(3)


def seed_order(conn, order_id: str) -> None:
    from decision.models import MerchantOrderStatus, OrderView, PaymentStatus
    from psycopg.types.json import Json
    from worker.viewcodec import serialize_view

    now = datetime.now(timezone.utc)
    view = OrderView(
        order_id=order_id,
        razorpay_status=PaymentStatus.CAPTURED,
        razorpay_amount=50_000,
        razorpay_currency="INR",
        razorpay_last_event_at=now - timedelta(minutes=20),
        merchant_status=MerchantOrderStatus.PENDING,
        merchant_last_event_at=now - timedelta(minutes=19),
    )
    conn.execute(
        "INSERT INTO order_views (order_id, view) VALUES (%s, %s)",
        (order_id, Json(serialize_view(view))),
    )
    conn.execute(
        "INSERT INTO orders_state (order_id, status, recheck_at) "
        "VALUES (%s, 'PENDING_RECHECK', %s)",
        (order_id, now - timedelta(minutes=10)),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--child", metavar="ORDER_ID", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.child:
        run_child(args.child)
        return 3  # unreachable

    import psycopg

    from worker.config import load_settings
    from worker.sweep import SweepService

    settings = load_settings()
    order_id = f"order_CHAOS{uuid.uuid4().hex[:12]}"
    failures: list[str] = []

    def check(ok: bool, msg: str) -> None:
        print(f"  [{'ok' if ok else 'FAIL'}] {msg}")
        if not ok:
            failures.append(msg)

    try:
        conn = psycopg.connect(settings.postgres_dsn, autocommit=True)
    except Exception as exc:
        print(f"cannot reach postgres: {exc}", file=sys.stderr)
        return 2

    with conn:
        seed_order(conn, order_id)
        print(f"seeded {order_id} as PENDING_RECHECK")

        child = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--child", order_id],
            cwd=str(Path(__file__).resolve().parents[1]),
        )
        check(child.returncode == 137, f"worker died mid-claim (exit {child.returncode}, want 137)")

        status, token = conn.execute(
            "SELECT status, claim_token FROM orders_state WHERE order_id = %s", (order_id,)
        ).fetchone()
        check(status == "PROCESSING" and token is not None,
              f"row left in PROCESSING with a claim_token (got {status}, token={'set' if token else 'NULL'})")
        dead_token = token

        sweep_settings = dataclasses.replace(
            settings, stuck_processing_timeout_seconds=0, pending_sweep_grace_seconds=0
        )
        service = SweepService.from_settings(sweep_settings)
        try:
            first = service.run_once()
            second = service.run_once()
        finally:
            service.close()
        print(f"  sweep pass 1 (reclaimed, resolved) = {first}; pass 2 = {second}")

        terminal_rows = conn.execute(
            "SELECT decision FROM audit_log WHERE order_id = %s AND decision = ANY(%s)",
            (order_id, list(TERMINAL_DECISIONS)),
        ).fetchall()
        check(len(terminal_rows) == 1,
              f"exactly one terminal audit action (got {[r[0] for r in terminal_rows]})")

        final = conn.execute(
            "SELECT status FROM orders_state WHERE order_id = %s", (order_id,)
        ).fetchone()[0]
        check(final in TERMINAL_STATUSES, f"order ended in a terminal status (got {final})")

        if dead_token is not None:
            holders = conn.execute(
                "SELECT order_id FROM orders_state WHERE claim_token = %s", (dead_token,)
            ).fetchall()
            check(not holders, f"no row still holds the dead claim_token (holders: {holders})")

    if failures:
        print(f"\nFAIL: {len(failures)} assertion(s) failed for {order_id}")
        return 1
    print(f"\nOK: {order_id} recovered with exactly one terminal action")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Seed N synthetic orders, wait for every one to reach a terminal state, print
a counts table. Exits non-zero if any order is unaccounted for.

Needs the compose stack up (`docker compose up -d --build`) and the repo venv
(host deps: confluent-kafka, psycopg), talking to the host ports from .env:

    .venv/Scripts/python.exe scripts/run_batch.py --count 150

By default a stuck or silent order takes about 30 minutes to dead-letter (10
minute recheck window, 3 attempts). To finish a batch in a few minutes, start
the stack with short timings first:

    WORKER_RECHECK_DELAY_SECONDS=20 WORKER_SWEEP_INTERVAL_SECONDS=20 \\
    MERCHANT_SIM_LAG_SECONDS=40 docker compose up -d

"Terminal" means the order has an orders_state row in RESOLVED_AUTOCORRECTED,
RESOLVED_NATURALLY, DISPUTED or DEAD_LETTER, or has an order_views row and no
orders_state row (processed, no reconciliation action needed). Anything still
PENDING_RECHECK / PROCESSING, or never seen by the worker at all, is
unaccounted for when --timeout expires.

Exit codes: 0 all accounted for, 1 some unaccounted for, 2 setup failure.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

TERMINAL = ("RESOLVED_AUTOCORRECTED", "RESOLVED_NATURALLY", "DISPUTED", "DEAD_LETTER")
IN_PROGRESS = ("PENDING_RECHECK", "PROCESSING")
NO_ACTION = "NO_ACTION_NEEDED"
NOT_SEEN = "NOT_SEEN_BY_WORKER"

_CLASSIFY = """
    SELECT i.order_id, s.status, v.order_id IS NOT NULL
    FROM unnest(%s::text[]) AS i(order_id)
    LEFT JOIN orders_state s ON s.order_id = i.order_id
    LEFT JOIN order_views v ON v.order_id = i.order_id
"""


def classify(conn, order_ids: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for order_id, status, has_view in conn.execute(_CLASSIFY, (order_ids,)).fetchall():
        if status is not None:
            out[order_id] = status
        else:
            out[order_id] = NO_ACTION if has_view else NOT_SEEN
    return out


def is_terminal(label: str) -> bool:
    return label in TERMINAL or label == NO_ACTION


def format_table(counts: Counter, total: int) -> str:
    rows = [*TERMINAL, NO_ACTION, *IN_PROGRESS, NOT_SEEN]
    width = max(len(r) for r in rows)
    lines = [f"{'state'.ljust(width)}  count", f"{'-' * width}  -----"]
    for r in rows:
        lines.append(f"{r.ljust(width)}  {counts.get(r, 0):>5}")
    lines.append(f"{'-' * width}  -----")
    lines.append(f"{'total'.ljust(width)}  {sum(counts.values()):>5}  (seeded {total})")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--count", type=int, default=150, help="orders to seed (default 150)")
    parser.add_argument("--timeout", type=float, default=2400, help="seconds to wait (default 2400)")
    parser.add_argument("--poll", type=float, default=5, help="poll interval in seconds")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    import psycopg

    from merchant_sim.config import load_settings
    from merchant_sim.simulator import emit_synthetic_captures

    settings = load_settings()
    try:
        order_ids = emit_synthetic_captures(settings, args.count)
    except Exception as exc:
        print(f"could not seed orders: {exc}", file=sys.stderr)
        return 2
    print(f"seeded {len(order_ids)} orders; waiting up to {args.timeout:.0f}s")

    deadline = time.monotonic() + args.timeout
    last_pending = -1
    with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
        while True:
            labels = classify(conn, order_ids)
            counts = Counter(labels.values())
            pending = sum(1 for v in labels.values() if not is_terminal(v))
            if pending != last_pending:
                print(f"  {len(order_ids) - pending}/{len(order_ids)} terminal")
                last_pending = pending
            if pending == 0 or time.monotonic() >= deadline:
                break
            time.sleep(args.poll)

    print()
    print(format_table(counts, len(order_ids)))
    if pending:
        unaccounted = sorted(o for o, v in labels.items() if not is_terminal(v))
        print(f"\nFAIL: {pending} order(s) unaccounted for, e.g. {unaccounted[:5]}")
        return 1
    print(f"\nOK: all {len(order_ids)} orders accounted for")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

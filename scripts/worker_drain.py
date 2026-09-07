"""Run the reconciliation worker until the topic is drained, then exit cleanly.

For the Phase 4 checkpoints (feed an event by hand, then inspect Postgres) and
for local iteration. Not a production entrypoint -- that is ``python -m
worker.consumer``, which runs forever.

    python scripts/worker_drain.py
    python scripts/worker_drain.py --idle 2 --max 20
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from worker.consumer import ReconciliationWorker  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--idle", type=float, default=3.0, help="stop after N idle seconds")
    parser.add_argument("--max", type=float, default=30.0, help="hard cap on total run seconds")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )

    worker = ReconciliationWorker.from_settings()
    count = worker.run_until_idle(idle_seconds=args.idle, max_seconds=args.max)
    logging.getLogger("verisync.worker").info("drained %d message(s)", count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run every lite scenario through the Kafka pipeline and through lite mode and
compare the terminal outcome of each. Exits non-zero on any disagreement or any
order that never finishes.

Needs the compose stack up with SHORT timings (the pipeline waits on its own
recheck window and the silent scenario needs three of them), and the repo venv:

    WORKER_RECHECK_DELAY_SECONDS=15 WORKER_SWEEP_INTERVAL_SECONDS=15 \\
    MERCHANT_SIM_LAG_SECONDS=30 docker compose up -d --build
    .venv/Scripts/python.exe scripts/parity_check.py

Kafka side: for each scenario it inserts the merchant_orders row (so the
simulator leaves the order alone), then publishes the scenario's gateway and
merchant envelopes to the topic. The paid transitions for the normal and
lagging scenarios are fired by the running merchant-sim service. Orders are
named order_PARITY_<scenario>_<hex> in the compose database.

Lite side: the same scenarios through lite.service in a throwaway database
created next to the compose one (schema.sql applied, dropped afterwards), so
the running sweep and worker services never touch it.

--lag-seconds must match the stack's MERCHANT_SIM_LAG_SECONDS (default 30).
Exit codes: 0 all agree, 1 disagreement or timeout, 2 setup failure.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "order_PARITY_"

_OUTCOME_SQL = """
    SELECT i.order_id, s.status,
           COALESCE((SELECT array_agg(r.reason ORDER BY r.id)
                     FROM review_queue r WHERE r.order_id = i.order_id), '{}')
    FROM unnest(%s::text[]) AS i(order_id)
    LEFT JOIN orders_state s ON s.order_id = i.order_id
"""


def fetch_outcomes(conn, order_ids):
    from lite.outcomes import classify

    rows = conn.execute(_OUTCOME_SQL, (list(order_ids),)).fetchall()
    return {oid: classify(status, reasons or []) for oid, status, reasons in rows}


def wait_terminal(conn, ids, timeout, label):
    deadline = time.monotonic() + timeout
    last = -1
    while True:
        outcomes = fetch_outcomes(conn, ids.values())
        done = sum(1 for o in outcomes.values() if o.terminal)
        if done != last:
            print(f"  [{label}] {done}/{len(ids)} terminal")
            last = done
        if done == len(ids) or time.monotonic() > deadline:
            return {k: outcomes[oid] for k, oid in ids.items()}
        time.sleep(2)


def run_kafka(args):
    import psycopg

    from lite.scenarios import SCENARIOS, build_plan, new_order_id
    from merchant_sim.config import load_settings
    from merchant_sim.store import MerchantOrderStore
    from ingestion.publisher import KafkaLifecyclePublisher

    settings = load_settings()
    publisher = KafkaLifecyclePublisher.from_settings(settings)
    ids = {}
    try:
        with psycopg.connect(settings.postgres_dsn, autocommit=True) as conn:
            store = MerchantOrderStore(None)
            now = datetime.now(timezone.utc)
            for key in SCENARIOS:
                oid = new_order_id(key, prefix=PREFIX)
                plan = build_plan(key, oid, 50_000, now=now,
                                  immediate_seconds=settings.immediate_seconds,
                                  lag_seconds=args.lag_seconds)
                store.create_if_absent(conn, order_id=oid, amount=50_000, currency="INR",
                                       profile=plan.profile, next_action_at=plan.next_action_at)
                if plan.paid_now:
                    store.mark_paid(conn, oid)
                for env in plan.envelopes:
                    publisher.publish(order_id=oid, envelope=env)
                ids[key] = oid
            print(f"published {len(ids)} scenario orders to Kafka")
            return wait_terminal(conn, ids, args.timeout, "kafka")
    finally:
        publisher.close()


def run_lite(args):
    import psycopg
    from psycopg_pool import ConnectionPool

    from lite.config import LiteSettings
    from lite.scenarios import SCENARIOS
    from lite.service import build_service
    from merchant_sim.config import load_settings
    from worker.config import load_settings as load_worker_settings

    base_dsn = load_settings().postgres_dsn
    head, _, _ = base_dsn.rpartition("/")
    name = f"verisync_parity_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(f"{head}/postgres", autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = f"{head}/{name}"
    pool = None
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
        worker = dataclasses.replace(
            load_worker_settings(env_file=None), postgres_dsn=dsn,
            recheck_delay_seconds=2, pending_sweep_grace_seconds=0, max_reclaim_retries=3,
        )
        pool = ConnectionPool(dsn, min_size=1, max_size=6, open=True,
                              kwargs={"prepare_threshold": None})
        service = build_service(pool, LiteSettings(worker=worker, immediate_seconds=1, lag_seconds=6))
        ids = {k: service.submit_orders(amount=50_000, scenario=k, count=1)[0] for k in SCENARIOS}
        print(f"submitted {len(ids)} scenario orders to lite mode")
        deadline = time.monotonic() + args.timeout
        with psycopg.connect(dsn, autocommit=True) as conn:
            while True:
                service.tick()
                outcomes = fetch_outcomes(conn, ids.values())
                if all(o.terminal for o in outcomes.values()) or time.monotonic() > deadline:
                    return {k: outcomes[oid] for k, oid in ids.items()}
                time.sleep(0.5)
    finally:
        if pool is not None:
            pool.close()
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--timeout", type=float, default=300, help="seconds per mode")
    parser.add_argument("--lag-seconds", type=int, default=30)
    args = parser.parse_args()

    from lite.scenarios import SCENARIOS

    try:
        kafka = run_kafka(args)
        lite = run_lite(args)
    except Exception as exc:
        print(f"setup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    width = max(len(k) for k in SCENARIOS)
    print(f"\n{'scenario'.ljust(width)}  {'expected':26} {'kafka':26} {'lite':26} result")
    bad = 0
    for key, spec in SCENARIOS.items():
        k, l = kafka[key], lite[key]
        agree = k.terminal and l.terminal and k.tag == l.tag == spec.expected_tag
        bad += 0 if agree else 1
        print(f"{key.ljust(width)}  {spec.expected_tag:26} {k.tag:26} {l.tag:26} "
              f"{'ok' if agree else 'DISAGREE' if k.tag != l.tag else 'MISMATCH-EXPECTED'}")
    if bad:
        print(f"\nFAIL: {bad} scenario(s) differ")
        return 1
    print("\nOK: kafka and lite agree on every scenario, and match the decision table")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Lite mode against a real Postgres: every scenario end to end.

Uses the compose Postgres (host port 15432 by default; override with
LITE_TEST_ADMIN_DSN). Each run creates a throwaway database, applies
db/schema.sql into it and drops it afterwards, so the running pipeline's data
and its sweep service are never involved. Skipped when Postgres is not
reachable.

    docker compose up -d postgres
    pytest tests/integration -q
"""

from __future__ import annotations

import dataclasses
import os
import threading
import time
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg_pool import ConnectionPool  # noqa: E402

from lite.config import LiteSettings  # noqa: E402
from lite.scenarios import SCENARIOS, build_plan, new_order_id  # noqa: E402
from lite.service import TICK_LOCK_KEY, CapExceeded, build_service  # noqa: E402
from worker.config import load_settings as load_worker_settings  # noqa: E402

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
ADMIN_DSN = os.environ.get(
    "LITE_TEST_ADMIN_DSN", "postgresql://verisync:verisync@localhost:15432/postgres"
)


def _with_db(dsn: str, name: str) -> str:
    head, _, _ = dsn.rpartition("/")
    return f"{head}/{name}"


@pytest.fixture(scope="module")
def dsn():
    name = f"verisync_lite_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True, connect_timeout=3)
    except Exception as exc:  # no database in this environment
        pytest.skip(f"postgres not reachable: {exc}")
    admin.execute(f'CREATE DATABASE "{name}"')
    test_dsn = _with_db(ADMIN_DSN, name)
    with psycopg.connect(test_dsn, autocommit=True) as conn:
        conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
    try:
        yield test_dsn
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


def _settings(dsn: str, **over) -> LiteSettings:
    worker = dataclasses.replace(
        load_worker_settings(env_file=None),
        postgres_dsn=dsn,
        recheck_delay_seconds=1,
        pending_sweep_grace_seconds=0,
        max_reclaim_retries=3,
    )
    base = dict(worker=worker, immediate_seconds=1, lag_seconds=4, tick_limit=20, max_live_orders=500)
    base.update(over)
    return LiteSettings(**base)


@pytest.fixture(scope="module")
def pool(dsn):
    p = ConnectionPool(dsn, min_size=1, max_size=6, open=True, timeout=10,
                       kwargs={"prepare_threshold": None})
    yield p
    p.close()


@pytest.fixture(scope="module")
def service(dsn, pool):
    return build_service(pool, _settings(dsn))


def _tick_until(service, ids, *, timeout=45.0):
    """Tick until every id is terminal; return {order_id: order dict}."""
    deadline = time.monotonic() + timeout
    while True:
        service.tick()
        orders = {o["order_id"]: o for o in service.recent_orders(limit=200)}
        if all(orders[i]["outcome"]["terminal"] for i in ids):
            return orders
        if time.monotonic() > deadline:
            stuck = {i: orders[i]["outcome"]["tag"] for i in ids if not orders[i]["outcome"]["terminal"]}
            pytest.fail(f"not terminal after {timeout}s: {stuck}")
        time.sleep(0.3)


def test_every_scenario_ends_where_the_decision_table_says(service):
    ids = {key: service.submit_orders(amount=50_000, scenario=key, count=1)[0] for key in SCENARIOS}
    orders = _tick_until(service, list(ids.values()))
    actual = {key: orders[oid]["outcome"]["tag"] for key, oid in ids.items()}
    expected = {key: spec.expected_tag for key, spec in SCENARIOS.items()}
    assert actual == expected
    for key, oid in ids.items():
        assert orders[oid]["scenario"] == key


def test_late_merchant_paid_event_is_ignored_after_autocorrect(service, pool):
    oid = service.submit_orders(amount=50_000, scenario="lagging", count=1)[0]
    _tick_until(service, [oid])
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        service.tick()
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT status FROM merchant_orders WHERE order_id = %s", (oid,)
            ).fetchone()
        if row[0] == "paid":
            break
        time.sleep(0.3)
    with pool.connection() as conn:
        decisions = [r[0] for r in conn.execute(
            "SELECT decision FROM audit_log WHERE order_id = %s ORDER BY id", (oid,)
        ).fetchall()]
        status = conn.execute(
            "SELECT status FROM orders_state WHERE order_id = %s", (oid,)
        ).fetchone()[0]
    assert status == "RESOLVED_AUTOCORRECTED"
    assert decisions[-1] == "LOCKED_REPLAY_IGNORED"
    assert decisions.count("APPLY_AUTOCORRECT") == 1


def test_silent_order_is_dead_lettered_with_a_review_row(service, pool):
    oid = service.submit_orders(amount=50_000, scenario="silent", count=1)[0]
    _tick_until(service, [oid])
    with pool.connection() as conn:
        reasons = [r[0] for r in conn.execute(
            "SELECT reason FROM review_queue WHERE order_id = %s", (oid,)
        ).fetchall()]
    assert "dead_letter" in reasons


def test_processing_the_same_envelope_twice_is_absorbed_by_the_unique_constraint(service, pool):
    processor = service._processor
    oid = new_order_id("stuck")
    from datetime import datetime, timezone

    plan = build_plan("stuck", oid, 50_000, now=datetime.now(timezone.utc),
                      immediate_seconds=1, lag_seconds=4)
    first = processor.process_event(plan.envelopes[0])
    again = processor.process_event(plan.envelopes[0])
    assert first.result.audit_written is True
    assert again.result.audit_written is False
    with pool.connection() as conn:
        n = conn.execute("SELECT count(*) FROM audit_log WHERE order_id = %s", (oid,)).fetchone()[0]
    assert n == 1


def test_tick_does_nothing_while_another_session_holds_the_lock(service, dsn):
    holder = psycopg.connect(dsn)  # a transaction stays open until commit/close
    try:
        assert holder.execute("SELECT pg_try_advisory_xact_lock(%s)", (TICK_LOCK_KEY,)).fetchone()[0]
        assert service.tick().ran is False
    finally:
        holder.rollback()
        holder.close()
    assert service.tick().ran is True


def test_concurrent_ticks_never_both_run(service, dsn):
    holder = psycopg.connect(dsn)
    results = []
    try:
        holder.execute("SELECT pg_try_advisory_xact_lock(%s)", (TICK_LOCK_KEY,))
        threads = [threading.Thread(target=lambda: results.append(service.tick().ran)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        holder.rollback()
        holder.close()
    assert results == [False] * 4


def test_global_cap_refuses_beyond_the_limit(dsn, pool):
    with pool.connection() as conn:
        existing = conn.execute(
            "SELECT count(*) FROM merchant_orders WHERE starts_with(order_id, 'order_LIVE_')"
        ).fetchone()[0]
    capped = build_service(pool, _settings(dsn, max_live_orders=existing + 2))
    assert len(capped.submit_orders(amount=50_000, scenario="stuck", count=2)) == 2
    with pytest.raises(CapExceeded):
        capped.submit_orders(amount=50_000, scenario="stuck", count=1)

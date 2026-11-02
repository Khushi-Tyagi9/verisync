"""Shared fixtures for the integration tests: a throwaway database on the
compose Postgres (host port 15432 by default; override with
LITE_TEST_ADMIN_DSN) with db/schema.sql applied, and a lite service on it.
Skipped when Postgres is not reachable."""

from __future__ import annotations

import dataclasses
import os
import time
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg_pool import ConnectionPool  # noqa: E402

from lite.config import LiteSettings  # noqa: E402
from lite.service import build_service  # noqa: E402
from worker.config import load_settings as load_worker_settings  # noqa: E402

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


def make_settings(dsn: str, **over) -> LiteSettings:
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
    return build_service(pool, make_settings(dsn))


@pytest.fixture(scope="module")
def settings_for():
    return make_settings


@pytest.fixture(scope="module")
def tick_until():
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

    return _tick_until

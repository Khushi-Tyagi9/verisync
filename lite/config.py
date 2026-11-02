"""Settings for the Postgres-only lite mode. Env overrides, short demo timings."""

from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass
from typing import Optional

from worker.config import WorkerSettings, load_settings as load_worker_settings

MAX_ORDERS_PER_REQUEST = 10
MIN_AMOUNT = 1_000          # paise; scenarios subtract 100 for the mismatch
MAX_AMOUNT = 10_000_000     # paise


@dataclass(frozen=True)
class LiteSettings:
    worker: WorkerSettings
    immediate_seconds: int = 2
    lag_seconds: int = 60
    tick_limit: int = 20
    max_live_orders: int = 500
    # Wall-clock allowance for one tick; keeps it inside a serverless function limit.
    tick_budget_seconds: float = 6.0


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def load_lite_settings(
    postgres_dsn: Optional[str] = None, *, env_file: Optional[str] = ".env"
) -> LiteSettings:
    base = load_worker_settings(env_file)
    worker = dataclasses.replace(
        base,
        postgres_dsn=postgres_dsn or base.postgres_dsn,
        # Short window so a demo finishes in about a minute. The fallback sweep
        # is the only path in this mode, so it fires the moment recheck_at
        # passes (grace 0).
        recheck_delay_seconds=_int("DASHBOARD_LIVE_RECHECK_SECONDS", 20),
        pending_sweep_grace_seconds=0,
    )
    return LiteSettings(
        worker=worker,
        immediate_seconds=_int("DASHBOARD_LIVE_IMMEDIATE_SECONDS", 2),
        lag_seconds=_int("DASHBOARD_LIVE_LAG_SECONDS", 60),
        tick_limit=_int("DASHBOARD_LIVE_TICK_LIMIT", 20),
        max_live_orders=_int("DASHBOARD_LIVE_MAX_ORDERS", 500),
        tick_budget_seconds=float(_int("DASHBOARD_LIVE_TICK_BUDGET_SECONDS", 6)),
    )

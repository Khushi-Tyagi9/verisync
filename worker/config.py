"""Environment-backed settings for the reconciliation worker.

Parallel to ``ingestion/config.py``. Every value has a working default aimed at
the local docker-compose stack, so the worker starts with no ``.env`` at all;
override any of them through the environment (or ``.env``) for other setups.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

_DEFAULTS = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
    "LIFECYCLE_TOPIC": "order-lifecycle-events",
    "WORKER_CONSUMER_GROUP": "verisync-worker",
    "POSTGRES_DSN": "postgresql://verisync:verisync@localhost:15432/verisync",
    "REDIS_URL": "redis://localhost:6379/0",
}

# Timings. Seconds everywhere; converted to the units each client wants at the
# call site. Defaults mirror the architecture plan:
#   recheck delay .................. 10 min   (decision.DEFAULT_RECHECK_DELAY)
#   idempotency key TTL ............ 48 h     (Redis pre-filter)
#   PENDING_RECHECK sweep grace .... 1 min    (recheck_at < NOW() - INTERVAL '1 minute')
#   stuck-PROCESSING timeout ....... 5 min    (updated_at < NOW() - INTERVAL '5 minutes')
#   sweep cadence ................. 3-5 min   (a fallback, deliberately not per-second)
_TIMING_DEFAULTS = {
    "WORKER_RECHECK_DELAY_SECONDS": 600,
    "WORKER_IDEMPOTENCY_TTL_SECONDS": 172_800,
    "WORKER_PENDING_SWEEP_GRACE_SECONDS": 60,
    "WORKER_STUCK_PROCESSING_TIMEOUT_SECONDS": 300,
    "WORKER_SWEEP_INTERVAL_SECONDS": 180,
    "WORKER_MAX_RECLAIM_RETRIES": 3,
}


@dataclass(frozen=True)
class WorkerSettings:
    kafka_bootstrap_servers: str
    lifecycle_topic: str
    consumer_group: str
    postgres_dsn: str
    redis_url: str

    recheck_delay_seconds: int
    idempotency_ttl_seconds: int
    pending_sweep_grace_seconds: int
    stuck_processing_timeout_seconds: int
    sweep_interval_seconds: int
    max_reclaim_retries: int


def _int_env(name: str) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return int(_TIMING_DEFAULTS[name])
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def load_settings(env_file: Optional[str] = ".env") -> WorkerSettings:
    if env_file:
        load_dotenv(env_file)

    return WorkerSettings(
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", _DEFAULTS["KAFKA_BOOTSTRAP_SERVERS"]
        ),
        lifecycle_topic=os.environ.get("LIFECYCLE_TOPIC", _DEFAULTS["LIFECYCLE_TOPIC"]),
        consumer_group=os.environ.get(
            "WORKER_CONSUMER_GROUP", _DEFAULTS["WORKER_CONSUMER_GROUP"]
        ),
        postgres_dsn=os.environ.get("POSTGRES_DSN", _DEFAULTS["POSTGRES_DSN"]),
        redis_url=os.environ.get("REDIS_URL", _DEFAULTS["REDIS_URL"]),
        recheck_delay_seconds=_int_env("WORKER_RECHECK_DELAY_SECONDS"),
        idempotency_ttl_seconds=_int_env("WORKER_IDEMPOTENCY_TTL_SECONDS"),
        pending_sweep_grace_seconds=_int_env("WORKER_PENDING_SWEEP_GRACE_SECONDS"),
        stuck_processing_timeout_seconds=_int_env(
            "WORKER_STUCK_PROCESSING_TIMEOUT_SECONDS"
        ),
        sweep_interval_seconds=_int_env("WORKER_SWEEP_INTERVAL_SECONDS"),
        max_reclaim_retries=_int_env("WORKER_MAX_RECLAIM_RETRIES"),
    )

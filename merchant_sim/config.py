"""Settings for the merchant simulator. Local-stack defaults, env overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

_DEFAULTS = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
    "LIFECYCLE_TOPIC": "order-lifecycle-events",
    "MERCHANT_SIM_CONSUMER_GROUP": "merchant-sim",
    "POSTGRES_DSN": "postgresql://verisync:verisync@localhost:15432/verisync",
}

# Behavior profile mix. Weights need not sum to anything in particular.
_DEFAULT_PROFILE_WEIGHTS = {
    "immediate": 40,   # pending, then paid within a beat
    "lagging": 25,     # pending, then paid after a delay (often past the recheck window)
    "stuck": 25,       # pending, never paid  -> the worker must auto-correct
    "silent": 10,      # no events at all     -> the worker dead-letters after retries
}


@dataclass(frozen=True)
class MerchantSimSettings:
    kafka_bootstrap_servers: str
    lifecycle_topic: str
    consumer_group: str
    postgres_dsn: str

    # Seconds. A 'lagging' order's paid event fires this long after its capture
    # is observed; the default sits well past the worker's 10-minute recheck so
    # the lag is visible as a real correction, not just resolved-naturally.
    lag_seconds: int = 900
    # A tiny delay for 'immediate' so the pending and paid events are ordered.
    immediate_seconds: int = 2
    # How often the scheduler scans merchant_orders for due transitions.
    scan_interval_seconds: int = 5

    # Force every new order onto one profile (testing / checkpoints). None =
    # weighted-random from profile_weights.
    force_profile: Optional[str] = None

    profile_weights: dict = field(default_factory=lambda: dict(_DEFAULT_PROFILE_WEIGHTS))


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def load_settings(env_file: Optional[str] = ".env") -> MerchantSimSettings:
    if env_file:
        load_dotenv(env_file)
    return MerchantSimSettings(
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", _DEFAULTS["KAFKA_BOOTSTRAP_SERVERS"]
        ),
        lifecycle_topic=os.environ.get("LIFECYCLE_TOPIC", _DEFAULTS["LIFECYCLE_TOPIC"]),
        consumer_group=os.environ.get(
            "MERCHANT_SIM_CONSUMER_GROUP", _DEFAULTS["MERCHANT_SIM_CONSUMER_GROUP"]
        ),
        postgres_dsn=os.environ.get("POSTGRES_DSN", _DEFAULTS["POSTGRES_DSN"]),
        lag_seconds=_int_env("MERCHANT_SIM_LAG_SECONDS", 900),
        immediate_seconds=_int_env("MERCHANT_SIM_IMMEDIATE_SECONDS", 2),
        scan_interval_seconds=_int_env("MERCHANT_SIM_SCAN_INTERVAL_SECONDS", 5),
        force_profile=os.environ.get("MERCHANT_SIM_FORCE_PROFILE") or None,
    )

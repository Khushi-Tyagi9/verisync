"""Environment-backed settings for the ingestion service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

_DEFAULTS = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
    "LIFECYCLE_TOPIC": "order-lifecycle-events",
    "POSTGRES_DSN": "postgresql://verisync:verisync@localhost:15432/verisync",
}


@dataclass(frozen=True)
class Settings:
    webhook_secret: str
    kafka_bootstrap_servers: str
    lifecycle_topic: str
    postgres_dsn: str


def load_settings(env_file: Optional[str] = ".env") -> Settings:
    if env_file:
        load_dotenv(env_file)

    try:
        webhook_secret = os.environ["RAZORPAY_WEBHOOK_SECRET"]
    except KeyError as exc:
        raise RuntimeError(
            "RAZORPAY_WEBHOOK_SECRET is required (copy .env.example to .env)"
        ) from exc

    return Settings(
        webhook_secret=webhook_secret,
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", _DEFAULTS["KAFKA_BOOTSTRAP_SERVERS"]
        ),
        lifecycle_topic=os.environ.get("LIFECYCLE_TOPIC", _DEFAULTS["LIFECYCLE_TOPIC"]),
        postgres_dsn=os.environ.get("POSTGRES_DSN", _DEFAULTS["POSTGRES_DSN"]),
    )

"""Settings for the dashboard. Local-stack defaults, env overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

_DEFAULT_DSN = "postgresql://verisync:verisync@localhost:15432/verisync"


@dataclass(frozen=True)
class DashboardSettings:
    postgres_dsn: str
    host: str = "127.0.0.1"
    port: int = 8050
    # How often the page re-fetches the summary, in milliseconds.
    poll_interval_ms: int = 2000
    # Rows shown in the live activity feed.
    activity_limit: int = 30


def load_settings(env_file: Optional[str] = ".env") -> DashboardSettings:
    if env_file:
        load_dotenv(env_file)

    def _int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc

    return DashboardSettings(
        postgres_dsn=os.environ.get("POSTGRES_DSN", _DEFAULT_DSN),
        host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        port=_int("DASHBOARD_PORT", 8050),
        poll_interval_ms=_int("DASHBOARD_POLL_INTERVAL_MS", 2000),
        activity_limit=_int("DASHBOARD_ACTIVITY_LIMIT", 30),
    )

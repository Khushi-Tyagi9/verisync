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
    # Off by default so a public (Vercel) deployment stays read-only. When on,
    # the app exposes POST /api/demo/orders and the page shows a control for it.
    demo_mode: bool = False
    # Shown in the page header when set, e.g. to mark a read-only deployment as
    # a snapshot of a past run ("Snapshot of a 150-order batch run, Oct 8").
    snapshot_label: Optional[str] = None
    # Serverless live mode (Postgres only): POST /api/orders, POST /api/tick.
    # Off by default; when off those routes do not exist (404).
    live_mode: bool = False
    # Optional shared secret required (X-Write-Token or Bearer) on order writes.
    write_token: Optional[str] = None


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

    def _bool(name: str) -> bool:
        return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")

    return DashboardSettings(
        postgres_dsn=os.environ.get("POSTGRES_DSN", _DEFAULT_DSN),
        host=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        port=_int("DASHBOARD_PORT", 8050),
        poll_interval_ms=_int("DASHBOARD_POLL_INTERVAL_MS", 2000),
        activity_limit=_int("DASHBOARD_ACTIVITY_LIMIT", 30),
        demo_mode=_bool("DASHBOARD_DEMO_MODE"),
        live_mode=_bool("DASHBOARD_LIVE_MODE"),
        write_token=os.environ.get("DASHBOARD_WRITE_TOKEN", "").strip() or None,
        snapshot_label=os.environ.get("DASHBOARD_SNAPSHOT_LABEL", "").strip() or None,
    )

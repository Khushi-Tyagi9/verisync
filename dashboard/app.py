"""FastAPI dashboard app.

Run it with:  python -m dashboard.app
or:           uvicorn --factory dashboard.app:create_app
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from .config import DashboardSettings, load_settings
from .queries import build_summary

_INDEX_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")


def create_app(
    *, settings: Optional[DashboardSettings] = None, pool=None
) -> FastAPI:
    """Application factory. Pass `pool` to inject a fake in tests; leave it None
    and a psycopg pool is opened on startup and closed on shutdown."""
    settings = settings or load_settings()
    page = _INDEX_HTML.replace("__POLL_INTERVAL_MS__", str(settings.poll_interval_ms))

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.pool is None:
            from psycopg_pool import ConnectionPool

            app.state.pool = ConnectionPool(
                settings.postgres_dsn, min_size=1, max_size=4, open=True, timeout=10
            )
            app.state.owns_pool = True
        try:
            yield
        finally:
            if getattr(app.state, "owns_pool", False):
                with contextlib.suppress(Exception):
                    app.state.pool.close()

    app = FastAPI(title="verisync dashboard", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.pool = pool
    app.state.owns_pool = False

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return page

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/api/summary")
    def summary():
        with app.state.pool.connection() as conn:
            data = build_summary(conn, activity_limit=settings.activity_limit)
        return JSONResponse(data)

    return app


def main() -> int:
    import uvicorn

    settings = load_settings()
    uvicorn.run(
        create_app(settings=settings),
        host=settings.host,
        port=settings.port,
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

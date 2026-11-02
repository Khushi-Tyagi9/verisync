"""FastAPI dashboard app.

Run it with:  python -m dashboard.app
or:           uvicorn --factory dashboard.app:create_app
"""

from __future__ import annotations

import contextlib
import html
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

try:
    from .config import DashboardSettings, load_settings
    from .queries import build_summary
except ImportError:
    # Vercel's Python runtime loads this file as a standalone module with no
    # package context, so the relative imports above fail there. config.py
    # and queries.py sit as siblings to this file in both cases, so the
    # absolute form resolves the same modules when run that way.
    from config import DashboardSettings, load_settings
    from queries import build_summary

_INDEX_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")


DEMO_MAX_ORDERS = 50

_DEMO_CONTROL_HTML = """<span class="demo">
      <label>Simulate <input type="number" id="demoCount" min="1" max="50" value="10" /> orders</label>
      <button type="button" id="demoGo">Simulate</button>
      <span class="sub" id="demoMsg"></span>
    </span>
    <script>
      document.getElementById("demoGo").addEventListener("click", async () => {
        const n = document.getElementById("demoCount").value;
        const msg = document.getElementById("demoMsg");
        msg.textContent = "sending...";
        try {
          const res = await fetch("/api/demo/orders?count=" + encodeURIComponent(n), { method: "POST" });
          const body = await res.json();
          msg.textContent = res.ok ? "emitted " + body.emitted : (body.detail || res.status);
        } catch (e) {
          msg.textContent = "failed";
        }
      });
    </script>"""


def _emit_synthetic_orders(count: int) -> list[str]:
    # Imported here, not at module top: merchant_sim pulls in the Kafka client,
    # which the read-only Vercel deployment does not install and never calls.
    from merchant_sim.config import load_settings as load_sim_settings
    from merchant_sim.simulator import emit_synthetic_captures

    return emit_synthetic_captures(load_sim_settings(), count)


def create_app(
    *,
    settings: Optional[DashboardSettings] = None,
    pool=None,
    emit_orders: Optional[Callable[[int], list[str]]] = None,
) -> FastAPI:
    """Application factory. Pass `pool` / `emit_orders` to inject fakes in
    tests; leave `pool` None and a psycopg pool is opened on startup and closed
    on shutdown."""
    settings = settings or load_settings()
    emit_orders = emit_orders or _emit_synthetic_orders
    page = _INDEX_HTML.replace("__POLL_INTERVAL_MS__", str(settings.poll_interval_ms))
    page = page.replace(
        "<!--SNAPSHOT_LABEL-->",
        f'<span class="snapshot">{html.escape(settings.snapshot_label)}</span>'
        if settings.snapshot_label
        else "",
    )
    page = page.replace(
        "<!--DEMO_CONTROL-->", _DEMO_CONTROL_HTML if settings.demo_mode else ""
    )

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

    if settings.demo_mode:
        # Registered only in demo mode, so with it off the route is a plain 404.
        @app.post("/api/demo/orders")
        def demo_orders(count: int = Query(10, ge=1, le=DEMO_MAX_ORDERS)):
            try:
                order_ids = emit_orders(count)
            except Exception as exc:
                raise HTTPException(
                    status_code=502, detail=f"could not emit orders: {exc}"
                )
            return JSONResponse({"emitted": len(order_ids), "order_ids": order_ids})

    return app


app = create_app()
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

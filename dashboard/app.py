"""FastAPI dashboard app.

Run it with:  python -m dashboard.app
or:           uvicorn --factory dashboard.app:create_app
"""

from __future__ import annotations

import contextlib
import hmac
import html
import time
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

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
_LIVE_HTML = (Path(__file__).parent / "live.html").read_text(encoding="utf-8")

# The page polls every couple of seconds; one tick per instance per second is plenty.
_MIN_TICK_GAP_SECONDS = 1.0


class OrderRequest(BaseModel):
    amount: int
    scenario: str
    # Hard per-request ceiling; lite.config.MAX_ORDERS_PER_REQUEST mirrors it.
    count: int = Field(1, ge=1, le=10)


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
    live_service=None,
) -> FastAPI:
    """Application factory. Pass `pool` / `emit_orders` to inject fakes in
    tests; leave `pool` None and a psycopg pool is opened on startup and closed
    on shutdown. `live_service` (lite.service.LiteService-shaped) is only used
    when settings.live_mode is on; if None it is built from the pool on startup."""
    settings = settings or load_settings()
    emit_orders = emit_orders or _emit_synthetic_orders
    page = _INDEX_HTML.replace("__POLL_INTERVAL_MS__", str(settings.poll_interval_ms))
    page = page.replace(
        "<!--SNAPSHOT_LABEL-->",
        f'<span class="snapshot">{html.escape(settings.snapshot_label)}</span>'
        if settings.snapshot_label
        else "",
    )
    page = page.replace("<!--LIVE_PANEL-->", _LIVE_HTML if settings.live_mode else "")
    page = page.replace(
        "__LIVE_NEEDS_TOKEN__", "true" if settings.write_token else "false"
    )
    page = page.replace(
        "<!--DEMO_CONTROL-->", _DEMO_CONTROL_HTML if settings.demo_mode else ""
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.pool is None:
            from psycopg_pool import ConnectionPool

            app.state.pool = ConnectionPool(
                settings.postgres_dsn,
                min_size=1,
                max_size=4,
                open=True,
                timeout=10,
                # Neon pools with PgBouncer in transaction mode; server-side
                # prepared statements do not survive being moved between
                # backends, so never auto-prepare.
                kwargs={"prepare_threshold": None},
            )
            app.state.owns_pool = True
        if settings.live_mode and app.state.live is None:
            from lite.service import build_service

            app.state.live = build_service(app.state.pool)
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
    app.state.live = live_service
    app.state.last_tick = 0.0

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

    if settings.live_mode:
        _add_live_routes(app, settings)

    return app


def _check_token(settings: DashboardSettings, x_write_token, authorization) -> None:
    expected = settings.write_token
    if not expected:
        return
    supplied = x_write_token
    if not supplied and authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if not supplied or not hmac.compare_digest(supplied.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="missing or invalid write token")


def _add_live_routes(app: FastAPI, settings: DashboardSettings) -> None:
    """Registered only when DASHBOARD_LIVE_MODE is on, so otherwise they 404.

    lite is imported lazily: on a read-only deployment nothing here runs."""

    @app.get("/api/live/scenarios")
    def live_scenarios():
        from lite.config import MAX_AMOUNT, MAX_ORDERS_PER_REQUEST, MIN_AMOUNT
        from lite.scenarios import SCENARIOS

        return {
            "needs_token": bool(settings.write_token),
            "max_per_request": MAX_ORDERS_PER_REQUEST,
            "amount": {"min": MIN_AMOUNT, "max": MAX_AMOUNT},
            "scenarios": [
                {"key": s.key, "label": s.label, "expected_tag": s.expected_tag, "why": s.why}
                for s in SCENARIOS.values()
            ],
        }

    @app.post("/api/orders", status_code=201)
    def live_create_orders(
        req: OrderRequest,
        x_write_token: Optional[str] = Header(None),
        authorization: Optional[str] = Header(None),
    ):
        from lite.service import CapExceeded, LiteError

        _check_token(settings, x_write_token, authorization)
        try:
            ids = app.state.live.submit_orders(
                amount=req.amount, scenario=req.scenario, count=req.count
            )
        except CapExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc))
        except LiteError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"created": len(ids), "order_ids": ids}

    @app.post("/api/tick")
    def live_tick():
        # Deliberately not token-gated: it only advances existing synthetic
        # orders, is bounded, and is serialized by a Postgres advisory lock.
        now = time.monotonic()
        if now - app.state.last_tick < _MIN_TICK_GAP_SECONDS:
            return {"ran": False, "throttled": True}
        app.state.last_tick = now
        result = app.state.live.tick()
        return {
            "ran": result.ran,
            "throttled": False,
            "reclaimed": result.reclaimed,
            "rechecks_resolved": result.rechecks_resolved,
            "merchant_paid": result.merchant_paid,
        }

    @app.get("/api/live/orders")
    def live_orders():
        return {"orders": app.state.live.recent_orders()}


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

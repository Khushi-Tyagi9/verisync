"""FastAPI dashboard app.

Run it with:  python -m dashboard.app
or:           uvicorn --factory dashboard.app:create_app
"""

from __future__ import annotations

import contextlib
import hmac
import logging
import mimetypes
import html
import time
from pathlib import Path
from typing import Callable, Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

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

_log = logging.getLogger("verisync.dashboard")

# Some platforms (Windows, slim images) do not map .woff2; the font is served from /assets.
mimetypes.add_type("font/woff2", ".woff2")

_INDEX_HTML = (Path(__file__).parent / "index.html").read_text(encoding="utf-8")
_LIVE_HTML = (Path(__file__).parent / "live.html").read_text(encoding="utf-8")

# The page polls every couple of seconds; one tick per instance per second is plenty.
_MIN_TICK_GAP_SECONDS = 1.0


# Returned when the reconcile engine packages are missing from the deployment,
# for example a Vercel project whose Root Directory is still "dashboard": the page
# and assets ship, but decision/, worker/ and reconcile_tool/ do not.
_ENGINE_MISSING = (
    "The reconcile engine is not part of this deployment, so files cannot be "
    "checked here. Deploy from the repository root (Vercel Root Directory) so "
    "decision/, worker/ and reconcile_tool/ are included."
)
_RECONCILE_FAILED = (
    "The server could not process these files. Nothing was stored. If it keeps "
    "happening, report it together with the files."
)


def _load_reconcile_api():
    """The reconcile module, or None when the engine is not deployed."""
    try:
        from reconcile_tool import api
    except ModuleNotFoundError:
        return None
    return api


_LIVE_BADGE_HTML = (
    '<div class="badge-live">Live demo: serverless mode. Postgres only. '
    "The full Redpanda and Redis pipeline runs via docker compose.</div>"
)
_LIVE_TAB_HTML = (
    '<button class="tab" role="tab" id="tab-live" aria-controls="panel-live" '
    'aria-selected="false" tabindex="-1">Live scenarios</button>'
)
_LIVE_SCRIPT_HTML = '<script src="/assets/live.js" defer></script>'


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
    live = settings.live_mode
    page = page.replace("<!--LIVE_BADGE-->", _LIVE_BADGE_HTML if live else "")
    page = page.replace("<!--LIVE_TAB-->", _LIVE_TAB_HTML if live else "")
    page = page.replace("<!--LIVE_PANEL-->", _LIVE_HTML if live else "")
    page = page.replace("<!--LIVE_SCRIPT-->", _LIVE_SCRIPT_HTML if live else "")
    page = page.replace("__LIVE_MODE__", "true" if live else "false")
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
    app.mount("/assets", StaticFiles(directory=Path(__file__).parent / "assets"), name="assets")
    app.state.settings = settings
    app.state.pool = pool
    app.state.owns_pool = False
    app.state.live = live_service
    app.state.last_tick = 0.0

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return page

    @app.post("/api/reconcile")
    async def reconcile_endpoint(request: Request):
        """Stateless: parse the two exports in the body and return verdicts.
        Always on. Touches no database, stores nothing, and never logs the input."""
        api = _load_reconcile_api()
        if api is None:
            return JSONResponse({"error": _ENGINE_MISSING}, status_code=503)
        MAX_BODY_BYTES, handle_reconcile = api.MAX_BODY_BYTES, api.handle_reconcile

        too_big = JSONResponse(
            {"error": f"Request body is larger than {MAX_BODY_BYTES // (1024 * 1024)} MB."},
            status_code=413,
        )
        declared = request.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > MAX_BODY_BYTES:
            return too_big
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                return too_big
            chunks.append(chunk)
        try:
            status, payload = await run_in_threadpool(handle_reconcile, b"".join(chunks))
        except Exception as exc:
            # Only the exception type: messages can quote cell values.
            _log.error("reconcile failed: %s", type(exc).__name__)
            return JSONResponse({"error": _RECONCILE_FAILED}, status_code=500)
        return JSONResponse(payload, status_code=status)

    @app.get("/api/reconcile/sample")
    def reconcile_sample():
        api = _load_reconcile_api()
        if api is None:
            return JSONResponse({"error": _ENGINE_MISSING}, status_code=503)
        return api.sample_data()

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
            "partial": result.partial,
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

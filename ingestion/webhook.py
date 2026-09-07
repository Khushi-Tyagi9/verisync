"""FastAPI ingestion app.

Run it with:  uvicorn --factory ingestion.webhook:create_app
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import Settings, load_settings
from .out_of_scope import OutOfScopeStore, PostgresOutOfScopeStore
from .payload import build_envelope, extract_event_type, extract_order_id
from .publisher import KafkaLifecyclePublisher, LifecyclePublisher, PublishError
from .signatures import verify_signature
from .topics import ensure_topic

WEBHOOK_PATH = "/webhooks/razorpay"


def create_app(
    *,
    settings: Optional[Settings] = None,
    publisher: Optional[LifecyclePublisher] = None,
    oos_store: Optional[OutOfScopeStore] = None,
) -> FastAPI:
    """Application factory. Pass `publisher` / `oos_store` to inject fakes in
    tests; leave them None and they are built from `settings` on startup."""
    settings = settings or load_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.publisher is None:
            with contextlib.suppress(Exception):
                ensure_topic(settings.kafka_bootstrap_servers, settings.lifecycle_topic)
            app.state.publisher = KafkaLifecyclePublisher.from_settings(settings)
        if app.state.oos_store is None:
            app.state.oos_store = PostgresOutOfScopeStore.from_settings(settings)
        try:
            yield
        finally:
            for obj in (app.state.publisher, app.state.oos_store):
                close = getattr(obj, "close", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        close()

    app = FastAPI(title="verisync ingestion", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.publisher = publisher
    app.state.oos_store = oos_store

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post(WEBHOOK_PATH)
    async def razorpay_webhook(request: Request):
        # 1. Raw body FIRST -- before anything can parse and reserialize it.
        raw = await request.body()

        # 2. Signature over those exact bytes.
        signature = request.headers.get("X-Razorpay-Signature", "")
        if not verify_signature(raw, signature, settings.webhook_secret):
            raise HTTPException(status_code=401, detail="invalid or missing signature")

        # 3. Stable per-event dedup key (Razorpay's own recommended key).
        event_id = request.headers.get("X-Razorpay-Event-Id", "").strip()
        if not event_id:
            raise HTTPException(status_code=400, detail="missing X-Razorpay-Event-Id header")

        try:
            body = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise HTTPException(status_code=400, detail="request body is not valid JSON")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="request body must be a JSON object")

        event_type = extract_event_type(body)
        order_id = extract_order_id(body)

        # 4. Scope gate.
        if not order_id:
            app.state.oos_store.record(
                event_id=event_id, event_type=event_type, payload=body
            )
            return JSONResponse(
                status_code=200,
                content={"status": "out_of_scope", "event_id": event_id},
            )

        # 5. In scope -> normalized envelope onto the unified topic, keyed by order_id.
        envelope = build_envelope(
            event_id=event_id,
            event_type=event_type,
            order_id=order_id,
            raw=body,
            received_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            app.state.publisher.publish(order_id=order_id, envelope=envelope)
        except PublishError as exc:
            raise HTTPException(
                status_code=503, detail=f"could not enqueue event: {exc}"
            )

        return JSONResponse(
            status_code=202,
            content={"status": "accepted", "event_id": event_id, "order_id": order_id},
        )

    return app

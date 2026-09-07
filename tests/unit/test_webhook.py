"""Route-level tests for the ingestion app, using injected fakes (no infra)."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from ingestion.config import Settings
from ingestion.publisher import PublishError
from ingestion.webhook import WEBHOOK_PATH, create_app

SECRET = "test_secret"
SETTINGS = Settings(
    webhook_secret=SECRET,
    kafka_bootstrap_servers="unused",
    lifecycle_topic="order-lifecycle-events",
    postgres_dsn="unused",
)


class FakePublisher:
    def __init__(self, raise_error: bool = False):
        self.published: list[tuple[str, dict]] = []
        self.raise_error = raise_error

    def publish(self, *, order_id: str, envelope: dict) -> None:
        if self.raise_error:
            raise PublishError("broker unreachable")
        self.published.append((order_id, envelope))

    def close(self) -> None:  # pragma: no cover - trivial
        pass


class FakeOOSStore:
    def __init__(self):
        self.records: dict[str, dict] = {}

    def record(self, *, event_id: str, event_type, payload: dict) -> None:
        self.records.setdefault(event_id, {"event_type": event_type, "payload": payload})

    def close(self) -> None:  # pragma: no cover - trivial
        pass


def sign(raw: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def make_body(order_id="order_ABC123", event="payment.captured"):
    return {
        "entity": "event",
        "event": event,
        "contains": ["payment"],
        "payload": {"payment": {"entity": {"id": "pay_1", "order_id": order_id, "amount": 50000}}},
        "created_at": 1_757_246_400,
    }


@pytest.fixture
def ctx():
    publisher = FakePublisher()
    oos = FakeOOSStore()
    app = create_app(settings=SETTINGS, publisher=publisher, oos_store=oos)
    with TestClient(app) as client:
        yield client, publisher, oos


def _post(client, raw: bytes, *, sig: str | None, event_id: str | None = "evt_1"):
    headers = {"Content-Type": "application/json"}
    if sig is not None:
        headers["X-Razorpay-Signature"] = sig
    if event_id is not None:
        headers["X-Razorpay-Event-Id"] = event_id
    return client.post(WEBHOOK_PATH, content=raw, headers=headers)


def test_valid_signed_in_scope_event_is_published(ctx):
    client, publisher, oos = ctx
    raw = json.dumps(make_body("order_XYZ")).encode()

    resp = _post(client, raw, sig=sign(raw))

    assert resp.status_code == 202
    assert resp.json()["status"] == "accepted"
    assert len(publisher.published) == 1
    order_id, envelope = publisher.published[0]
    assert order_id == "order_XYZ"
    assert envelope["event_id"] == "evt_1"
    assert envelope["order_id"] == "order_XYZ"
    assert envelope["raw"]["event"] == "payment.captured"
    assert oos.records == {}


def test_signature_is_verified_against_raw_bytes_not_reserialized_json(ctx):
    client, publisher, _ = ctx
    # Unusual spacing + key order: only verification over the exact bytes passes.
    raw = b'{"created_at": 1757246400 ,   "event":"payment.captured","payload":{"payment":{"entity":{"order_id":"order_RAW","id":"pay_1"}}}}'

    resp = _post(client, raw, sig=sign(raw))

    assert resp.status_code == 202
    assert publisher.published[0][0] == "order_RAW"


def test_out_of_scope_event_is_recorded_not_published(ctx):
    client, publisher, oos = ctx
    raw = json.dumps(make_body(order_id=None)).encode()

    resp = _post(client, raw, sig=sign(raw), event_id="evt_oos")

    assert resp.status_code == 200
    assert resp.json()["status"] == "out_of_scope"
    assert publisher.published == []
    assert "evt_oos" in oos.records


def test_out_of_scope_record_is_idempotent_on_event_id(ctx):
    client, _, oos = ctx
    raw = json.dumps(make_body(order_id=None)).encode()

    _post(client, raw, sig=sign(raw), event_id="evt_dup")
    _post(client, raw, sig=sign(raw), event_id="evt_dup")

    assert list(oos.records) == ["evt_dup"]


def test_missigned_payload_is_rejected_401_and_has_no_side_effects(ctx):
    client, publisher, oos = ctx
    raw = json.dumps(make_body()).encode()

    resp = _post(client, raw, sig="deadbeef" * 8)

    assert resp.status_code == 401
    assert publisher.published == []
    assert oos.records == {}


def test_missing_signature_header_is_401(ctx):
    client, _, _ = ctx
    raw = json.dumps(make_body()).encode()
    assert _post(client, raw, sig=None).status_code == 401


def test_missing_event_id_header_is_400(ctx):
    client, _, _ = ctx
    raw = json.dumps(make_body()).encode()
    assert _post(client, raw, sig=sign(raw), event_id=None).status_code == 400


def test_correctly_signed_non_json_body_is_400(ctx):
    client, publisher, _ = ctx
    raw = b"this is not json"
    resp = _post(client, raw, sig=sign(raw))
    assert resp.status_code == 400
    assert publisher.published == []


def test_publish_failure_returns_503_so_razorpay_retries():
    publisher = FakePublisher(raise_error=True)
    oos = FakeOOSStore()
    app = create_app(settings=SETTINGS, publisher=publisher, oos_store=oos)
    with TestClient(app) as client:
        raw = json.dumps(make_body()).encode()
        resp = _post(client, raw, sig=sign(raw))
    assert resp.status_code == 503


def test_health_endpoint(ctx):
    client, _, _ = ctx
    assert client.get("/health").json() == {"status": "ok"}

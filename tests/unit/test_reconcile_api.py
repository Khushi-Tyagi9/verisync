"""POST /api/reconcile: always on, stateless, bounded, no database."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.config import DashboardSettings
from reconcile_tool.api import MAX_BODY_BYTES
from reconcile_tool.parse import MAX_ROWS

SAMPLE = Path(__file__).resolve().parents[2] / "reconcile_tool" / "sample"


def _client():
    # No pool is injected and no lifespan runs: any database use would fail.
    app = create_app(settings=DashboardSettings(postgres_dsn="postgresql://nobody@127.0.0.1:1/none"))
    return TestClient(app)


def _post(client, **body):
    return client.post("/api/reconcile", content=json.dumps(body), headers={"content-type": "application/json"})


GOOD = {"gateway": "order_id,status,amount\nA,captured,100\n", "merchant": "order_id,status\nA,pending\n"}


def test_works_with_no_database_configured_and_no_live_mode_or_token():
    res = _post(_client(), **GOOD)
    assert res.status_code == 200
    body = res.json()
    assert body["orders"][0]["verdict"] == "AUTO_CORRECT"
    assert body["amount_unit"] == "rupees"
    assert "does not change any merchant system" in body["notice"]


def test_runs_the_bundled_sample_end_to_end_with_golden_counts():
    c = _client()
    sample = c.get("/api/reconcile/sample").json()
    assert sample["gateway"] == (SAMPLE / "gateway.csv").read_text(encoding="utf-8")
    body = _post(c, **sample).json()
    assert body["summary"]["orders"] == 14
    by = body["summary"]["by_verdict"]
    assert {k: (v["count"], v["amount_paise"]) for k, v in by.items()} == {
        "AUTO_CORRECT": (3, 251_910), "CONSISTENT": (3, 214_125), "FLAGGED_RISKY": (1, 75_000),
        "DISPUTED": (2, 800_000), "CORRELATION_FAILED": (2, 240_000), "UNRESOLVED": (3, 324_000),
    }
    assert sorted((i["source"], i["row"]) for i in body["invalid_rows"]) == [
        ("gateway", 19), ("gateway", 20), ("merchant", 15), ("merchant", 16)]
    row = next(o for o in body["orders"] if o["order_id"] == "order_1002")
    assert set(row) >= {"order_id", "verdict", "reason", "gateway_status", "merchant_status",
                        "amount", "recommended_action", "decision_trail"}
    assert row["decision_trail"][-1]["step"].startswith("recheck")


def test_amount_unit_paise_is_respected():
    body = _post(_client(), **GOOD, amount_unit="paise").json()
    assert body["orders"][0]["amount"] == 100


def test_invalid_amount_unit_is_rejected():
    res = _post(_client(), **GOOD, amount_unit="dollars")
    assert res.status_code == 422 and res.json()["field"] == "amount_unit"


@pytest.mark.parametrize(
    "body",
    [{"merchant": "order_id,status\n"}, {"gateway": 5, "merchant": "x"}, {"gateway": "a", "merchant": None}],
)
def test_missing_or_non_text_inputs_are_422(body):
    assert _post(_client(), **body).status_code == 422


def test_non_json_and_non_object_bodies_are_422():
    c = _client()
    assert c.post("/api/reconcile", content=b"not json").status_code == 422
    assert c.post("/api/reconcile", content=b"[1,2]").status_code == 422


def test_missing_column_message_is_clear():
    res = _post(_client(), gateway="order_id,status\nA,captured\n", merchant="order_id,status\nA,paid\n")
    assert res.status_code == 422
    assert "amount" in res.json()["error"] and res.json()["field"] == "gateway"


def test_row_limit_returns_422_with_a_clear_message():
    rows = "order_id,status\n" + "".join(f"O{i},paid\n" for i in range(MAX_ROWS + 1))
    res = _post(_client(), gateway="order_id,status,amount\nA,captured,1\n", merchant=rows)
    assert res.status_code == 422
    assert str(MAX_ROWS) in res.json()["error"] and res.json()["field"] == "merchant"


def test_at_the_row_limit_it_still_works():
    rows = "order_id,status\n" + "".join(f"O{i},paid\n" for i in range(MAX_ROWS))
    res = _post(_client(), gateway="order_id,status,amount\nA,captured,1\n", merchant=rows)
    assert res.status_code == 200 and res.json()["input"]["merchant_rows"] == MAX_ROWS


def test_body_over_two_megabytes_is_413_by_content_length():
    big = json.dumps({"gateway": "x" * (MAX_BODY_BYTES + 10), "merchant": ""})
    res = _client().post("/api/reconcile", content=big)
    assert res.status_code == 413 and "2 MB" in res.json()["error"]


def test_body_over_two_megabytes_is_413_when_streamed_without_a_length():
    def chunks():
        yield b'{"gateway": "'
        for _ in range(3):
            yield b"x" * (MAX_BODY_BYTES // 2)
        yield b'", "merchant": ""}'

    res = _client().post("/api/reconcile", content=chunks())
    assert res.status_code == 413


def test_invalid_rows_are_returned_not_dropped():
    res = _post(_client(), gateway="order_id,status,amount\nA,captured,1\nB,bogus,1\n",
                merchant="order_id,status\nA,paid\n")
    body = res.json()
    assert body["input"] == {"gateway_rows": 1, "merchant_rows": 1, "invalid_rows": 1}
    assert body["invalid_rows"][0]["row"] == 3 and "not recognized" in body["invalid_rows"][0]["reasons"][0]


def test_nothing_is_logged_and_error_bodies_do_not_echo_the_input(caplog):
    secret = "SENTINEL_ORDER_9f3a"
    c = _client()
    with caplog.at_level(logging.DEBUG):
        ok = _post(c, gateway=f"order_id,status,amount\n{secret},captured,1\n", merchant="order_id,status\n")
        bad = c.post("/api/reconcile", content=f"{secret} not json".encode())
        wrong_shape = _post(c, gateway=5, merchant=secret)
    assert ok.status_code == 200
    # Malformed bodies are never echoed. (A missing-column error does list the
    # header names it found, on purpose, so a bad export is easy to fix.)
    assert secret not in bad.text and secret not in wrong_shape.text
    assert secret not in caplog.text


def test_endpoint_survives_the_real_lifespan_with_an_unreachable_database():
    pytest.importorskip("psycopg_pool")
    app = create_app(settings=DashboardSettings(postgres_dsn="postgresql://nobody@127.0.0.1:1/none"))
    with TestClient(app) as c:
        assert _post(c, **GOOD).status_code == 200


def test_page_and_health_do_not_need_the_database_either():
    c = _client()
    assert c.get("/health").json() == {"status": "ok"}
    assert c.get("/").status_code == 200

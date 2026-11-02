"""Regression: gateway_sample.csv + merchant_sample.csv (as uploaded by a user)
returned HTTP 500 with a non-JSON body, which the page showed as "(500).null".

Cause: a deployment whose Root Directory is "dashboard" ships the page and its
assets but not decision/, worker/ or reconcile_tool/, so the lazy import inside
POST /api/reconcile raised ModuleNotFoundError and FastAPI answered with a
plain-text 500. The files themselves are fine: their one bad row (an empty
order_id) must come back as a row error with its row number.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.config import DashboardSettings

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "user_samples"
GATEWAY = (FIXTURES / "gateway_sample.csv").read_text(encoding="utf-8")
MERCHANT = (FIXTURES / "merchant_sample.csv").read_text(encoding="utf-8")
BODY = json.dumps({"gateway": GATEWAY, "merchant": MERCHANT, "amount_unit": "rupees"})


def _client():
    # No lifespan runs, so no database is ever opened.
    app = create_app(settings=DashboardSettings(postgres_dsn=""))
    return TestClient(app, raise_server_exceptions=False)


def test_user_files_are_reconciled_and_the_bad_row_is_a_row_error():
    res = _client().post("/api/reconcile", content=BODY)
    assert res.status_code == 200
    body = res.json()
    assert body["input"] == {"gateway_rows": 8, "merchant_rows": 9, "invalid_rows": 1}
    assert body["invalid_rows"] == [
        {"source": "gateway", "row": 10, "order_id": None, "reasons": ["order_id is missing"]}
    ]
    assert {o["order_id"]: o["verdict"] for o in body["orders"]} == {
        "order_1001": "CONSISTENT",
        "order_1002": "AUTO_CORRECT",
        "order_1003": "FLAGGED_RISKY",
        "order_1004": "DISPUTED",  # gateway refund row only, merchant paid
        "order_1005": "CORRELATION_FAILED",
        "order_1006": "AUTO_CORRECT",
        "order_1007": "CONSISTENT",
        "order_1008": "CONSISTENT",
        "order_9001": "UNRESOLVED",
    }
    # The blank-id row never becomes an order of its own.
    assert all(o["order_id"] for o in body["orders"])


def test_user_files_with_windows_line_endings_give_the_same_result():
    crlf = json.dumps({"gateway": GATEWAY.replace("\n", "\r\n"), "merchant": MERCHANT.replace("\n", "\r\n")})
    res = _client().post("/api/reconcile", content=crlf)
    assert res.status_code == 200
    assert res.json()["invalid_rows"] == [
        {"source": "gateway", "row": 10, "order_id": None, "reasons": ["order_id is missing"]}
    ]


def test_user_files_read_as_paise_report_every_decimal_amount_as_a_row_error():
    # The files are in rupees; read as paise, 1200.50 is not a whole number of
    # paise. That is a row error with a row number for each row, never a 500.
    body = json.dumps({"gateway": GATEWAY, "merchant": MERCHANT, "amount_unit": "paise"})
    res = _client().post("/api/reconcile", content=body)
    assert res.status_code == 200
    bad = {(i["source"], i["row"]) for i in res.json()["invalid_rows"]}
    assert ("gateway", 2) in bad and ("gateway", 10) in bad and ("merchant", 2) in bad
    assert all(i["reasons"] for i in res.json()["invalid_rows"])


def test_missing_engine_is_a_clear_json_503_not_a_text_500(monkeypatch):
    # Simulates a deployment without the reconcile_tool package.
    monkeypatch.setitem(sys.modules, "reconcile_tool", None)
    monkeypatch.setitem(sys.modules, "reconcile_tool.api", None)
    c = _client()
    for res in (c.post("/api/reconcile", content=BODY), c.get("/api/reconcile/sample")):
        assert res.status_code == 503
        assert res.headers["content-type"].startswith("application/json")
        assert "Root Directory" in res.json()["error"]


def test_unexpected_failure_is_json_and_neither_echoes_nor_logs_the_input(monkeypatch, caplog):
    import reconcile_tool.api as api

    def boom(_body):
        raise ValueError("cell value SENTINEL_9f3a")

    monkeypatch.setattr(api, "handle_reconcile", boom)
    with caplog.at_level(logging.DEBUG):
        res = _client().post("/api/reconcile", content=BODY)
    assert res.status_code == 500
    assert res.headers["content-type"].startswith("application/json")
    assert "could not process these files" in res.json()["error"]
    assert "SENTINEL_9f3a" not in res.text and "SENTINEL_9f3a" not in caplog.text
    assert "ValueError" in caplog.text


def test_dashboard_only_layout_answers_with_json_not_a_text_500(tmp_path):
    """The real failure mode: only dashboard/ deployed (Vercel Root Directory
    "dashboard"). Run in a separate interpreter with the repo off sys.path."""
    shutil.copytree(ROOT / "dashboard", tmp_path / "dashboard", ignore=shutil.ignore_patterns("__pycache__"))
    (tmp_path / "body.json").write_text(BODY, encoding="utf-8")
    script = textwrap.dedent(
        """
        import json, sys
        sys.path.insert(0, ".")
        import app
        from fastapi.testclient import TestClient
        assert "reconcile_tool" not in sys.modules
        c = TestClient(app.app, raise_server_exceptions=False)
        r = c.post("/api/reconcile", content=open("../body.json", encoding="utf-8").read())
        print(json.dumps({"status": r.status_code, "type": r.headers["content-type"], "body": r.json()}))
        """
    )
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "POSTGRES_DSN")}
    # -E ignores PYTHONPATH; the working directory holds only the copied
    # dashboard/, so the repo's packages are not importable.
    out = subprocess.run(
        [sys.executable, "-E", "-c", script],
        cwd=tmp_path / "dashboard", capture_output=True, text=True, timeout=120, env=env,
    )
    assert out.returncode == 0, out.stderr[-2000:]
    result = json.loads(out.stdout.strip().splitlines()[-1])
    assert result["status"] == 503
    assert result["type"].startswith("application/json")
    assert "Root Directory" in result["body"]["error"]


def test_page_script_never_renders_null_into_the_error_box():
    src = (ROOT / "dashboard" / "assets" / "tool.js").read_text(encoding="utf-8")
    start = src.index("function showError")
    block = src[start:src.index("\n  }\n", start)]
    assert ".filter(Boolean)" in block
    # Non-JSON error bodies are read as text first, then parsed defensively.
    assert "await res.text()" in src and "JSON.parse(raw)" in src

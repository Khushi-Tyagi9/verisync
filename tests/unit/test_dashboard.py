"""Dashboard summary shaping (pure) + the static endpoints. No database."""

from __future__ import annotations

from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.config import DashboardSettings
from dashboard.queries import assemble_summary


def _summary(**over):
    base = dict(
        status_counts={},
        orders_tracked=0,
        orphan_pointers=0,
        review_open_by_reason={},
        review_recent=[],
        out_of_scope_total=0,
        decision_counts={},
        activity=[],
    )
    base.update(over)
    return assemble_summary(**base)


def test_empty_summary_has_every_status_bucket_at_zero():
    s = _summary()
    assert s["orders"]["by_status"] == {
        "RESOLVED_AUTOCORRECTED": 0,
        "RESOLVED_NATURALLY": 0,
        "PENDING_RECHECK": 0,
        "PROCESSING": 0,
        "DISPUTED": 0,
        "DEAD_LETTER": 0,
    }
    assert s["orders"]["in_progress"] == 0
    assert s["review_queue"]["open"] == 0
    assert s["out_of_scope"]["total"] == 0


def test_outcome_rollups():
    s = _summary(
        status_counts={
            "RESOLVED_AUTOCORRECTED": 5,
            "RESOLVED_NATURALLY": 3,
            "PENDING_RECHECK": 2,
            "PROCESSING": 1,
            "DISPUTED": 1,
            "DEAD_LETTER": 4,
        },
        orders_tracked=20,
    )
    o = s["orders"]
    assert o["auto_corrected"] == 5
    assert o["resolved_naturally"] == 3
    assert o["resolved_total"] == 8
    assert o["disputed"] == 1
    assert o["dead_letter"] == 4
    assert o["in_progress"] == 3
    assert o["with_pointer"] == 16
    assert o["no_action_needed"] == 4  # 20 tracked - 16 with a pointer


def test_no_action_needed_never_negative():
    s = _summary(status_counts={"DISPUTED": 10}, orders_tracked=3)
    assert s["orders"]["no_action_needed"] == 0


def test_orphan_pointers_is_surfaced_not_hidden():
    # An orders_state row with no order_views row is reported as a number, so a
    # broken invariant is visible instead of clamped away.
    s = _summary(status_counts={"DEAD_LETTER": 1}, orders_tracked=0, orphan_pointers=1)
    assert s["orders"]["orphan_pointers"] == 1
    assert s["orders"]["no_action_needed"] == 0  # still clamped, but the orphan shows

    clean = _summary(status_counts={"DEAD_LETTER": 1}, orders_tracked=1, orphan_pointers=0)
    assert clean["orders"]["orphan_pointers"] == 0


def test_review_open_is_sum_of_reasons_and_kept_separate_from_oos():
    s = _summary(
        review_open_by_reason={"no_merchant_record": 2, "dead_letter": 3},
        out_of_scope_total=99,
    )
    assert s["review_queue"]["open"] == 5
    assert s["review_queue"]["by_reason"] == {"no_merchant_record": 2, "dead_letter": 3}
    assert s["out_of_scope"]["total"] == 99  # distinct channel, not folded in


def _client():
    settings = DashboardSettings(postgres_dsn="unused", poll_interval_ms=1500)
    # non-None pool so the lifespan does not try to open a real one
    return TestClient(create_app(settings=settings, pool=object()))


def test_health_ok():
    with _client() as c:
        assert c.get("/health").json() == {"status": "ok"}


def test_index_serves_html_with_poll_interval_substituted():
    with _client() as c:
        r = c.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "__POLL_INTERVAL_MS__" not in r.text
        assert "const POLL_MS = 1500;" in r.text


# --- demo mode -------------------------------------------------------------


def _demo_client(*, demo_mode, emit=None):
    calls = []

    def fake_emit(count):
        calls.append(count)
        if emit is not None:
            return emit(count)
        return [f"order_{i}" for i in range(count)]

    app = create_app(
        settings=DashboardSettings(postgres_dsn="unused", demo_mode=demo_mode),
        pool=object(),
        emit_orders=fake_emit,
    )
    return TestClient(app), calls


def test_demo_route_is_404_and_control_absent_when_off():
    client, calls = _demo_client(demo_mode=False)
    assert client.post("/api/demo/orders?count=5").status_code == 404
    assert "demoGo" not in client.get("/").text
    assert calls == []


def test_demo_mode_env_flag_defaults_to_off(monkeypatch):
    from dashboard.config import load_settings

    monkeypatch.delenv("DASHBOARD_DEMO_MODE", raising=False)
    assert load_settings(env_file=None).demo_mode is False
    monkeypatch.setenv("DASHBOARD_DEMO_MODE", "true")
    assert load_settings(env_file=None).demo_mode is True


def test_demo_route_emits_and_control_rendered_when_on():
    client, calls = _demo_client(demo_mode=True)
    assert "demoGo" in client.get("/").text
    res = client.post("/api/demo/orders?count=7")
    assert res.status_code == 200
    assert res.json()["emitted"] == 7
    assert calls == [7]


def test_demo_route_validates_count_range():
    client, calls = _demo_client(demo_mode=True)
    for bad in ("0", "51", "-3", "abc"):
        assert client.post(f"/api/demo/orders?count={bad}").status_code == 422
    assert client.post("/api/demo/orders?count=50").status_code == 200
    assert client.post("/api/demo/orders?count=1").status_code == 200
    assert calls == [50, 1]


def test_demo_route_reports_emit_failure_as_502():
    def boom(_count):
        raise RuntimeError("broker down")

    client, _ = _demo_client(demo_mode=True, emit=boom)
    assert client.post("/api/demo/orders?count=2").status_code == 502


def test_dashboard_import_does_not_pull_in_kafka():
    import subprocess
    import sys

    code = (
        "import sys; import dashboard.app; "
        "assert 'confluent_kafka' not in sys.modules and 'merchant_sim' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


# --- snapshot label --------------------------------------------------------


def _label_client(label):
    app = create_app(
        settings=DashboardSettings(postgres_dsn="unused", snapshot_label=label),
        pool=object(),
    )
    return TestClient(app)


def test_snapshot_label_shown_in_header_when_set():
    page = _label_client("Snapshot of a 150-order batch run, Oct 8").get("/").text
    assert 'class="snapshot">Snapshot of a 150-order batch run, Oct 8<' in page


def test_snapshot_label_absent_when_unset():
    page = _label_client(None).get("/").text
    assert 'class="snapshot"' not in page
    assert "<!--SNAPSHOT_LABEL-->" not in page


def test_snapshot_label_is_html_escaped():
    page = _label_client("<script>alert(1)</script>").get("/").text
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;" in page


def test_snapshot_label_env_blank_means_unset(monkeypatch):
    from dashboard.config import load_settings

    monkeypatch.setenv("DASHBOARD_SNAPSHOT_LABEL", "  ")
    assert load_settings(env_file=None).snapshot_label is None
    monkeypatch.setenv("DASHBOARD_SNAPSHOT_LABEL", "Oct 8 run")
    assert load_settings(env_file=None).snapshot_label == "Oct 8 run"


# --- live mode -------------------------------------------------------------


class _FakeLive:
    def __init__(self, *, error=None):
        self.submitted, self.ticks, self.error = [], 0, error

    def submit_orders(self, *, amount, scenario, count):
        if self.error:
            raise self.error
        self.submitted.append((amount, scenario, count))
        return [f"order_LIVE_{scenario}_{i:012x}" for i in range(count)]

    def tick(self):
        from lite.service import TickResult

        self.ticks += 1
        return TickResult(ran=True, reclaimed=0, rechecks_resolved=1, merchant_paid=2)

    def recent_orders(self, limit=25):
        return []


def _live_client(*, live_mode=True, token=None, live=None):
    live = live or _FakeLive()
    app = create_app(
        settings=DashboardSettings(postgres_dsn="unused", live_mode=live_mode, write_token=token),
        pool=object(),
        live_service=live,
    )
    return TestClient(app), live


_ORDER = {"amount": 50_000, "scenario": "normal", "count": 2}


def test_live_routes_are_404_and_panel_absent_when_off():
    client, live = _live_client(live_mode=False)
    assert client.post("/api/orders", json=_ORDER).status_code == 404
    assert client.post("/api/tick").status_code == 404
    assert client.get("/api/live/orders").status_code == 404
    assert client.get("/api/live/scenarios").status_code == 404
    page = client.get("/").text
    assert "Live demo: serverless mode" not in page
    assert "<!--LIVE_PANEL-->" not in page and "__LIVE_NEEDS_TOKEN__" not in page
    assert live.submitted == [] and live.ticks == 0


def test_live_mode_env_flags_default_off(monkeypatch):
    from dashboard.config import load_settings

    monkeypatch.delenv("DASHBOARD_LIVE_MODE", raising=False)
    monkeypatch.delenv("DASHBOARD_WRITE_TOKEN", raising=False)
    s = load_settings(env_file=None)
    assert s.live_mode is False and s.write_token is None
    monkeypatch.setenv("DASHBOARD_LIVE_MODE", "1")
    monkeypatch.setenv("DASHBOARD_WRITE_TOKEN", " s3cret ")
    s = load_settings(env_file=None)
    assert s.live_mode is True and s.write_token == "s3cret"


def test_live_panel_shows_the_serverless_badge_when_on():
    page = _live_client()[0].get("/").text
    assert "Live demo: serverless mode. Postgres only." in page
    assert "The full Redpanda and Redis pipeline runs via docker compose." in page


def test_live_order_creation():
    client, live = _live_client()
    res = client.post("/api/orders", json=_ORDER)
    assert res.status_code == 201
    assert res.json()["created"] == 2
    assert live.submitted == [(50_000, "normal", 2)]


def test_live_order_count_is_capped_at_ten_per_request():
    client, live = _live_client()
    assert client.post("/api/orders", json={**_ORDER, "count": 11}).status_code == 422
    assert client.post("/api/orders", json={**_ORDER, "count": 0}).status_code == 422
    assert client.post("/api/orders", json={**_ORDER, "count": 10}).status_code == 201
    assert live.submitted == [(50_000, "normal", 10)]


def test_live_write_token_is_required_when_configured():
    client, live = _live_client(token="s3cret")
    assert client.post("/api/orders", json=_ORDER).status_code == 401
    assert client.post("/api/orders", json=_ORDER, headers={"X-Write-Token": "nope"}).status_code == 401
    assert client.post("/api/orders", json=_ORDER, headers={"X-Write-Token": "s3cret"}).status_code == 201
    assert client.post("/api/orders", json=_ORDER, headers={"Authorization": "Bearer s3cret"}).status_code == 201
    assert len(live.submitted) == 2


def test_live_cap_and_validation_errors_map_to_http_codes():
    from lite.service import CapExceeded, LiteError

    capped, _ = _live_client(live=_FakeLive(error=CapExceeded("cap reached (5/5)")))
    res = capped.post("/api/orders", json=_ORDER)
    assert res.status_code == 429 and "cap reached" in res.json()["detail"]
    bad, _ = _live_client(live=_FakeLive(error=LiteError("unknown scenario")))
    assert bad.post("/api/orders", json=_ORDER).status_code == 400


def test_live_tick_needs_no_token_and_is_throttled_per_instance():
    client, live = _live_client(token="s3cret")
    first = client.post("/api/tick").json()
    assert first["ran"] is True and first["rechecks_resolved"] == 1 and first["merchant_paid"] == 2
    second = client.post("/api/tick").json()
    assert second == {"ran": False, "throttled": True}
    assert live.ticks == 1


def test_live_scenarios_endpoint_lists_the_catalog():
    body = _live_client(token="x")[0].get("/api/live/scenarios").json()
    assert body["needs_token"] is True and body["max_per_request"] == 10
    keys = {s["key"] for s in body["scenarios"]}
    assert keys == {"normal", "lagging", "stuck", "silent", "risky_drift", "reversal", "correlation_failure"}


def test_read_only_import_does_not_load_lite_or_kafka():
    import subprocess
    import sys

    code = (
        "import sys; import dashboard.app; "
        "bad = [m for m in ('lite', 'confluent_kafka', 'redis', 'merchant_sim') if m in sys.modules]; "
        "assert not bad, bad"
    )
    subprocess.run([sys.executable, "-c", code], check=True)

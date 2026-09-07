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

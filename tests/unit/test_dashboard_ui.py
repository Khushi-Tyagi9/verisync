"""Structure of the rebuilt page: assets, tabs, notice, live-only pieces, and
that every element id the scripts reach for exists in the markup."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.config import DashboardSettings

DASH = Path(__file__).resolve().parents[2] / "dashboard"


def _page(**over):
    app = create_app(settings=DashboardSettings(postgres_dsn="unused", **over), pool=object(), live_service=object())
    return TestClient(app)


def test_page_has_no_unsubstituted_placeholders():
    for over in ({}, {"live_mode": True, "write_token": "x"}):
        text = _page(**over).get("/").text
        assert not re.findall(r"__[A-Z_]+__", text)
        assert not re.findall(r"<!--[A-Z_]+-->", text)


def test_first_screen_is_the_tool_with_the_recommendation_notice():
    text = _page().get("/").text
    assert "Recommendations only." in text
    assert "does not change any merchant system" in text
    for needle in ("Provide your data", "Review the results", "Take the recommended actions",
                   "Load sample data", "Run reconciliation", "Gateway template (CSV)",
                   "Merchant template (CSV)", "Download corrections CSV", "Download review CSV"):
        assert needle in text
    assert 'id="tab-reconcile" aria-controls="panel-reconcile" aria-selected="true"' in text


def test_live_tab_and_badge_only_appear_in_live_mode():
    off = _page().get("/").text
    assert 'id="tab-live"' not in off and "panel-live" not in off and "live.js" not in off
    assert "Live demo: serverless mode" not in off
    on = _page(live_mode=True).get("/").text
    assert 'id="tab-live"' in on and 'id="panel-live"' in on and "/assets/live.js" in on
    assert "Live demo: serverless mode. Postgres only. The full Redpanda and Redis pipeline runs via docker compose." in on
    assert 'data-live="true"' in on and 'data-live="false"' in off


def test_system_activity_tab_is_always_present():
    assert 'id="tab-activity"' in _page().get("/").text


def test_static_assets_are_served():
    c = _page()
    for name, kind in (("app.css", "text/css"), ("shell.js", "javascript"), ("tool.js", "javascript"),
                       ("activity.js", "javascript"), ("live.js", "javascript")):
        r = c.get(f"/assets/{name}")
        assert r.status_code == 200 and kind in r.headers["content-type"], name
    assert c.get("/assets/nope.js").status_code == 404


def test_every_element_id_used_by_the_scripts_exists_in_the_markup():
    html = (DASH / "index.html").read_text(encoding="utf-8") + (DASH / "live.html").read_text(encoding="utf-8")
    present = set(re.findall(r'\bid="([^"]+)"', html))
    for script in ("shell.js", "tool.js", "activity.js", "live.js"):
        src = (DASH / "assets" / script).read_text(encoding="utf-8")
        used = set(re.findall(r'(?:\$|getElementById)\(\s*"([A-Za-z0-9_-]+)"\s*\)', src))
        # ids built from a prefix ("txt" + which) are checked explicitly below
        missing = used - present
        assert not missing, (script, missing)
    for which in ("Gateway", "Merchant"):
        for prefix in ("txt", "meta", "file", "drop"):
            assert f'id="{prefix}{which}"' in html


def test_css_defines_both_themes_and_a_phone_layout():
    css = (DASH / "assets" / "app.css").read_text(encoding="utf-8")
    assert "prefers-color-scheme: dark" in css
    assert "max-width: 760px" in css
    assert ":focus-visible" in css
    for token in ("--bg", "--panel", "--ink", "--accent", "--v-auto-ink", "--v-risk-bg"):
        assert css.count(token + ":") >= 2, token       # light and dark definitions


def test_table_header_is_semantic_and_sortable():
    text = _page().get("/").text
    assert text.count('aria-sort="none"') == 5 and 'scope="col"' in text

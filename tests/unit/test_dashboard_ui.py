"""Structure of the page: assets, tabs, copy, page order, the visual rules, and
that every element id the scripts reach for exists in the markup."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.config import DashboardSettings

DASH = Path(__file__).resolve().parents[2] / "dashboard"
CSS = (DASH / "assets" / "app.css").read_text(encoding="utf-8")
CSS_RULES = re.sub(r"/\*.*?\*/", "", CSS, flags=re.S)   # declarations only, no comments
DESCRIPTION = (
    "Compare your payment gateway's records with your merchant's order records, and see "
    "which orders match, which can be safely corrected, and which need a person to look."
)


def _page(**over):
    app = create_app(settings=DashboardSettings(postgres_dsn="unused", **over), pool=object(), live_service=object())
    return TestClient(app)


def test_page_has_no_unsubstituted_placeholders():
    for over in ({}, {"live_mode": True, "write_token": "x"}):
        text = _page(**over).get("/").text
        assert not re.findall(r"__[A-Z_]+__", text)
        assert not re.findall(r"<!--[A-Z_]+-->", text)


def test_first_screen_copy():
    text = _page().get("/").text
    assert DESCRIPTION in text.replace("&#39;", "'")
    assert "Recommendations only." in text and "does not change any merchant system" in text
    for needle in ("Upload data", "Gateway export", "Merchant orders", ">Run<", "Results",
                   "Recommended actions", "Load sample data", "Gateway template (CSV)",
                   "Merchant template (CSV)", "Download corrections CSV", "Download review CSV"):
        assert needle in text, needle
    assert 'id="tab-reconcile" aria-controls="panel-reconcile" aria-selected="true"' in text


def test_page_order_matches_the_layout():
    text = _page().get("/").text
    order = [text.index(DESCRIPTION.split(",")[0]), text.index('id="howto"'), text.index('class="uploads"'),
             text.index('id="run"'), text.index('id="cards"'), text.index('class="grid results"')]
    assert order == sorted(order)


def test_how_it_works_is_collapsed_and_explains_every_verdict():
    text = _page().get("/").text
    start = text.index('<details class="howto" id="howto"')
    tag = text[start:text.index(">", start)]
    assert " open" not in tag
    body = text[start:text.index("</details>", start)]
    for code in ("AUTO_CORRECT", "CONSISTENT", "FLAGGED_RISKY", "DISPUTED", "CORRELATION_FAILED", "UNRESOLVED"):
        assert f"<code>{code}</code>" in body
    assert "Gateway captured but merchant behind</strong> is treated as safe" in body
    assert "Gateway failed but merchant shows paid</strong> is always flagged for a person" in body
    for step in ("Upload a gateway export and a merchant export (CSV or JSON), or load the sample.",
                 "compares the two statuses and amounts.", "Nothing is changed or stored."):
        assert step in body


def test_engine_terms_stay_off_the_main_screen():
    text = _page().get("/").text
    main_screen = text[:text.index('<details class="howto"')] + text[text.index("</details>"):]
    for term in ("drift=", "INDETERMINATE", "ARM_RECHECK", "FLAG_FOR_REVIEW"):
        assert term not in main_screen


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


def test_static_assets_and_the_font_are_served():
    c = _page()
    for name, kind in (("app.css", "text/css"), ("shell.js", "javascript"), ("tool.js", "javascript"),
                       ("activity.js", "javascript"), ("live.js", "javascript"),
                       ("fonts/inter-latin-wght-normal.woff2", "font/woff2")):
        r = c.get(f"/assets/{name}")
        assert r.status_code == 200 and kind in r.headers["content-type"], (name, r.headers.get("content-type"))
    assert c.get("/assets/nope.js").status_code == 404
    assert (DASH / "assets" / "fonts" / "LICENSE-Inter.txt").exists()


def test_every_element_id_used_by_the_scripts_exists_in_the_markup():
    html = (DASH / "index.html").read_text(encoding="utf-8") + (DASH / "live.html").read_text(encoding="utf-8")
    present = set(re.findall(r'\bid="([^"]+)"', html))
    for script in ("shell.js", "tool.js", "activity.js", "live.js"):
        src = (DASH / "assets" / script).read_text(encoding="utf-8")
        used = set(re.findall(r'(?:\$|getElementById)\(\s*"([A-Za-z0-9_-]+)"\s*\)', src))
        assert not used - present, (script, used - present)
    for which in ("Gateway", "Merchant"):
        for prefix in ("txt", "meta", "file", "drop"):
            assert f'id="{prefix}{which}"' in html


def test_visual_rules():
    assert '"Inter"' in CSS and "system-ui" in CSS
    assert "prefers-color-scheme: dark" in CSS
    assert "max-width: 760px" in CSS and ":focus-visible" in CSS
    assert "--content: 1080px" in CSS
    assert "serif" not in CSS_RULES.replace("sans-serif", ""), "no serif fonts"
    assert "italic" not in CSS_RULES
    assert "dashed" not in CSS_RULES
    assert "box-shadow" not in CSS_RULES and "gradient" not in CSS_RULES and "text-shadow" not in CSS_RULES
    assert "position: sticky" in CSS


def test_body_table_and_title_sizes():
    assert "font: 400 15px/24px var(--sans)" in CSS
    assert re.search(r"table\.grid \{[^}]*font-size: 14px", CSS)
    assert re.search(r"\.page-title \{[^}]*font-size: 24px;[^}]*font-weight: 600", CSS)


def test_upload_panels_have_solid_borders_and_no_numbered_circles():
    html = (DASH / "index.html").read_text(encoding="utf-8")
    assert "stepno" not in html and "stepno" not in CSS
    assert re.search(r"\.drop \{[^}]*border: 1px solid var\(--border\)", CSS)


def test_table_header_is_semantic_and_sortable():
    text = _page().get("/").text
    assert text.count('aria-sort="none"') == 5 and 'scope="col"' in text

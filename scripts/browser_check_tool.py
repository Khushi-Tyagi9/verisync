"""Drive the reconcile tool in headless Chrome and save screenshots.

Starts the app with NO usable database (POSTGRES_DSN points nowhere) to show the
tool needs none, then for each of desktop/phone x light/dark: loads the sample,
runs it, checks the results, expands a row, downloads both CSVs, and confirms the
page never scrolls sideways. The activity tab is visited once per theme to show
its error state without a database.

Needs Chrome and, in the venv you run it with, the project requirements plus
playwright (not a project dependency):

    .venv/Scripts/python.exe -m pip install playwright
    .venv/Scripts/python.exe scripts/browser_check_tool.py --out screenshots

Exit codes: 0 every check passed, 1 a check failed.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

VIEWPORTS = {"desktop": {"width": 1280, "height": 900}, "phone": {"width": 390, "height": 844}}
SCHEMES = ("light", "dark")

EXPECTED_CORRECTIONS = {"order_1002", "order_1010", "order_1014"}
EXPECTED_REVIEW = {"order_1003", "order_1004", "order_1005", "order_1006",
                   "order_1007", "order_1008", "order_1011", "order_1013"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", default="screenshots")
    parser.add_argument("--channel", default="chrome")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    from playwright.sync_api import sync_playwright

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {
        **os.environ,
        "POSTGRES_DSN": "postgresql://nobody:none@127.0.0.1:1/none",
        "DASHBOARD_POLL_INTERVAL_MS": "1500",
    }
    for k in ("DASHBOARD_LIVE_MODE", "DASHBOARD_DEMO_MODE", "DASHBOARD_WRITE_TOKEN"):
        env.pop(k, None)
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app:app", "--port", str(port)],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    failures: list[str] = []

    def check(ok: bool, msg: str) -> None:
        print(f"  [{'ok' if ok else 'FAIL'}] {msg}")
        if not ok:
            failures.append(msg)

    try:
        base = f"http://127.0.0.1:{port}"
        for _ in range(60):
            try:
                urllib.request.urlopen(base + "/health", timeout=2)
                break
            except OSError:
                time.sleep(0.5)

        with sync_playwright() as p:
            browser = p.chromium.launch(channel=args.channel, headless=True)
            for scheme in SCHEMES:
                for vp_name, vp in VIEWPORTS.items():
                    tag = f"{vp_name}-{scheme}"
                    print(tag)
                    ctx = browser.new_context(viewport=vp, color_scheme=scheme, accept_downloads=True)
                    page = ctx.new_page()
                    problems: list[str] = []
                    page.on("pageerror", lambda e: problems.append(str(e)))
                    page.on("console", lambda m: m.type == "error" and "Failed to load resource" not in m.text and problems.append(m.text))
                    page.goto(base)

                    page.keyboard.press("Tab")
                    check(page.evaluate("document.activeElement.className") == "skip", "first Tab stop is the skip link")
                    page.keyboard.press("Escape")
                    page.evaluate("document.activeElement.blur()")

                    layout = page.evaluate("""() => {
                      const r = (s) => document.querySelector(s).getBoundingClientRect();
                      const tabs = document.querySelector('.tabs');
                      const wm = document.querySelector('.wordmark');
                      return {
                        scrollY: window.scrollY, topbar: r('.topbar').top, wordmark: r('.wordmark').top,
                        wmOverflow: wm.scrollHeight - wm.clientHeight,
                        tabsOverflowY: tabs.scrollHeight - tabs.clientHeight,
                        tabsStyle: getComputedStyle(tabs).overflowY,
                        font: getComputedStyle(document.body).fontFamily,
                        interLoaded: document.fonts.check('15px Inter'),
                        howtoOpen: document.getElementById('howto').open,
                        titleGap: document.querySelector('#panel-reconcile .page-title').getBoundingClientRect().top - r('.topbar').bottom,
                      };
                    }""")
                    check(layout["scrollY"] == 0 and layout["topbar"] >= 0 and layout["wordmark"] >= 0 and layout["wmOverflow"] <= 0,
                          f"header is fully visible, nothing clipped ({layout})")
                    check(layout["tabsOverflowY"] <= 0 and layout["tabsStyle"] == "hidden",
                          "tab row has no vertical overflow, so no stray scrollbar")
                    check(layout["font"].startswith('"Inter"') or layout["font"].startswith("Inter"), "Inter is the first font")
                    page.evaluate("document.fonts.ready")
                    check(page.evaluate("document.fonts.check('15px Inter')"), "self-hosted Inter loaded")
                    check(layout["howtoOpen"] is False, "How it works starts collapsed")
                    check(layout["titleGap"] >= 24, f"page title has room below the top bar ({layout['titleGap']:.0f}px)")
                    page.screenshot(path=str(out / f"{tag}-1-empty.png"), full_page=True)

                    page.click("#howto > summary")
                    howto = page.inner_text("#howto")
                    check(all(c in howto for c in ("AUTO_CORRECT", "CONSISTENT", "FLAGGED_RISKY", "DISPUTED",
                                                    "CORRELATION_FAILED", "UNRESOLVED")),
                          "How it works explains all six verdicts")
                    page.screenshot(path=str(out / f"{tag}-0-how-it-works.png"), full_page=True)
                    page.click("#howto > summary")

                    page.click("#loadSample")
                    page.wait_for_function("document.getElementById('txtGateway').value.length > 50")
                    check(not page.is_disabled("#run"), "Run is enabled once both inputs are filled")
                    page.click("#run")
                    page.wait_for_selector("#rows tr.datarow", state="attached")
                    n_rows = page.locator("#rows tr.datarow").count()
                    check(n_rows == 14, f"14 order rows rendered (got {n_rows})")
                    chips = page.locator("#chips .chip").count()
                    check(chips == 7, f"filter chips: All + 6 verdicts (got {chips})")
                    check(page.locator("#invalidBox").is_visible(), "skipped rows are listed, not hidden")
                    items = page.locator("#cards .item")
                    check(items.count() == 7, f"summary row: total + 6 verdicts ({items.count()})")
                    if vp_name == "desktop":
                        tops = page.eval_on_selector_all("#cards .item", "els => els.map(e => e.offsetTop)")
                        check(len(set(tops)) == 1, "summary counts sit in a single horizontal row")
                    main_text = page.inner_text("#rows")
                    check("drift=" not in main_text and "INDETERMINATE" not in main_text and "!=" not in main_text,
                          "no engine terms in the collapsed results table")
                    page.screenshot(path=str(out / f"{tag}-2-results.png"), full_page=True)

                    page.click("#chips .chip >> text=Data conflict")
                    check(page.locator("#rows tr.datarow").count() == 2, "verdict chip filters the table")
                    page.fill("#q", "1007")
                    check(page.locator("#rows tr.datarow").count() == 1, "search narrows the table")
                    page.fill("#q", "")
                    page.click("#chips .chip >> text=All")

                    btn = page.locator("#rows .expander", has_text="order_1006")
                    btn.click()
                    check(btn.get_attribute("aria-expanded") == "true", "row expands")
                    trail = page.locator("tr.trailrow:not([hidden]) ol.trail")
                    check(trail.count() == 1 and trail.is_visible() and "FLAG_FOR_REVIEW" in trail.inner_text(),
                          "decision trail is shown with the engine's actions")
                    reason = page.locator("#rows tr.datarow", has_text="order_1006").locator("td.reason").inner_text()
                    check("disagree on the amount" in reason and "!=" not in reason, f"plain reason on screen ({reason!r})")
                    check("!=" in page.inner_text("tr.trailrow:not([hidden])"), "engine detail stays inside the expanded trail")
                    page.screenshot(path=str(out / f"{tag}-3-expanded.png"), full_page=True)

                    if vp_name == "phone":
                        page.select_option("#sortSel", "amount:1")   # header is collapsed on phones
                    else:
                        page.click("th[data-sort=amount] button")
                    check(page.get_attribute("th[data-sort=amount]", "aria-sort") == "ascending", "sorting by amount sets aria-sort")
                    amounts = page.eval_on_selector_all("#rows tr.datarow td.money", "tds => tds.map(t => t.innerText.replace(/[^0-9.]/g, ''))")
                    check([float(a) for a in amounts] == sorted(float(a) for a in amounts), "rows are in amount order")

                    with page.expect_download() as d:
                        page.click("#dlCorrections")
                    rows = list(csv.DictReader(io.StringIO(Path(d.value.path()).read_text(encoding="utf-8"))))
                    check({r["order_id"] for r in rows} == EXPECTED_CORRECTIONS and
                          all(r["recommended_action"] == "mark_paid" for r in rows),
                          f"corrections CSV has the 3 mark_paid orders ({len(rows)} rows)")
                    check({r["order_id"]: r["amount_rupees"] for r in rows}.get("order_1014") == "220.10",
                          "corrections CSV amounts are exact")
                    with page.expect_download() as d:
                        page.click("#dlReview")
                    rows = list(csv.DictReader(io.StringIO(Path(d.value.path()).read_text(encoding="utf-8"))))
                    check({r["order_id"] for r in rows} == EXPECTED_REVIEW, f"review CSV has the 8 orders needing a person ({len(rows)} rows)")

                    if vp_name == "desktop":
                        page.evaluate("document.querySelector('table.results tbody tr:nth-child(12)').scrollIntoView({block: 'start'})")
                        page.evaluate("window.scrollBy(0, -10)")
                        head_top = page.evaluate("document.querySelector('table.results thead th').getBoundingClientRect().top")
                        check(-1 <= head_top <= 1, f"table header sticks to the top while scrolling ({head_top:.1f}px)")
                        page.evaluate("window.scrollTo(0, 0)")
                    overflow = page.evaluate("document.documentElement.scrollWidth - window.innerWidth")
                    check(overflow <= 0, f"no horizontal page scroll (overflow {overflow}px)")

                    if vp_name == "desktop" or scheme == "dark":
                        page.click("#tab-activity")
                        page.wait_for_selector("#actError:not([hidden])", timeout=30000)
                        check("unavailable" in page.inner_text("#actError"), "activity tab shows a designed error state without a database")
                        page.screenshot(path=str(out / f"{tag}-4-activity.png"), full_page=True)
                        page.click("#tab-reconcile")

                    if tag == "desktop-light":
                        page.click("[data-clear=Gateway]")
                        page.fill("#txtGateway", "order_id,state\nA,captured\n")
                        page.click("#run")
                        page.wait_for_selector("#resError:not([hidden])")
                        check("amount" in page.inner_text("#resError"), "bad input shows a clear error")
                        page.screenshot(path=str(out / f"{tag}-5-error.png"), full_page=True)

                        # A plain-text 500 (what a broken deployment returns) and a
                        # JSON error must both show a readable message, never "null".
                        page.route("**/api/reconcile", lambda r: r.fulfill(
                            status=500, content_type="text/plain; charset=utf-8", body="Internal Server Error"))
                        page.click("#run")
                        page.wait_for_function("document.getElementById('resError').innerText.includes('500')")
                        text = page.inner_text("#resError")
                        check("null" not in text and "server hit an error (500)" in text,
                              f"text 500 shows a readable message ({text!r})")
                        page.unroute("**/api/reconcile")
                        page.route("**/api/reconcile", lambda r: r.fulfill(
                            status=503, content_type="application/json",
                            body='{"error": "The reconcile engine is not part of this deployment. Root Directory."}'))
                        page.click("#run")
                        page.wait_for_function("document.getElementById('resError').innerText.includes('Root Directory')")
                        text = page.inner_text("#resError")
                        check("null" not in text and "not part of this deployment" in text,
                              f"JSON error shows the server's message ({text!r})")
                        page.unroute("**/api/reconcile")

                    check(not problems, f"no page errors ({problems[:2]})")
                    ctx.close()
            browser.close()
    finally:
        server.terminate()

    print(f"screenshots in {out.resolve()}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

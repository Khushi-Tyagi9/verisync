"""Drive the live order form in headless Chrome and check the outcome table.

Starts the app (DASHBOARD_LIVE_MODE=1, short timings) against a throwaway
database on the compose Postgres, submits every scenario through the real form,
waits for all outcomes to render, and saves a screenshot. Needs Chrome
installed and, in the venv you run it with, the project requirements plus
playwright (not a project dependency):

    .venv/Scripts/python.exe -m pip install playwright
    .venv/Scripts/python.exe scripts/browser_check.py --screenshot live_orders.png

Exit codes: 0 every scenario rendered its expected outcome, 1 a check failed.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ADMIN_DSN = os.environ.get(
    "LITE_TEST_ADMIN_DSN", "postgresql://verisync:verisync@localhost:15432/postgres"
)

# option label -> outcome label shown in the table
EXPECT = {
    "Normal (merchant pays at once)": "Resolved naturally",
    "Lagging merchant": "Auto-corrected",
    "Stuck merchant (never pays)": "Auto-corrected",
    "Silent merchant (no record)": "Dead-lettered",
    "Risky drift (gateway failed, merchant paid)": "Flagged: unsafe drift",
    "Refund on a settled order": "Disputed",
    "Correlation failure (amount mismatch)": "Flagged: amount mismatch",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--screenshot", default="live_orders.png")
    parser.add_argument("--channel", default="chrome", help="playwright browser channel")
    args = parser.parse_args()

    import psycopg
    from playwright.sync_api import sync_playwright

    name = f"verisync_browser_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = ADMIN_DSN.rsplit("/", 1)[0] + "/" + name
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = {
        **os.environ,
        "POSTGRES_DSN": dsn,
        "DASHBOARD_LIVE_MODE": "1",
        "DASHBOARD_LIVE_RECHECK_SECONDS": "3",
        "DASHBOARD_LIVE_LAG_SECONDS": "8",
        "DASHBOARD_LIVE_IMMEDIATE_SECONDS": "1",
        "DASHBOARD_POLL_INTERVAL_MS": "1500",
    }
    env.pop("DASHBOARD_WRITE_TOKEN", None)
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
            page = browser.new_page(viewport={"width": 1280, "height": 1500})
            errors: list[str] = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto(base)
            page.click("#tab-live")
            page.wait_for_selector("#liveScenario option", state="attached")

            check("Live demo: serverless mode. Postgres only." in page.inner_text(".badge-live"),
                  "serverless badge shown")
            check(not page.is_visible("#liveTokenWrap"), "write token field hidden when no token is configured")

            for label in EXPECT:
                page.select_option("#liveScenario", label=label)
                page.click("#liveGo")
                page.wait_for_function(
                    "document.getElementById('liveMsg').textContent.startsWith('created')"
                )

            deadline = time.monotonic() + 90
            rows: list[list[str]] = []
            while time.monotonic() < deadline:
                rows = page.eval_on_selector_all(
                    "#liveRows tr",
                    "trs => trs.map(t => [...t.children].map(c => c.innerText.trim()))",
                )
                if len(rows) == len(EXPECT) and all(
                    "as expected" in r[2] or "unexpected" in r[2] for r in rows
                ):
                    break
                time.sleep(1)
            time.sleep(3)  # let the summary cards catch up before the screenshot
            page.screenshot(path=args.screenshot, full_page=True)
            browser.close()

        check(len(rows) == len(EXPECT), f"{len(rows)} outcome rows rendered (want {len(EXPECT)})")
        got = {r[1]: r[2] for r in rows}
        for label, want in EXPECT.items():
            cell = got.get(label, "<missing>").replace("\n", " ")
            check(cell.startswith(want) and "as expected" in cell, f"{label}: {cell}")
        check(not errors, f"no page errors ({errors})")
    finally:
        server.terminate()
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()

    print(f"screenshot: {Path(args.screenshot).resolve()}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

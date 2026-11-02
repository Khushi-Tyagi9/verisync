"""Simulate the Vercel deployment layout and drive it over HTTP.

Needs Postgres from the compose stack on the host (default port 15432) and
network access for pip:

    python scripts/vercel_sim.py

What it does:
  1. Copies the repo files Vercel would deploy (tracked + untracked, minus
     .gitignore) into a temp directory: Root Directory = repo root.
  2. Creates a clean venv there and installs ONLY requirements.txt, then
     asserts confluent-kafka and redis are not installed.
  3. Creates a throwaway database (schema.sql applied, dropped afterwards).
  4. Runs `uvicorn app:app` from the copy (the root app.py entrypoint) with
     DASHBOARD_LIVE_MODE=1 and a write token, then checks /health, /, the
     token gate, POST /api/orders, POST /api/tick and the final outcomes.

Exit codes: 0 all checks passed, 1 a check failed, 2 setup failure.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ADMIN_DSN = os.environ.get(
    "LITE_TEST_ADMIN_DSN", "postgresql://verisync:verisync@localhost:15432/postgres"
)
TOKEN = "sim-token"
EXPECT = {
    "normal": "RESOLVED_NATURALLY",
    "stuck": "RESOLVED_AUTOCORRECTED",
    "risky_drift": "FLAGGED_UNSAFE_DRIFT",
    "reversal": "DISPUTED",
    "correlation_failure": "FLAGGED_AMOUNT_MISMATCH",
}

failures: list[str] = []


def check(ok: bool, msg: str) -> None:
    print(f"  [{'ok' if ok else 'FAIL'}] {msg}")
    if not ok:
        failures.append(msg)


def call(base, method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(base + path, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw[:1] in "{[" else raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        return exc.code, (json.loads(raw) if raw[:1] in "{[" else raw)


def copy_deployable(dest: Path) -> int:
    files = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.splitlines()
    n = 0
    for rel in files:
        src = ROOT / rel
        if src.is_file():
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)
            n += 1
    return n


def main() -> int:
    import psycopg

    work = Path(tempfile.mkdtemp(prefix="verisync_vercel_sim_"))
    deploy = work / "deploy"
    deploy.mkdir()
    name = f"verisync_sim_{uuid.uuid4().hex[:8]}"
    server = None
    admin = None
    try:
        print(f"copied {copy_deployable(deploy)} files to {deploy}")
        subprocess.run([sys.executable, "-m", "venv", str(work / "venv")], check=True)
        vpy = work / "venv" / ("Scripts" if os.name == "nt" else "bin") / "python"
        subprocess.run([str(vpy), "-m", "pip", "install", "-q", "-r", "requirements.txt"],
                       cwd=deploy, check=True)
        frozen = subprocess.run([str(vpy), "-m", "pip", "freeze"], capture_output=True, text=True).stdout.lower()
        check("confluent-kafka" not in frozen and "redis" not in frozen,
              "clean venv has neither confluent-kafka nor redis")

        admin = psycopg.connect(ADMIN_DSN, autocommit=True, connect_timeout=3)
        admin.execute(f'CREATE DATABASE "{name}"')
        head, _, _ = ADMIN_DSN.rpartition("/")
        dsn = f"{head}/{name}"
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute((ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        env = {
            **{k: v for k, v in os.environ.items() if not k.startswith(("DASHBOARD_", "POSTGRES_"))},
            "POSTGRES_DSN": dsn,
            "DASHBOARD_LIVE_MODE": "1",
            "DASHBOARD_WRITE_TOKEN": TOKEN,
            "DASHBOARD_LIVE_RECHECK_SECONDS": "3",
            "DASHBOARD_LIVE_LAG_SECONDS": "8",
            "DASHBOARD_LIVE_IMMEDIATE_SECONDS": "1",
        }
        server = subprocess.Popen(
            [str(vpy), "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=deploy, env=env,
        )
        base = f"http://127.0.0.1:{port}"
        for _ in range(60):
            try:
                if call(base, "GET", "/health")[0] == 200:
                    break
            except OSError:
                time.sleep(0.5)
        else:
            print("server never became healthy", file=sys.stderr)
            return 2

        print("checks:")
        check(call(base, "GET", "/health") == (200, {"status": "ok"}), "GET /health")
        code, page = call(base, "GET", "/")
        check(code == 200 and "Live demo: serverless mode" in page, "GET / serves the live panel")
        code, css = call(base, "GET", "/assets/app.css")
        check(code == 200 and "--v-auto-ink" in css, "GET /assets/app.css serves the stylesheet")
        code, sample = call(base, "GET", "/api/reconcile/sample")
        check(code == 200 and "order_1001" in sample["gateway"], "GET /api/reconcile/sample")
        code, rec = call(base, "POST", "/api/reconcile", {**sample, "amount_unit": "rupees"})
        counts = {k: v["count"] for k, v in rec["summary"]["by_verdict"].items()} if code == 200 else {}
        check(counts == {"AUTO_CORRECT": 3, "CONSISTENT": 3, "FLAGGED_RISKY": 1, "DISPUTED": 2,
                         "CORRELATION_FAILED": 2, "UNRESOLVED": 3},
              f"POST /api/reconcile on the sample, no token needed ({counts})")
        order = {"amount": 50_000, "scenario": "normal", "count": 1}
        check(call(base, "POST", "/api/orders", order)[0] == 401, "POST /api/orders without token -> 401")
        check(call(base, "POST", "/api/orders", {**order, "count": 11}, {"X-Write-Token": TOKEN})[0] == 422,
              "count above 10 -> 422")

        ids = {}
        for scenario in EXPECT:
            code, body = call(base, "POST", "/api/orders",
                              {"amount": 50_000, "scenario": scenario, "count": 1},
                              {"X-Write-Token": TOKEN})
            check(code == 201, f"POST /api/orders scenario={scenario} -> {code}")
            if code == 201:
                ids[scenario] = body["order_ids"][0]

        deadline = time.monotonic() + 90
        outcomes = {}
        while time.monotonic() < deadline:
            call(base, "POST", "/api/tick")
            _, body = call(base, "GET", "/api/live/orders")
            outcomes = {o["order_id"]: o["outcome"] for o in body["orders"]}
            if all(outcomes.get(i, {}).get("terminal") for i in ids.values()):
                break
            time.sleep(1.1)  # the server throttles ticks to one per second
        tick = call(base, "POST", "/api/tick")
        check(tick[0] == 200, "POST /api/tick -> 200")
        for scenario, oid in ids.items():
            got = outcomes.get(oid, {}).get("tag")
            check(got == EXPECT[scenario], f"{scenario}: {got} (want {EXPECT[scenario]})")
    except Exception as exc:
        print(f"setup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(10)
            except subprocess.TimeoutExpired:
                server.kill()
        if admin is not None:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            admin.close()
        shutil.rmtree(work, ignore_errors=True)

    if failures:
        print(f"\nFAIL: {len(failures)} check(s) failed")
        return 1
    print("\nOK: the Vercel layout works with only requirements.txt installed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

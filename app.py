"""Vercel entrypoint. Vercel's Root Directory is the repo root, so decision/,
worker/, merchant_sim/ and lite/ are bundled next to dashboard/. The Python
runtime looks for a top-level `app` in a root-level app.py.

Not used by docker compose, which runs `python -m dashboard.app`.
"""

from dashboard.app import app  # noqa: F401

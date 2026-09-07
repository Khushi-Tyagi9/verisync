"""verisync reconciliation worker (Phase 4).

Consumes the unified ``order-lifecycle-events`` topic, runs the pure decision
logic from ``decision/`` against per-order state in Postgres, and manages the
recheck delay queue plus its two fallback sweeps.

All I/O lives here. The ``decision/`` package it drives stays infra-free.
"""

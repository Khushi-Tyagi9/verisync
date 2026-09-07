"""The recheck runner: the fast path that resolves armed rechecks when their
delay window expires.

It polls the Redis delay queue for due entries and, for each one:

1. Claims the row with a fresh fencing token (atomic UPDATE, its own
   transaction, so the PROCESSING+token state is durable before any work).
2. Re-fetches current state from Postgres. It never acts on the state captured
   when the recheck was scheduled.
3. Re-runs the decision logic with ``require_merchant_record=True`` (a merchant
   record still absent at recheck time is a genuine anomaly, not lag).
4. Applies the outcome with token verification. If the completion UPDATE matches
   zero rows the claim was stolen by the stuck-PROCESSING sweep; it writes
   nothing to audit_log and moves on.

The Postgres fallback sweep in worker/sweep.py is the backstop for anything that
never reached the Redis queue (a crash between the orders_state write and the
push).

Run it with:  python -m worker.recheck
"""

from __future__ import annotations

import dataclasses
import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from decision.engine import evaluate
from decision.models import Action, Event, OrderView, ReconStatus, ReviewItem, Source

from .config import WorkerSettings, load_settings
from .delayqueue import RedisDelayQueue
from .state import PostgresStateStore

log = logging.getLogger("verisync.recheck")

_TERMINAL_ACTIONS = {
    Action.APPLY_AUTOCORRECT,
    Action.MARK_RESOLVED_NATURALLY,
    Action.ROUTE_DISPUTED,
}


@dataclasses.dataclass(frozen=True)
class _Outcome:
    # A datetime => re-arm and re-push the queue at this score. None => done
    # (terminal, dead-lettered, or claim lost).
    rearm_at: Optional[datetime] = None


class RecheckRunner:
    def __init__(
        self,
        *,
        store: PostgresStateStore,
        delay_queue: RedisDelayQueue,
        settings: WorkerSettings,
    ) -> None:
        self._store = store
        self._dq = delay_queue
        self._settings = settings
        self._recheck_delay = timedelta(seconds=settings.recheck_delay_seconds)
        self._max_retries = settings.max_reclaim_retries
        self._stopping = False

    @classmethod
    def from_settings(cls, settings: Optional[WorkerSettings] = None) -> "RecheckRunner":
        from .consumer import build_redis  # shared client factory

        settings = settings or load_settings()
        return cls(
            store=PostgresStateStore.from_settings(settings),
            delay_queue=RedisDelayQueue(build_redis(settings)),
            settings=settings,
        )

    def request_stop(self, *_signal_args: Any) -> None:
        self._stopping = True

    def run(self, *, poll_interval: float = 2.0) -> None:
        log.info("recheck runner up: delay=%ss", self._settings.recheck_delay_seconds)
        try:
            while not self._stopping:
                fired = self.run_once()
                if fired == 0:
                    time.sleep(poll_interval)
        finally:
            self.close()

    def run_once(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        due = self._dq.due(now)
        for order_id in due:
            try:
                self.resolve_order(order_id, now)
            except Exception:
                log.exception("recheck failed for %s; leaving it for the sweep", order_id)
        return len(due)

    # ------------------------------------------------------------------ #
    def resolve_order(self, order_id: str, now: datetime) -> None:
        """Claim, re-fetch, re-evaluate, and complete one armed recheck. The
        same path for the fast queue and the Postgres fallback sweep, so both
        go through the identical atomic claim and token-verified completion."""
        with self._store.connection() as conn:
            token = self._store.claim_recheck(conn, order_id)
        if token is None:
            # Already claimed, resolved, or no longer pending. Drop the stale
            # queue entry; Postgres is the authority.
            self._dq.remove(order_id)
            log.debug("no claim for %s (not pending); dropped from queue", order_id)
            return

        with self._store.connection() as conn:
            state = self._store.read_state(conn, order_id)
        view = state.view or OrderView.empty(order_id)

        synthetic = Event(
            event_id=f"system:recheck:{order_id}:{token}",
            source=Source.SYSTEM,
            event_type="recheck.fired",
            order_id=order_id,
            occurred_at=now,
        )
        evaluation = evaluate(
            event=synthetic,
            view=view,
            current_status=ReconStatus.PROCESSING,
            now=now,
            recheck_delay=self._recheck_delay,
            require_merchant_record=True,
        )
        decision = evaluation.decision

        with self._store.connection() as conn:
            outcome = self._apply(conn, order_id, token, decision)

        if outcome.rearm_at is not None:
            # A deliberate fresh window after the prior one fired: force the new
            # (later) score rather than letting "earliest wins" drop it.
            self._dq.push(order_id, outcome.rearm_at, only_if_earlier=False)
            log.info("recheck %s -> re-armed until %s", order_id, outcome.rearm_at.isoformat())
        else:
            self._dq.remove(order_id)
            log.info("recheck %s -> %s", order_id, decision.action.value)

    def _apply(self, conn, order_id: str, token: str, decision) -> _Outcome:
        action = decision.action

        if action in _TERMINAL_ACTIONS:
            new_status = decision.new_status.value
            if not self._store.complete_claim(
                conn, order_id=order_id, token=token, new_status=new_status
            ):
                log.warning("claim stolen for %s before completion; no audit written", order_id)
                return _Outcome()
            self._store.insert_audit(conn, decision.audit)
            return _Outcome()

        if action is Action.FLAG_FOR_REVIEW:
            # Merchant record still absent at recheck: a definitive anomaly, not
            # lag. Retire the pointer to DEAD_LETTER and queue it for a human.
            if not self._store.complete_claim(
                conn, order_id=order_id, token=token, new_status="DEAD_LETTER"
            ):
                log.warning("claim stolen for %s before flag; no audit written", order_id)
                return _Outcome()
            audit = dataclasses.replace(
                decision.audit, decision="FLAG_FOR_REVIEW_DEAD_LETTER", new_state="DEAD_LETTER"
            )
            self._store.insert_audit(conn, audit)
            if decision.review is not None:
                self._store.enqueue_review(conn, decision.review)
            return _Outcome()

        if action is Action.ARM_RECHECK:
            fresh = decision.recheck_at or (
                datetime.now(timezone.utc) + self._recheck_delay
            )
            res = self._store.rearm_or_deadletter(
                conn,
                order_id=order_id,
                token=token,
                fresh_recheck_at=fresh,
                max_retries=self._max_retries,
            )
            if res is None:
                log.warning("claim stolen for %s during re-arm; no audit written", order_id)
                return _Outcome()
            new_status, retry_count = res
            if new_status == "DEAD_LETTER":
                audit = dataclasses.replace(
                    decision.audit, decision="RECHECK_RETRIES_EXHAUSTED", new_state="DEAD_LETTER"
                )
                self._store.insert_audit(conn, audit)
                self._store.enqueue_review(
                    conn,
                    ReviewItem(
                        order_id=order_id,
                        reason="dead_letter",
                        event_id=decision.audit.event_id,
                        details={"retry_count": retry_count, "last_reason": decision.reason},
                    ),
                )
                return _Outcome()
            self._store.insert_audit(conn, decision.audit)
            return _Outcome(rearm_at=fresh)

        # decide() does not produce NO_OP / LOCKED_REPLAY_IGNORED for a
        # PROCESSING row, but never leave a claim dangling if that changes.
        log.warning("unexpected %s at recheck for %s; re-arming", action.value, order_id)
        fresh = datetime.now(timezone.utc) + self._recheck_delay
        res = self._store.rearm_or_deadletter(
            conn, order_id=order_id, token=token,
            fresh_recheck_at=fresh, max_retries=self._max_retries,
        )
        return _Outcome(rearm_at=fresh if res and res[0] != "DEAD_LETTER" else None)

    def close(self) -> None:
        for name, obj in (("store", self._store), ("delay_queue", self._dq)):
            try:
                obj.close()
            except Exception as exc:
                log.warning("error closing %s: %s", name, exc)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    runner = RecheckRunner.from_settings()
    signal.signal(signal.SIGINT, runner.request_stop)
    signal.signal(signal.SIGTERM, runner.request_stop)
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""The two Postgres fallback sweeps. Both run on a slow cadence (minutes, not
seconds); they are backstops, not the primary path.

* Stuck-PROCESSING recovery: a worker claimed a row and never finished. Reclaim
  it to PENDING_RECHECK past the watchdog timeout, clearing claim_token so the
  dead worker's token can never match a future claim, and count the retry. At
  the ceiling, route to DEAD_LETTER and raise a review instead of reclaiming
  forever.
* PENDING_RECHECK fallback: an armed recheck whose Redis push was lost to a
  crash between the orders_state write and the queue push. Drive it through the
  exact same claim + token-verified completion the fast runner uses.

Order matters: reclaim stuck rows first, so a row recovered to PENDING_RECHECK
with a now-past recheck_at is picked up by the second sweep in the same pass.

Run it with:  python -m worker.sweep
"""

from __future__ import annotations

import dataclasses
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Optional

from decision.models import AuditEntry, ReviewItem, Source

from .config import WorkerSettings, load_settings
from .delayqueue import RedisDelayQueue
from .recheck import RecheckRunner
from .state import PostgresStateStore

log = logging.getLogger("verisync.sweep")


class StuckProcessingSweep:
    def __init__(self, *, store: PostgresStateStore, settings: WorkerSettings) -> None:
        self._store = store
        self._timeout = settings.stuck_processing_timeout_seconds
        self._max_retries = settings.max_reclaim_retries

    def run_once(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        with self._store.connection() as conn:
            reclaimed = self._store.reclaim_stuck_processing(
                conn, timeout_seconds=self._timeout, max_retries=self._max_retries
            )
            for order_id, new_status, retry_count in reclaimed:
                self._store.insert_audit(
                    conn,
                    AuditEntry(
                        event_id=f"system:sweep:stuck:{order_id}:{retry_count}",
                        order_id=order_id,
                        source=Source.SYSTEM,
                        event_type="sweep.stuck_processing",
                        decision="DEAD_LETTER" if new_status == "DEAD_LETTER" else "RECLAIM",
                        direction="NEUTRAL",
                        old_state="PROCESSING",
                        new_state=new_status,
                    ),
                )
                if new_status == "DEAD_LETTER":
                    self._store.enqueue_review(
                        conn,
                        ReviewItem(
                            order_id=order_id,
                            reason="dead_letter",
                            details={
                                "retry_count": retry_count,
                                "source": "stuck_processing_sweep",
                            },
                        ),
                    )
        for order_id, new_status, retry_count in reclaimed:
            log.info(
                "stuck-PROCESSING %s -> %s (retry_count=%d)", order_id, new_status, retry_count
            )
        return len(reclaimed)


class PendingRecheckSweep:
    def __init__(
        self,
        *,
        store: PostgresStateStore,
        runner: RecheckRunner,
        settings: WorkerSettings,
    ) -> None:
        self._store = store
        self._runner = runner
        self._grace = settings.pending_sweep_grace_seconds

    def run_once(self, now: Optional[datetime] = None) -> int:
        now = now or datetime.now(timezone.utc)
        with self._store.connection() as conn:
            orphans = self._store.list_orphaned_pending(conn, grace_seconds=self._grace)
        for order_id in orphans:
            try:
                self._runner.resolve_order(order_id, now)
            except Exception:
                log.exception("fallback recheck failed for %s", order_id)
        if orphans:
            log.info("PENDING_RECHECK fallback resolved %d orphan(s)", len(orphans))
        return len(orphans)


@dataclasses.dataclass
class SweepService:
    stuck: StuckProcessingSweep
    pending: PendingRecheckSweep
    settings: WorkerSettings
    _stopping: bool = False

    @classmethod
    def from_settings(cls, settings: Optional[WorkerSettings] = None) -> "SweepService":
        from .consumer import build_redis

        settings = settings or load_settings()
        store = PostgresStateStore.from_settings(settings)
        runner = RecheckRunner(
            store=store,
            delay_queue=RedisDelayQueue(build_redis(settings)),
            settings=settings,
        )
        return cls(
            stuck=StuckProcessingSweep(store=store, settings=settings),
            pending=PendingRecheckSweep(store=store, runner=runner, settings=settings),
            settings=settings,
        )

    def request_stop(self, *_signal_args: Any) -> None:
        self._stopping = True

    def run_once(self, now: Optional[datetime] = None) -> tuple[int, int]:
        now = now or datetime.now(timezone.utc)
        reclaimed = self.stuck.run_once(now)
        resolved = self.pending.run_once(now)
        return reclaimed, resolved

    def run(self) -> None:
        interval = self.settings.sweep_interval_seconds
        log.info(
            "sweeps up: interval=%ss stuck_timeout=%ss pending_grace=%ss max_retries=%d",
            interval,
            self.settings.stuck_processing_timeout_seconds,
            self.settings.pending_sweep_grace_seconds,
            self.settings.max_reclaim_retries,
        )
        try:
            while not self._stopping:
                try:
                    self.run_once()
                except Exception:
                    log.exception("sweep pass failed; retrying next interval")
                for _ in range(int(interval)):
                    if self._stopping:
                        break
                    time.sleep(1)
        finally:
            self.close()

    def close(self) -> None:
        # The runner owns the shared store and the delay queue; closing it
        # covers both. stuck._store is the same object.
        try:
            self.pending._runner.close()
        except Exception as exc:
            log.warning("error during sweep shutdown: %s", exc)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s %(message)s",
    )
    service = SweepService.from_settings()
    signal.signal(signal.SIGINT, service.request_stop)
    signal.signal(signal.SIGTERM, service.request_stop)
    service.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

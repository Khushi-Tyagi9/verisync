"""Lite mode with fakes: scenario plans against the pure decision layer, the
processor wiring, the tick lock, and the order caps. No database."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from decision.engine import evaluate
from decision.models import Action, ReconStatus
from lite.config import MAX_ORDERS_PER_REQUEST, LiteSettings
from lite.outcomes import classify
from lite.processor import LiteProcessor, NoopDelayQueue
from lite.scenarios import SCENARIOS, build_plan, new_order_id, scenario_of
from lite.service import CAP_LOCK_KEY, TICK_LOCK_KEY, CapExceeded, LiteError, LiteService
from worker.config import load_settings as load_worker_settings
from worker.normalize import envelope_to_event

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def _plan(scenario, amount=50_000, order_id=None):
    order_id = order_id or new_order_id(scenario)
    return order_id, build_plan(
        scenario, order_id, amount, now=NOW, immediate_seconds=2, lag_seconds=60
    )


# --------------------------------------------------------------------------- #
# scenarios
# --------------------------------------------------------------------------- #
def test_order_ids_carry_prefix_and_scenario():
    for key in SCENARIOS:
        oid = new_order_id(key)
        assert oid.startswith("order_LIVE_")
        assert scenario_of(oid) == key
    assert scenario_of("order_LIVE_bogus_0123456789ab") is None
    assert scenario_of("order_other") is None


def test_every_scenario_profile_satisfies_the_merchant_orders_check():
    assert {s.profile for s in SCENARIOS.values()} <= {"immediate", "lagging", "stuck", "silent"}


@pytest.mark.parametrize("key", list(SCENARIOS))
def test_every_envelope_normalizes_to_an_event(key):
    order_id, plan = _plan(key)
    assert plan.envelopes
    ids = set()
    for env in plan.envelopes:
        ev = envelope_to_event(env)
        assert ev is not None and ev.order_id == order_id
        ids.add(ev.event_id)
    assert len(ids) == len(plan.envelopes)


def test_timer_driven_scenarios_schedule_the_paid_transition():
    assert _plan("normal")[1].next_action_at == NOW + timedelta(seconds=2)
    assert _plan("lagging")[1].next_action_at == NOW + timedelta(seconds=60)
    for key in ("stuck", "silent", "risky_drift", "reversal", "correlation_failure"):
        assert _plan(key)[1].next_action_at is None
    assert _plan("risky_drift")[1].paid_now and _plan("reversal")[1].paid_now


def _fold(plan_key):
    """Run a scenario's immediate events through the pure engine, as the
    processor would, and return the decision list and last status."""
    order_id, plan = _plan(plan_key)
    view, status, decisions = None, None, []
    for env in plan.envelopes:
        ev = envelope_to_event(env)
        res = evaluate(event=ev, view=view, current_status=status, now=NOW,
                       recheck_delay=timedelta(seconds=20))
        view = res.view
        d = res.decision
        decisions.append(d)
        if d.new_status is not None:
            status = d.new_status
    return decisions, status


def test_risky_drift_is_flagged_never_autocorrected():
    decisions, status = _fold("risky_drift")
    assert decisions[-1].action is Action.FLAG_FOR_REVIEW
    assert decisions[-1].review.reason == "unsafe_drift"
    assert status is None


def test_correlation_failure_is_flagged_as_amount_mismatch():
    decisions, status = _fold("correlation_failure")
    assert decisions[-1].action is Action.FLAG_FOR_REVIEW
    assert decisions[-1].review.reason == "amount_mismatch"
    assert status is None


def test_reversal_on_settled_order_routes_to_disputed():
    decisions, status = _fold("reversal")
    assert decisions[-1].action is Action.ROUTE_DISPUTED
    assert status is ReconStatus.DISPUTED


@pytest.mark.parametrize("key", ["normal", "lagging", "stuck", "silent"])
def test_timer_scenarios_arm_a_recheck_on_capture(key):
    decisions, status = _fold(key)
    assert decisions[0].action is Action.ARM_RECHECK
    assert status is ReconStatus.PENDING_RECHECK


def test_classify_outcomes():
    assert classify("RESOLVED_AUTOCORRECTED", []).tag == "RESOLVED_AUTOCORRECTED"
    assert classify("PENDING_RECHECK", []).terminal is False
    assert classify(None, ["unsafe_drift"]).tag == "FLAGGED_UNSAFE_DRIFT"
    assert classify(None, ["amount_mismatch"]).terminal is True
    assert classify("DEAD_LETTER", ["dead_letter"]).tag == "DEAD_LETTER"
    assert classify(None, []).tag == "NO_ACTION"


# --------------------------------------------------------------------------- #
# fakes
# --------------------------------------------------------------------------- #
class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, script):
        self.script = script
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        for needle, rows in self.script.items():
            if needle in sql:
                return _Result(rows)
        return _Result([])


class FakePool:
    def __init__(self, script):
        self.script = script
        self.conns = []

    @contextmanager
    def connection(self):
        conn = FakeConn(self.script)
        self.conns.append(conn)
        yield conn


class FakeMerchantStore:
    def __init__(self, due=()):
        self.created, self.paid, self._due = [], [], list(due)

    def create_if_absent(self, conn, **kw):
        self.created.append(kw)
        return True

    def mark_paid(self, conn, order_id):
        self.paid.append(order_id)
        return True

    def due_transitions(self, conn, *, now, limit=200):
        return self._due[:limit]

    @contextmanager
    def connection(self):
        yield object()


class RecordingProcessor:
    def __init__(self):
        self.events, self.rechecks, self.merchant = [], [], []

    def process_event(self, envelope, *, now=None):
        self.events.append(envelope["event_id"])

    def process_due_rechecks(self, now, limit):
        from lite.processor import RecheckCounts

        self.rechecks.append(limit)
        return RecheckCounts(reclaimed=1, resolved=2)

    def process_due_merchant_transitions(self, now, limit):
        self.merchant.append(limit)
        return 3


def _settings(**over):
    base = dict(worker=load_worker_settings(env_file=None), tick_limit=20, max_live_orders=5)
    base.update(over)
    return LiteSettings(**base)


def _service(*, script=None, processor=None, merchant=None, **over):
    pool = FakePool(script or {})
    proc = processor or RecordingProcessor()
    merch = merchant or FakeMerchantStore()
    svc = LiteService(pool=pool, processor=proc, merchant_store=merch,
                      settings=_settings(**over), clock=lambda: NOW)
    return svc, pool, proc, merch


# --------------------------------------------------------------------------- #
# tick lock
# --------------------------------------------------------------------------- #
def test_tick_skips_work_when_lock_is_held_elsewhere():
    svc, pool, proc, _ = _service(script={"pg_try_advisory_xact_lock": [(False,)]})
    result = svc.tick()
    assert result.ran is False
    assert proc.rechecks == [] and proc.merchant == []


def test_tick_runs_bounded_work_under_a_transaction_level_lock():
    svc, pool, proc, _ = _service(script={"pg_try_advisory_xact_lock": [(True,)]}, tick_limit=7)
    result = svc.tick()
    assert (result.ran, result.reclaimed, result.rechecks_resolved, result.merchant_paid) == (True, 1, 2, 3)
    assert proc.rechecks == [7] and proc.merchant == [7]
    sql, params = pool.conns[0].executed[0]
    assert "pg_try_advisory_xact_lock" in sql and params == (TICK_LOCK_KEY,)
    # Never a session-level lock: those leak through a transaction-mode pooler.
    all_sql = " ".join(s for c in pool.conns for s, _ in c.executed)
    assert "pg_advisory_lock" not in all_sql and "pg_try_advisory_lock" not in all_sql


def test_tick_skips_merchant_stage_once_the_budget_is_spent():
    svc, _, proc, _ = _service(script={"pg_try_advisory_xact_lock": [(True,)]})
    svc.tick(budget_seconds=-1)
    assert proc.rechecks and proc.merchant == []


# --------------------------------------------------------------------------- #
# caps and validation
# --------------------------------------------------------------------------- #
def test_submit_refuses_beyond_the_global_cap_and_creates_nothing():
    svc, pool, proc, merch = _service(script={"count(*)": [(4,)]}, max_live_orders=5)
    with pytest.raises(CapExceeded):
        svc.submit_orders(amount=50_000, scenario="normal", count=2)
    assert merch.created == [] and proc.events == []
    assert pool.conns[0].executed[0][1] == (CAP_LOCK_KEY,)


def test_submit_at_the_cap_boundary_is_allowed():
    svc, _, proc, merch = _service(script={"count(*)": [(4,)]}, max_live_orders=5)
    ids = svc.submit_orders(amount=50_000, scenario="stuck", count=1)
    assert len(ids) == 1 and len(merch.created) == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(amount=50_000, scenario="nope", count=1),
        dict(amount=50_000, scenario="normal", count=0),
        dict(amount=50_000, scenario="normal", count=MAX_ORDERS_PER_REQUEST + 1),
        dict(amount=5, scenario="normal", count=1),
        dict(amount=10**12, scenario="normal", count=1),
        dict(amount="100", scenario="normal", count=1),
    ],
)
def test_submit_rejects_bad_requests_before_touching_the_database(kwargs):
    svc, pool, proc, merch = _service()
    with pytest.raises(LiteError):
        svc.submit_orders(**kwargs)
    assert pool.conns == [] and merch.created == []


def test_submit_creates_rows_then_feeds_events_in_order():
    svc, _, proc, merch = _service(script={"count(*)": [(0,)]})
    ids = svc.submit_orders(amount=50_000, scenario="reversal", count=2)
    assert len(ids) == 2 and all(i.startswith("order_LIVE_reversal_") for i in ids)
    assert merch.paid == ids                      # reversal: merchant already paid
    assert len(proc.events) == 6                  # captured, merchant paid, refund x 2
    assert proc.events[0].endswith(":captured") and proc.events[2].endswith(":refund")


def test_submit_never_marks_timer_scenarios_paid_up_front():
    svc, _, _, merch = _service(script={"count(*)": [(0,)]})
    svc.submit_orders(amount=50_000, scenario="lagging", count=1)
    assert merch.paid == []
    assert merch.created[0]["next_action_at"] == NOW + timedelta(seconds=svc._settings.lag_seconds)


# --------------------------------------------------------------------------- #
# processor wiring
# --------------------------------------------------------------------------- #
class FakeStore:
    def __init__(self):
        self.audit_ids = set()
        self.applied = []

    @contextmanager
    def connection(self):
        yield object()

    def read_state(self, conn, order_id):
        from worker.state import ReadState

        return ReadState(row=None, view=None)

    def apply(self, conn, *, event, decision, projected_view, prior_row):
        from worker.state import ApplyResult

        fresh = event.event_id not in self.audit_ids
        self.audit_ids.add(event.event_id)
        self.applied.append(event.event_id)
        return ApplyResult(audit_written=fresh, pointer_moved=fresh)


def _processor(merchant=None, store=None):
    return LiteProcessor(
        store=store or FakeStore(),
        merchant_store=merchant or FakeMerchantStore(),
        settings=load_worker_settings(env_file=None),
    )


def test_process_event_replay_is_absorbed_by_the_audit_constraint():
    store = FakeStore()
    proc = _processor(store=store)
    env = _plan("stuck")[1].envelopes[0]
    first = proc.process_event(env, now=NOW)
    again = proc.process_event(env, now=NOW)
    assert first.result.audit_written is True
    assert again.result.audit_written is False       # no Redis involved anywhere
    assert first.evaluation.decision.action is Action.ARM_RECHECK


def test_process_event_drops_unusable_envelopes():
    assert _processor().process_event({"nope": 1}) is None


def test_process_due_merchant_transitions_feeds_events_without_kafka():
    from merchant_sim.store import DueOrder

    merchant = FakeMerchantStore(due=[DueOrder("order_LIVE_normal_aaaaaaaaaaaa", 50_000, "INR")])
    store = FakeStore()
    proc = _processor(merchant=merchant, store=store)
    assert proc.process_due_merchant_transitions(NOW, 20) == 1
    assert merchant.paid == ["order_LIVE_normal_aaaaaaaaaaaa"]
    assert store.applied == ["merch:order_LIVE_normal_aaaaaaaaaaaa:paid"]


def test_noop_delay_queue_holds_nothing():
    q = NoopDelayQueue()
    q.push("o", NOW)
    assert q.due(NOW) == []
    q.remove("o")
    q.close()

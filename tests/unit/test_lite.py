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

    def process_due_rechecks(self, now, limit, *, should_stop=None):
        from lite.processor import RecheckCounts

        self.rechecks.append(limit)
        return RecheckCounts(reclaimed=1, resolved=2)

    def process_due_merchant_transitions(self, now, limit, *, before_rechecks=None, should_stop=None):
        self.merchant.append((limit, before_rechecks))
        return 1


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
    assert (result.ran, result.reclaimed, result.rechecks_resolved, result.merchant_paid) == (True, 1, 2, 2)
    assert proc.rechecks == [7]
    # paid transitions due before the recheck, the recheck pass, then those due after
    assert proc.merchant == [(7, True), (7, False)]
    sql, params = pool.conns[0].executed[0]
    assert "pg_try_advisory_xact_lock" in sql and params == (TICK_LOCK_KEY,)
    # Never a session-level lock: those leak through a transaction-mode pooler.
    all_sql = " ".join(s for c in pool.conns for s, _ in c.executed)
    assert "pg_advisory_lock" not in all_sql and "pg_try_advisory_lock" not in all_sql


def test_tick_does_no_work_when_the_budget_is_already_spent():
    svc, _, proc, _ = _service(script={"pg_try_advisory_xact_lock": [(True,)]})
    result = svc.tick(budget_seconds=-1)
    assert proc.rechecks == [] and proc.merchant == []
    assert result.ran is True and result.partial is True


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


def test_merchant_transitions_split_around_the_armed_recheck():
    from merchant_sim.store import DueOrder

    t0 = NOW
    due = [
        DueOrder("early", 1, "INR", t0 + timedelta(seconds=2)),    # paid before recheck
        DueOrder("late", 1, "INR", t0 + timedelta(seconds=60)),    # paid after recheck
        DueOrder("unarmed", 1, "INR", t0 + timedelta(seconds=90)),  # no recheck armed
    ]
    armed = {"early": t0 + timedelta(seconds=20), "late": t0 + timedelta(seconds=20)}

    class ArmedStore(FakeStore):
        @contextmanager
        def connection(self):
            class C:
                def execute(self_, sql, params):
                    return _Result(list(armed.items()))

            yield C()

    def run(flag):
        merchant = FakeMerchantStore(due=due)
        proc = _processor(merchant=merchant, store=ArmedStore())
        proc.process_due_merchant_transitions(NOW, 20, before_rechecks=flag)
        return merchant.paid

    assert run(True) == ["early", "unarmed"]
    assert run(False) == ["late"]
    assert run(None) == ["early", "late", "unarmed"]


# --------------------------------------------------------------------------- #
# tick time budget (fake clock)
# --------------------------------------------------------------------------- #
class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


class SlowProcessor(RecordingProcessor):
    """Each stage burns fake time, so the budget can run out between stages."""

    def __init__(self, clock, *, merchant_cost=0.0, recheck_cost=0.0):
        super().__init__()
        self.clock, self.merchant_cost, self.recheck_cost = clock, merchant_cost, recheck_cost

    def process_due_rechecks(self, now, limit, *, should_stop=None):
        self.clock.advance(self.recheck_cost)
        return super().process_due_rechecks(now, limit, should_stop=should_stop)

    def process_due_merchant_transitions(self, now, limit, *, before_rechecks=None, should_stop=None):
        self.clock.advance(self.merchant_cost)
        return super().process_due_merchant_transitions(
            now, limit, before_rechecks=before_rechecks, should_stop=should_stop
        )


def _timed_service(processor, clock, budget):
    pool = FakePool({"pg_try_advisory_xact_lock": [(True,)]})
    return LiteService(pool=pool, processor=processor, merchant_store=FakeMerchantStore(),
                       settings=_settings(tick_budget_seconds=budget),
                       clock=lambda: NOW, monotonic=clock)


def test_tick_within_budget_visits_every_stage_and_is_not_partial():
    clock = FakeClock()
    proc = SlowProcessor(clock, merchant_cost=1, recheck_cost=1)
    result = _timed_service(proc, clock, budget=10).tick()
    assert proc.merchant == [(20, True), (20, False)] and proc.rechecks == [20]
    assert result.partial is False


def test_tick_stops_before_the_recheck_stage_when_the_first_stage_spends_the_budget():
    clock = FakeClock()
    proc = SlowProcessor(clock, merchant_cost=5)
    result = _timed_service(proc, clock, budget=3).tick()
    assert proc.merchant == [(20, True)]
    assert proc.rechecks == []
    assert result.partial is True and result.merchant_paid == 1 and result.ran is True


def test_tick_stops_before_the_late_merchant_stage_when_rechecks_spend_the_budget():
    clock = FakeClock()
    proc = SlowProcessor(clock, recheck_cost=5)
    result = _timed_service(proc, clock, budget=3).tick()
    assert proc.merchant == [(20, True)] and proc.rechecks == [20]
    assert result.partial is True
    assert (result.reclaimed, result.rechecks_resolved) == (1, 2)   # work done is kept


class ClockedStore(FakeStore):
    """FakeStore whose apply() costs one fake second; reports no armed rechecks."""

    def __init__(self, clock):
        super().__init__()
        self.clock = clock

    @contextmanager
    def connection(self):
        class C:
            def execute(self_, sql, params=None):
                return _Result([])

        yield C()

    def apply(self, *a, **kw):
        self.clock.advance(1)
        return super().apply(*a, **kw)


def test_tick_stops_between_merchant_items_when_the_budget_runs_out():
    from merchant_sim.store import DueOrder

    clock = FakeClock()
    due = [DueOrder(f"order_LIVE_normal_{i:012x}", 50_000, "INR") for i in range(5)]
    merchant = FakeMerchantStore(due=due)
    processor = _processor(merchant=merchant, store=ClockedStore(clock))
    pool = FakePool({"pg_try_advisory_xact_lock": [(True,)]})
    svc = LiteService(pool=pool, processor=processor, merchant_store=merchant,
                      settings=_settings(tick_budget_seconds=2.5),
                      clock=lambda: NOW, monotonic=clock)

    result = svc.tick()

    # checks happen at t=0,1,2 (<2.5, process) and t=3 (stop): three of five
    assert len(merchant.paid) == 3
    assert result.merchant_paid == 3 and result.partial is True


def _sweep_parts(clock, n=5):
    from worker.sweep import PendingRecheckSweep

    resolved = []

    class Store:
        @contextmanager
        def connection(self):
            yield object()

        def list_orphaned_pending(self, conn, *, grace_seconds, limit):
            return [f"o{i}" for i in range(n)][:limit]

    class Runner:
        def resolve_order(self, order_id, now):
            resolved.append(order_id)
            clock.advance(1)

    return PendingRecheckSweep, Store(), Runner(), resolved


def test_pending_sweep_stops_between_items_when_asked():
    clock = FakeClock()
    cls, store, runner, resolved = _sweep_parts(clock)
    budget_end = 2.5
    sweep = cls(store=store, runner=runner, settings=load_worker_settings(env_file=None),
                should_stop=lambda: clock() >= budget_end)
    assert sweep.run_once(NOW) == 3 and resolved == ["o0", "o1", "o2"]


def test_pending_sweep_without_a_stop_hook_handles_everything_as_before():
    clock = FakeClock()
    cls, store, runner, resolved = _sweep_parts(clock)
    sweep = cls(store=store, runner=runner, settings=load_worker_settings(env_file=None))
    assert sweep.run_once(NOW) == 5 and len(resolved) == 5


def test_sweep_service_skips_the_pending_sweep_when_stopped_after_the_stuck_sweep():
    from worker.sweep import SweepService

    class Stuck:
        def run_once(self, now):
            return 2

    class Pending:
        def run_once(self, now):
            raise AssertionError("must not run")

    svc = SweepService(stuck=Stuck(), pending=Pending(), settings=load_worker_settings(env_file=None))
    assert svc.run_once(NOW, should_stop=lambda: True) == (2, 0)

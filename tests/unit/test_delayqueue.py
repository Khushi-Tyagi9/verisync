"""InMemoryDelayQueue semantics, especially 'earliest wins'. Pure."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from worker.delayqueue import InMemoryDelayQueue

BASE = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def test_new_member_is_added_regardless_of_only_if_earlier():
    dq = InMemoryDelayQueue()
    dq.push("o1", BASE + timedelta(minutes=10))
    assert dq.due(BASE + timedelta(minutes=11)) == ["o1"]


def test_repeat_detection_cannot_push_the_deadline_out():
    dq = InMemoryDelayQueue()
    dq.push("o1", BASE + timedelta(minutes=10))
    dq.push("o1", BASE + timedelta(minutes=45))  # a later re-detection

    # Still due at the original 10-minute mark, not pushed out to 45.
    assert dq.due(BASE + timedelta(minutes=15)) == ["o1"]


def test_repeat_detection_can_pull_the_deadline_in():
    dq = InMemoryDelayQueue()
    dq.push("o1", BASE + timedelta(minutes=30))
    dq.push("o1", BASE + timedelta(minutes=5))

    assert dq.due(BASE + timedelta(minutes=10)) == ["o1"]
    assert dq.due(BASE + timedelta(minutes=4)) == []


def test_forced_push_overrides_earliest_wins_for_a_deliberate_rearm():
    dq = InMemoryDelayQueue()
    dq.push("o1", BASE - timedelta(minutes=1))  # a stale, already-fired score
    dq.push("o1", BASE + timedelta(minutes=10), only_if_earlier=False)

    assert dq.due(BASE) == []
    assert dq.due(BASE + timedelta(minutes=11)) == ["o1"]


def test_due_is_ordered_by_score_and_respects_limit():
    dq = InMemoryDelayQueue()
    dq.push("late", BASE + timedelta(minutes=9))
    dq.push("early", BASE + timedelta(minutes=1))
    dq.push("mid", BASE + timedelta(minutes=5))

    assert dq.due(BASE + timedelta(minutes=10)) == ["early", "mid", "late"]
    assert dq.due(BASE + timedelta(minutes=10), limit=2) == ["early", "mid"]


def test_remove_drops_the_entry():
    dq = InMemoryDelayQueue()
    dq.push("o1", BASE)
    dq.remove("o1")
    dq.remove("o1")  # idempotent
    assert dq.due(BASE + timedelta(hours=1)) == []

"""Pure bits of the merchant simulator. No infrastructure."""

from __future__ import annotations

import random
from datetime import datetime, timezone

from merchant_sim.config import MerchantSimSettings
from merchant_sim.simulator import MerchantSimulator, _capture_amount_currency

NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)


def _settings(**kw) -> MerchantSimSettings:
    base = dict(
        kafka_bootstrap_servers="x",
        lifecycle_topic="t",
        consumer_group="g",
        postgres_dsn="d",
        lag_seconds=900,
        immediate_seconds=2,
        scan_interval_seconds=5,
    )
    base.update(kw)
    return MerchantSimSettings(**base)


def _sim(settings) -> MerchantSimulator:
    return MerchantSimulator(
        consumer=None, store=None, publisher=None, settings=settings, rng=random.Random(0)
    )


def test_capture_amount_currency_from_payment_entity():
    raw = {"payload": {"payment": {"entity": {"amount": 50000, "currency": "INR"}}}}
    assert _capture_amount_currency(raw) == (50000, "INR")


def test_capture_amount_currency_falls_back_to_order_entity():
    raw = {"payload": {"order": {"entity": {"amount": 12345, "currency": "USD"}}}}
    assert _capture_amount_currency(raw) == (12345, "USD")


def test_capture_amount_currency_missing_is_none():
    assert _capture_amount_currency({}) == (None, None)
    assert _capture_amount_currency({"payload": {}}) == (None, None)
    assert _capture_amount_currency("not a dict") == (None, None)


def test_force_profile_overrides_the_weighting():
    sim = _sim(_settings(force_profile="stuck"))
    assert {sim._pick_profile() for _ in range(20)} == {"stuck"}


def test_pick_profile_respects_weights():
    # All weight on 'lagging' -> only 'lagging' ever comes out.
    sim = _sim(_settings(profile_weights={"immediate": 0, "lagging": 1, "stuck": 0, "silent": 0}))
    assert {sim._pick_profile() for _ in range(30)} == {"lagging"}


def test_next_action_at_none_for_stuck_and_silent():
    sim = _sim(_settings())
    assert sim._next_action_at("stuck", NOW) is None
    assert sim._next_action_at("silent", NOW) is None


def test_next_action_at_uses_immediate_then_lag_offsets():
    sim = _sim(_settings(immediate_seconds=2, lag_seconds=900))
    assert (sim._next_action_at("immediate", NOW) - NOW).total_seconds() == 2
    assert (sim._next_action_at("lagging", NOW) - NOW).total_seconds() == 900

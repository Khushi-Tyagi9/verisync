"""Phase 5: a deliberately imperfect simulated merchant service.

Watches the unified ``order-lifecycle-events`` topic for Razorpay captures and
reacts the way a flaky WooCommerce-style integration would: some orders are
marked paid at once, some lag, some acknowledge the order but never pay it, and
some never emit anything. Publishes merchant-side events back onto the same
topic. It is not part of the reconciliation pipeline.
"""

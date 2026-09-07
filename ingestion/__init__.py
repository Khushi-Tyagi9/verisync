"""verisync webhook ingestion service (Phase 3).

Responsibilities, in order:
  1. Verify the Razorpay signature over the RAW request body.
  2. Scope gate on order_id: absent -> out_of_scope_events (quiet stat); present -> proceed.
  3. Publish a normalized envelope to the unified `order-lifecycle-events`
     topic, keyed by order_id.

It deliberately does NOT interpret payment status or make reconciliation
decisions -- that is the worker's job (Phase 4).
"""

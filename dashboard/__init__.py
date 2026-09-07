"""Phase 6: a live reconciliation dashboard.

Reads straight from Postgres (orders_state, audit_log, order_views,
review_queue, out_of_scope_events) and serves a page that polls itself every
couple of seconds, so a correction flowing through Kafka and the worker shows
up without a manual refresh. Read-only; it never writes.
"""

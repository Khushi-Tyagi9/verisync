-- Applied on top of the original schema.sql for an already-running database.
-- Fresh installs get this from schema.sql directly.
--
-- order_views holds the accumulated per-order projection the decision layer
-- folds each event into. It is kept separate from orders_state (the
-- reconciliation pointer) and is written on every event, so a merchant-side
-- record that arrives before any Razorpay event survives to recheck time.

BEGIN;

CREATE TABLE IF NOT EXISTS order_views (
    order_id    TEXT PRIMARY KEY,
    view        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;

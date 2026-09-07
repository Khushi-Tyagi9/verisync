-- Applied on top of the original schema.sql for an already-running database.
-- Fresh installs get this from schema.sql directly.
--
-- The Phase 5 merchant simulator's own order table. Not part of the
-- reconciliation pipeline; the simulated merchant service owns it and updates
-- it imperfectly on purpose (immediate / lagging / stuck / silent).

BEGIN;

CREATE TABLE IF NOT EXISTS merchant_orders (
    order_id       TEXT PRIMARY KEY,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'paid', 'failed', 'cancelled', 'refunded')),
    amount         BIGINT,
    currency       TEXT,
    profile        TEXT NOT NULL
                   CHECK (profile IN ('immediate', 'lagging', 'stuck', 'silent')),
    next_action_at TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_merchant_orders_due
    ON merchant_orders (next_action_at)
    WHERE next_action_at IS NOT NULL;

COMMIT;

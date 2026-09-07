-- verisync — schema.sql
-- Phase 1 DDL. Loaded automatically by the postgres container on first init
-- (mounted into /docker-entrypoint-initdb.d/). No application code depends on
-- anything here yet; this file only has to apply cleanly and be boring.
--
-- Design rules carried in from the architecture plan:
--   * orders_state is a mutable pointer, derived from the append-only audit_log.
--   * audit_log is append-only, source of truth, UNIQUE on event_id ONLY.
--   * out_of_scope_events is a quiet stat counter (no order_id, no human action).
--   * review_queue is the actionable anomaly queue (correlation failures + dead letters).

BEGIN;

-- ---------------------------------------------------------------------------
-- orders_state — current, mutable, per-order pointer
-- ---------------------------------------------------------------------------
CREATE TABLE orders_state (
    order_id     TEXT PRIMARY KEY,
    status       TEXT NOT NULL
                 CHECK (status IN (
                     'PENDING_RECHECK',
                     'PROCESSING',
                     'RESOLVED_AUTOCORRECTED',
                     'RESOLVED_NATURALLY',
                     'DISPUTED',
                     'DEAD_LETTER'
                 )),
    -- NULL unless a recheck is currently armed for this order.
    -- MUST be reset to NULL whenever the order reaches a terminal resolved
    -- state, otherwise a later drift episode can inherit a stale past
    -- timestamp through the LEAST() upsert and skip its own wait window.
    recheck_at   TIMESTAMPTZ,
    -- bounded-retry counter for the stuck-PROCESSING reclaim loop.
    retry_count  INTEGER NOT NULL DEFAULT 0,
    -- fencing token: set on every atomic claim, cleared to NULL on reclaim so
    -- a stale worker's old token can never match a future claim.
    claim_token  UUID,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fallback PENDING_RECHECK sweep:
--   WHERE status = 'PENDING_RECHECK' AND recheck_at < NOW() - INTERVAL '1 minute'
CREATE INDEX idx_orders_state_pending_recheck
    ON orders_state (recheck_at)
    WHERE status = 'PENDING_RECHECK';

-- Stuck-PROCESSING recovery sweep:
--   WHERE status = 'PROCESSING' AND updated_at < NOW() - INTERVAL '5 minutes'
CREATE INDEX idx_orders_state_stuck_processing
    ON orders_state (updated_at)
    WHERE status = 'PROCESSING';

-- ---------------------------------------------------------------------------
-- order_views — accumulated per-order projection (every order that has ever
-- produced an event), kept separate from orders_state.
-- ---------------------------------------------------------------------------
-- orders_state is the reconciliation *pointer*: it only exists once an order
-- has drift armed or a resolution. This table is the raw cross-event
-- projection the pure decision layer folds each event into (razorpay status,
-- merchant status, amounts, reversal ids). The worker persists it on every
-- event, including non-actionable ones, so a merchant record that arrives
-- before any Razorpay event is not lost by the time a recheck fires.
CREATE TABLE order_views (
    order_id    TEXT PRIMARY KEY,
    view        JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- audit_log — append-only source of truth
-- ---------------------------------------------------------------------------
CREATE TABLE audit_log (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Authoritative idempotency key. Razorpay's event_id is stable per event
    -- and is their own recommended dedup key. The worker's INSERT relies on
    -- this UNIQUE failing on conflict — that failure IS the guarantee.
    event_id        TEXT NOT NULL,

    order_id        TEXT,

    -- Sub-entity tracking so a second refund / a new dispute on the same
    -- payment keeps its own audit row instead of collapsing into one bucket.
    sub_entity_type TEXT,          -- e.g. 'payment' | 'refund' | 'dispute' | 'settlement'
    sub_entity_id   TEXT,          -- e.g. refund_id / dispute_id

    -- 'system' covers audit rows written by the worker's own actions: recheck
    -- resolutions, sweep reclaims, dead-lettering. They are decisions too.
    source          TEXT NOT NULL
                    CONSTRAINT audit_log_source_check
                    CHECK (source IN ('razorpay', 'merchant', 'system')),
    event_type      TEXT NOT NULL, -- raw lifecycle event name, e.g. 'payment.captured'

    old_state       TEXT,
    new_state       TEXT,

    -- Decision vocabulary is defined by the Phase 2 decision logic; left as
    -- free text here on purpose so the schema does not pin it prematurely.
    decision        TEXT NOT NULL,
    direction       TEXT,          -- safe-autocorrect / flag-only / disputed / neutral

    amount          BIGINT,        -- minor units (paise); nullable
    currency        TEXT,
    payload         JSONB,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_audit_log_event_id UNIQUE (event_id)
);

-- DELIBERATELY ABSENT: a unique index on (order_id, sub_entity_id, new_state).
-- This has been proposed and rejected repeatedly in design review. event_id is
-- stable per event (Razorpay's own dedup key), not per sub-entity, so this
-- composite key contradicts their documentation. Worse, it would silently
-- block legitimate re-occurrences — a dispute reopened after resolution, a
-- sub-entity legitimately reaching the same state twice — from ever being
-- recorded again. Do not add it.

CREATE INDEX idx_audit_log_order_id    ON audit_log (order_id, created_at);
CREATE INDEX idx_audit_log_sub_entity  ON audit_log (order_id, sub_entity_type, sub_entity_id);
CREATE INDEX idx_audit_log_created_at  ON audit_log (created_at);

-- ---------------------------------------------------------------------------
-- out_of_scope_events — quiet stat counter (no order_id present)
-- ---------------------------------------------------------------------------
-- Routine traffic: Payment Links, some legacy flows. Not an anomaly. No alert,
-- no review queue entry. event_id is kept UNIQUE so webhook retries of the
-- same out-of-scope event do not inflate the count (ingestion does
-- ON CONFLICT DO NOTHING).
CREATE TABLE out_of_scope_events (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     TEXT NOT NULL,
    event_type   TEXT,
    reason       TEXT NOT NULL DEFAULT 'no_order_id',
    payload      JSONB,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_out_of_scope_events_event_id UNIQUE (event_id)
);

CREATE INDEX idx_out_of_scope_events_received_at ON out_of_scope_events (received_at);
CREATE INDEX idx_out_of_scope_events_event_type  ON out_of_scope_events (event_type);

-- ---------------------------------------------------------------------------
-- review_queue — actionable anomaly queue
-- ---------------------------------------------------------------------------
-- Genuine correlation failures (validation failure, amount mismatch, no
-- matching merchant record) and dead-lettered rows. Requires human attention.
-- Kept strictly separate from out_of_scope_events so real anomalies never get
-- diluted by benign traffic.
CREATE TABLE review_queue (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id       TEXT,
    sub_entity_id  TEXT,
    -- NULL for entries raised by a sweep / dead-letter rather than a webhook.
    event_id       TEXT,
    reason         TEXT NOT NULL,   -- 'validation_failed' | 'amount_mismatch' | 'no_merchant_record' | 'dead_letter' | ...
    details        JSONB,
    status         TEXT NOT NULL DEFAULT 'OPEN'
                   CHECK (status IN ('OPEN', 'RESOLVED', 'DISMISSED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ
);

-- At most one review entry per triggering event; sweep/dead-letter rows have
-- no event_id and are exempt from the constraint.
CREATE UNIQUE INDEX uq_review_queue_event_id
    ON review_queue (event_id)
    WHERE event_id IS NOT NULL;

CREATE INDEX idx_review_queue_open
    ON review_queue (created_at)
    WHERE status = 'OPEN';

-- ---------------------------------------------------------------------------
-- merchant_orders — the Phase 5 simulator's own order table
-- ---------------------------------------------------------------------------
-- Not part of the reconciliation pipeline. The simulated merchant service owns
-- this table and updates it imperfectly on purpose: 'immediate' and 'lagging'
-- orders eventually reach 'paid', 'stuck' orders acknowledge the order but
-- never pay it, 'silent' orders never produce any event at all. It publishes
-- merchant-side events onto the same order-lifecycle-events topic.
CREATE TABLE merchant_orders (
    order_id       TEXT PRIMARY KEY,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'paid', 'failed', 'cancelled', 'refunded')),
    amount         BIGINT,
    currency       TEXT,
    profile        TEXT NOT NULL
                   CHECK (profile IN ('immediate', 'lagging', 'stuck', 'silent')),
    -- when the next transition (pending -> paid) is due; NULL when nothing is
    -- pending for this order (already paid, or a stuck/silent order).
    next_action_at TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_merchant_orders_due
    ON merchant_orders (next_action_at)
    WHERE next_action_at IS NOT NULL;

COMMIT;

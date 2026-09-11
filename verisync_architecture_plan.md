# verisync — Architecture Plan

Real-time reconciliation between Razorpay's confirmed payment status and a merchant's own order record. The successor to a hackathon batch version, built properly this time: real infrastructure, real webhooks, real concurrency, no deadline-driven scope cuts.

## Core principle, carried over unchanged

Never mutate a source record. Derive current state from an immutable, append-only log. Every fix in this plan, from the idempotency model to the sweep mechanics, exists to protect that one rule under real distributed-systems conditions.

## Direction-aware decision logic (unchanged from the original design)

- Razorpay confirms success, merchant record hasn't caught up → **auto-correct**, safe direction
- Razorpay says failed, merchant record shows success → **flag only, never auto-correct**, could mean fraud, not lag
- Reversal-type event on an already-settled order → route to `DISPUTED`, never dropped
- This logic lives in pure, I/O-free functions, unit-testable without any infrastructure running

---

## Phase 1: Infra scaffolding

- Docker Compose: **Redpanda** (Kafka-API-compatible, single binary, actually runs on a laptop), **Redis**, **Postgres**
- **Redis persistence is mandatory from day one**: `appendfsync everysec`. The delay-queue mechanism in Phase 4 depends on Redis surviving a restart without silently losing pending corrections.
- Goal: everything starts and talks to each other. No application logic yet.

## Phase 2: Core decision logic

- Port the correlation, drift comparison, and direction-aware decision logic from the batch design
- Adapt from "reads a complete batch row" to "builds up per-order state incrementally as events arrive"
- Zero I/O in this layer. Fully unit-tested in isolation.

## Phase 3: Webhook ingestion

1. **Signature verification, in the correct order**: capture `await request.body()` before any JSON parsing touches it. Compute HMAC-SHA256 over those raw bytes. Compare against `X-Razorpay-Signature`. Parsing first and re-serializing to verify breaks the signature silently, this order is non-negotiable.
2. **Scope gate**: check for `order_id` immediately.
   - Missing → `out_of_scope_events`, a stat counter only. No alert, no review queue entry. This is routine traffic (Payment Links, some legacy flows), not an anomaly, and mixing it with real anomalies causes alert fatigue that trains reviewers to stop checking.
   - Present → proceed.
3. **Publish to one unified Kafka topic**, `order-lifecycle-events`, keyed by `order_id`. Both Razorpay's events and the merchant-side events land here. Two separate topics, even both keyed by order_id, do not guarantee ordering across each other, only a single shared topic does.

## Phase 4: Reconciliation worker

**Idempotency, two layers, not one:**
- **Authoritative**: a unique constraint on `event_id` in `audit_log`. The insert fails on conflict. This is the real guarantee, single database, single transaction, no cross-system ordering to get wrong.
- **Fast pre-filter**: `idempotency:razorpay:{event_id}` in Redis, 48-hour TTL, for cheap early rejection of the common case. Not the source of truth, just an optimization in front of it.

**Business-state precedence, kept separate from idempotency:**
- Distinct sub-entities on the same payment (a second refund, a new dispute) get their own tracking keyed on the sub-entity ID (`refund_id`, `dispute_id`), so they don't collapse into one idempotency bucket and lose their own audit row.

**Kafka consumption:**
- Manual offset commits (`enable.auto.commit=False`), only after a successful Postgres write. This is an at-least-once delivery guarantee, not an ordering fix, Kafka's per-partition order already holds across rebalances. It's only safe because the idempotency layer above catches any replay this produces.

**Correlation failures** (order_id present, fails validation, amount mismatch, no matching merchant record) → `review_queue`. Reserved exclusively for genuine anomalies, kept separate from the benign `out_of_scope_events` bucket.

**Terminal lock with reversal allow-list**: settled states are locked against replay, except a named allow-list of reversal-type events, which route to `DISPUTED` instead of being dropped or silently absorbed.

**Time-decay window for safe drift, full mechanics:**
1. On detecting safe drift, write to Postgres first using an **UPSERT**, not a bare UPDATE, and preserve the earliest deadline on repeat detections:
   ```sql
   INSERT INTO orders_state (order_id, status, recheck_at, updated_at)
   VALUES (:order_id, 'PENDING_RECHECK', :recheck_at, NOW())
   ON CONFLICT (order_id) DO UPDATE
   SET status = 'PENDING_RECHECK',
       recheck_at = LEAST(orders_state.recheck_at, EXCLUDED.recheck_at),
       updated_at = NOW()
   WHERE orders_state.status != 'PROCESSING';
   ```
   A bare UPDATE assumes a row already exists, if this is the first event ever seen for an order, it silently matches zero rows and does nothing. The `LEAST()` guards against timer starvation: Razorpay routinely fires `payment.authorized` then `payment.captured` in close succession, both legitimately re-detecting the same safe drift if the merchant side hasn't caught up yet, without `LEAST()`, each re-detection would push `recheck_at` further out, and the correction could starve indefinitely under completely normal traffic.
   **`recheck_at` must be cleared to `NULL` whenever an order reaches a terminal resolved state.** Otherwise a brand-new drift episode on a previously-resolved order could inherit a stale, long-past timestamp through this same `LEAST()` comparison, making it eligible for the sweep instantly instead of waiting out its own window.
2. Push to a Redis sorted set second, scored by that same recheck time. This is the fast path, not the source of truth.
3. A lightweight consumer polls the sorted set for expired entries. On firing, it **re-fetches current state from Postgres**, never acts on the state captured when it was scheduled.
4. A Postgres sweep runs every 3–5 minutes (not every second) as a fallback: `SELECT order_id FROM orders_state WHERE status = 'PENDING_RECHECK' AND recheck_at < NOW() - INTERVAL '1 minute'`. Catches the case where the Redis push never happened, crash between the two writes.
5. **Claim with a fencing token, not a bare status check.** Both the fast path and the sweep go through the same atomic claim, generating a fresh token each time:
   ```sql
   UPDATE orders_state
   SET status = 'PROCESSING', claim_token = :new_token, updated_at = NOW()
   WHERE order_id = :order_id AND status = 'PENDING_RECHECK'
   RETURNING order_id, claim_token;
   ```
   A bare status check alone isn't sufficient: a worker that stalls past the watchdog timeout can have its claim reclaimed, then finish late and pass a status check that's now true for someone else's claim, not its own. The token makes "is this still my claim" unambiguous.
6. **Completion must verify the token, not just the status:**
   ```sql
   UPDATE orders_state
   SET status = 'RESOLVED_AUTOCORRECTED', updated_at = NOW()
   WHERE order_id = :order_id AND status = 'PROCESSING' AND claim_token = :my_token;
   ```
   If this returns 0 rows, the worker's claim was stolen, it must not write to `audit_log`, roll back and exit.
7. **Stuck-`PROCESSING` recovery sweep**, a second, separate sweep: reclaims rows stuck in `PROCESSING` past a timeout back to `PENDING_RECHECK`, **and clears `claim_token` to NULL on reclaim**, so a stale worker's old token can never accidentally match a future claim.
8. **Bounded retries on the reclaim, to prevent an infinite crash-reclaim loop**: add a `retry_count` column, incremented in the *same transaction* as the status flip and token clear:
   ```sql
   UPDATE orders_state
   SET status = CASE WHEN retry_count + 1 >= 3 THEN 'DEAD_LETTER' ELSE 'PENDING_RECHECK' END,
       retry_count = retry_count + 1,
       claim_token = NULL,
       updated_at = NOW()
   WHERE status = 'PROCESSING'
     AND updated_at < NOW() - INTERVAL '5 minutes'
     AND retry_count < 3;
   ```
   At `retry_count >= 3`, route to `DEAD_LETTER_QUEUE` / `review_queue` instead of reclaiming again.

## Phase 5: Merchant-side data source

No real merchant business behind this, so this needs its own simulated service, one that behaves imperfectly on purpose: some orders update immediately, some lag, some never update at all, mirroring the real WooCommerce failure mode this whole project is built around. Publishes onto the same unified `order-lifecycle-events` topic, not a separate stream.

## Phase 6: Dashboard

Reads live from Postgres. Visually separates `out_of_scope_events` (a quiet stat, nothing actionable) from `review_queue` (the actionable anomaly queue), matching the distinction already built into the ingestion and worker logic, don't let the dashboard blur back together what the pipeline deliberately kept apart.

## Phase 7: Real Razorpay test-mode webhooks, last

Once the full pipeline works end to end against synthetic and simulated events, point Razorpay's test-mode webhook config at the ingestion endpoint (via ngrok locally). Use real test UPI identifiers to generate genuine signed events. At this point, only one new thing is being tested, real signatures and real network timing, not the core logic and the internet at the same time.

**Outcome: attempted, deliberately stopped short of completion.** The reconciliation pipeline and the signature-verification code are built and tested against Razorpay's documented algorithm (raw-body HMAC-SHA256, correct capture-before-parse ordering), but connecting real Razorpay test-mode webhooks was blocked by account activation requiring bank-account verification via UPI, not by anything in this codebase. This was confirmed through multiple independent tests, not assumed from one failure:

- The webhooks API (`POST /v1/webhooks`) rejected creation with `BAD_REQUEST_ERROR` / `"Invalid event name/names: 1"` across every event-name combination tried (`payment.captured`+`payment.failed`, `payment.captured`+`payment.authorized`, `payment.authorized`+`payment.failed`), which ruled out any single event name as the cause, while `GET /v1/webhooks` and `GET /v1/payments` on the same key succeeded, ruling out auth and general API access.
- The Razorpay dashboard, attempting the identical webhook by hand, routes into bank-account-linking onboarding instead of completing the save, independently confirming an account-activation gate rather than an API-specific bug.
- Two different tunnel domains, ngrok and zrok, both produced the identical rejection, ruling out the tunnel URL as the cause.

This is a deliberate stopping point, not an unresolved bug: linking a real bank account to a test account is disproportionate to what this phase needed to prove, given the reconciliation logic is already independently verified through Phases 1 through 6, including a live chaos test (kill the worker mid-processing after it holds the claim, confirm the fencing token prevents double-execution and the sweep recovers the claim cleanly).

---

## State model summary

**`orders_state`** (current, mutable pointer, derived from the log): `order_id`, `status` (`PENDING_RECHECK` / `PROCESSING` / `RESOLVED_AUTOCORRECTED` / `RESOLVED_NATURALLY` / `DISPUTED` / `DEAD_LETTER`), `recheck_at`, `retry_count`, `claim_token`, `updated_at`

**`audit_log`** (append-only, source of truth): every decision ever made, unique constraint on `event_id`, never updated, only inserted

**`out_of_scope_events`**: stat counter, no order_id present, no human action

**`review_queue`**: genuine correlation failures and dead-lettered rows, requires human attention

## Testing discipline

- Unit tests for the pure decision logic, no infrastructure required
- Integration tests using testcontainers, spinning up real Redpanda, Redis, and Postgres per test run
- At least one deliberate chaos test: kill the worker mid-processing, restart it, confirm nothing fires twice, the real-world version of the exact bug already found and fixed once in the batch build

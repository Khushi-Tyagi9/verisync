# verisync — Implementation Guide

How to actually build this, in order, with checkpoints between phases so a mistake in phase 2 doesn't get buried under phase 5.

## Project structure

```
verisync/
  docker-compose.yml
  requirements.txt
  .env.example
  db/
    schema.sql              # orders_state, audit_log, out_of_scope_events DDL
  decision/
    correlate.py             # pure, no I/O
    compare.py                # pure, no I/O
    actions.py                 # pure, no I/O
  ingestion/
    webhook.py                  # FastAPI app, signature verification, scope gate
  worker/
    consumer.py                  # Kafka consumer, claim/completion logic
    sweep.py                      # PENDING_RECHECK fallback sweep + stuck-PROCESSING recovery
  merchant_sim/
    simulator.py                    # Phase 5, simulated merchant-side event source
  dashboard/
    app.py                            # Phase 6
  tests/
    unit/                               # decision logic, no infra
    integration/                        # testcontainers-based, real Redpanda/Redis/Postgres
```

Keep `decision/` genuinely dependency-free, no database client imports, no Kafka client imports, anywhere in that folder. That's what makes it testable in milliseconds instead of needing the whole stack running just to check a comparison rule.

## Build order, with a checkpoint after each phase

### Phase 1: Infra scaffolding
**Do:** write `docker-compose.yml` with Redpanda, Redis, Postgres. For Redis, use `command: redis-server --appendonly yes --appendfsync everysec --no-appendfsync-on-rewrite yes`, the last flag prevents disk I/O blocking during background AOF rewrites on a resource-constrained host, common on a laptop's shared Docker Desktop disk, while keeping the one-second durability guarantee for normal operations. Write `db/schema.sql` with the full DDL, `orders_state` (including `claim_token UUID`, `retry_count INT DEFAULT 0`), `audit_log` (unique constraint on `event_id` only, this is deliberate, see note below), `out_of_scope_events`, `review_queue`.

**Explicitly do not add a unique index on `(order_id, sub_entity_id, new_state)` in `audit_log`.** This has been proposed and rejected multiple times across the design review, it's built on a claim Razorpay's own documentation directly contradicts (`event_id` is stable per event, their own recommended dedup key, not per sub-entity), and the index itself would silently block legitimate re-occurrences, a dispute reopened after resolution, a sub-entity legitimately reaching the same state twice, from ever being recorded a second time.
**Checkpoint:** `docker-compose up`, connect to each service independently (psql into Postgres, redis-cli ping, check Redpanda Console loads), confirm the schema applied. Nothing else, no app code, until this is boring and reliable.

### Phase 2: Core decision logic
**Do:** implement `decision/correlate.py`, `compare.py`, `actions.py` as pure functions, take structured input, return a decision, no side effects.
**Checkpoint:** full unit test suite green, with zero infrastructure running. If a test needs Docker up, it's in the wrong folder.

### Phase 3: Webhook ingestion
**Do:** `ingestion/webhook.py`, the FastAPI endpoint, raw-body signature verification, the `order_id` scope gate, publish to `order-lifecycle-events` keyed by `order_id`.
**Checkpoint:** send a real signed test payload (construct it by hand using your webhook secret, or use Razorpay's test-mode webhook simulator if available) and confirm it lands on the Kafka topic, visible in Redpanda Console. Also send one deliberately mis-signed payload and confirm it's rejected with a 401.

### Phase 4: The reconciliation worker
**Do:** `worker/consumer.py`, consuming from Kafka, running the decision logic from Phase 2 against per-order state in Postgres, implementing the dual idempotency layer (Postgres unique constraint as authoritative, Redis TTL key as fast pre-filter), manual offset commits after successful writes.
Separately, `worker/sweep.py`, the PENDING_RECHECK fallback sweep, the stuck-PROCESSING recovery sweep with the fencing token and bounded retries.
**Checkpoint, in this order:**
1. Feed one synthetic event through by hand, confirm it produces the correct decision and audit row.
2. Feed the same event through twice, confirm the second is rejected by the idempotency constraint, not double-processed.
3. Manually set a row to `PROCESSING` with an old `updated_at`, run the sweep, confirm it reclaims correctly and clears the token.
4. Force three consecutive reclaims on the same row, confirm it lands in `DEAD_LETTER`, not a fourth reclaim.
This phase has the most moving parts, don't move to Phase 5 until all four of these pass, they're testing the actual bugs already found and fixed on paper.

### Phase 5: Merchant-side simulator
**Do:** `merchant_sim/simulator.py`, its own small order table, a script that updates it unevenly, immediate for some orders, delayed for others, never for a few, publishing to the same `order-lifecycle-events` topic.
**Checkpoint:** confirm at least one simulated "never updates" case actually produces a real, visible drift the worker catches and acts on correctly.

### Phase 6: Dashboard
**Do:** `dashboard/app.py`, reading live from Postgres, `out_of_scope_events` and `review_queue` shown as visually distinct sections.
**Checkpoint:** open it while Phase 4 and 5 are both running, watch a real correction appear without refreshing manually.

### Phase 7: Real Razorpay test-mode webhooks
**Do:** ngrok tunnel to your local ingestion endpoint, configure it in Razorpay's dashboard under test mode, trigger real test payments using their documented test UPI IDs.
**Checkpoint:** a real signed webhook, from Razorpay's actual servers, flows all the way through to a correct decision in the dashboard. This is the first point where you're testing real network timing instead of synthetic events.

## Working with Claude Code through this

Give it one phase at a time, not the whole plan at once, and require the checkpoint before moving on. A reasonable first message once the repo has this implementation guide and the architecture plan committed:

> Read `verisync_architecture_plan.md` and `verisync_implementation_guide.md` fully. Start with Phase 1 only: the Docker Compose file and the full schema.sql. Don't write any application code yet. Stop after Phase 1 and wait for me to confirm the checkpoint before continuing.

That last line matters, it stops it from barreling through all seven phases in one shot and leaving you no clean point to catch a problem before it's buried under five more phases of code built on top of it.

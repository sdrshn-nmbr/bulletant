# Edge AI Usage Billing

## Target user
- AI applications running local inference that need usage metering at the edge.
- Embedded or sidecar deployments that must work offline and reconcile later.

## Core flow
1. Record usage events locally with an idempotent `event_id`.
2. Update the ledger and enforce budget/credits in the same write batch.
3. Export events for later sync, then import idempotently upstream.
4. Replay events for audits and reconciliation.

## Sync model and audit guarantees
- Append-only event log; ledger entries are derived and stored for audit.
- Idempotent events via `entry/<event_id>` to prevent double-charging.
- Strict integer balances and costs to avoid rounding drift.
- Offline-first: export/import NDJSON for replayable sync.
- Optional per-account hash chain (`prev_hash`, `hash`) for tamper-evident logs.
- Price snapshots captured per event for cost provenance.

## Non-goals
- No full SQL engine or secondary index system.
- No multi-region consensus or distributed locking.
- No ANN/vector search for billing data.

## Key schema
- `acct/<tenant>/<id>`: account metadata
- `bal/<tenant>/<id>`: int64 balance
- `event/<tenant>/<ts>/<event_id>`: usage event
- `acct_event/<tenant>/<acct>/<ts>/<event_id>`: per-account event index
- `entry/<event_id>`: ledger entry (idempotency)
- `price/<plan_id>`: price plan definition
- `price_snapshot/<event_id>`: captured price inputs
- `acct_hash/<tenant>/<acct>`: last event hash for the account

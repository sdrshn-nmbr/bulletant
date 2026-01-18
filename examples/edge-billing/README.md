# Edge Billing Example

This walkthrough uses the HTTP API and CLI to meter local inference usage and sync it later.

## HTTP flow
Create an account, top up credits, record usage, and read the balance:

```
curl -X POST "http://localhost:8080/billing/accounts" \
  -H "Content-Type: application/json" \
  -d '{"tenant":"demo","id":"acct-1"}'

curl -X POST "http://localhost:8080/billing/credits" \
  -H "Content-Type: application/json" \
  -d '{"tenant":"demo","account_id":"acct-1","amount":1000}'

curl -X POST "http://localhost:8080/billing/usage" \
  -H "Content-Type: application/json" \
  -d '{"id":"event-1","tenant":"demo","account_id":"acct-1","units":25,"unit_price":10}'

curl "http://localhost:8080/billing/accounts/acct-1/balance?tenant=demo"
```

Export events for sync (NDJSON), then import elsewhere:

```
curl -X POST "http://localhost:8080/billing/sync/export" \
  -H "Content-Type: application/json" \
  -d '{"tenant":"demo","limit":100}' > billing.ndjson

curl -X POST "http://localhost:8080/billing/sync/import" \
  -H "Content-Type: application/json" \
  --data-binary @billing.ndjson
```

## CLI flow
```
go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-account create --tenant demo --id acct-1

go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-credit --tenant demo --account-id acct-1 --amount 1000

go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-usage record --tenant demo --account-id acct-1 --event-id event-1 --units 25 --unit-price 10

go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-balance --tenant demo --account-id acct-1

go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-export --tenant demo --limit 100 > billing.ndjson

go run ./cmd/bulletant --mode=http --base-url http://localhost:8080 \
  billing-import --file billing.ndjson
```

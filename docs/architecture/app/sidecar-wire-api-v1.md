# Sidecar Wire API v1

## Purpose
Define the wire-level contract between Python execution bridge and StockSharp sidecar gateway for staging-first real transport.

## Transport
- Protocol: HTTP/JSON
- Default base path: `/v1`
- Idempotency header: `X-Idempotency-Key`
- Fail-closed behavior: non-2xx response is treated as execution error; retry only for retryable codes.

## Endpoints

### 1. Submit intent
- `POST /v1/intents/submit`
- Request:
```json
{
  "intent_id": "INT-123",
  "intent": {
    "intent_id": "INT-123",
    "signal_id": "SIG-123",
    "mode": "live",
    "broker_adapter": "stocksharp-sidecar",
    "action": "buy",
    "contract_id": "BR-6.26",
    "qty": 1,
    "price": 82.5,
    "stop_price": 81.8,
    "created_at": "2026-03-18T12:00:00Z"
  }
}
```
- Response:
```json
{
  "ack": {
    "intent_id": "INT-123",
    "external_order_id": "gw-abcdef1234567890",
    "accepted": true,
    "broker_adapter": "stocksharp-sidecar",
    "state": "submitted"
  }
}
```

### 2. Cancel intent
- `POST /v1/intents/{intent_id}/cancel`
- Request:
```json
{
  "intent_id": "INT-123",
  "canceled_at": "2026-03-18T12:01:00Z"
}
```
- Response includes `state="canceled"` and `external_order_id`.

### 3. Replace intent
- `POST /v1/intents/{intent_id}/replace`
- Request:
```json
{
  "intent_id": "INT-123",
  "new_qty": 2,
  "new_price": 82.7,
  "replaced_at": "2026-03-18T12:02:00Z"
}
```
- Response includes `state="replaced"`, `new_qty`, `new_price`, `external_order_id`.

### 4. Broker updates stream
- `GET /v1/stream/updates?cursor=<cursor>&limit=<n>`
- Response:
```json
{
  "updates": [
    {
      "external_order_id": "gw-abcdef1234567890",
      "state": "submitted",
      "event_ts": "2026-03-18T12:00:00Z",
      "payload": {"intent_id": "INT-123"}
    }
  ],
  "next_cursor": "1"
}
```

### 5. Broker fills stream
- `GET /v1/stream/fills?cursor=<cursor>&limit=<n>`
- Response:
```json
{
  "fills": [
    {
      "external_order_id": "gw-abcdef1234567890",
      "fill_id": "FILL-123",
      "qty": 1,
      "price": 82.55,
      "fee": 0.01,
      "fill_ts": "2026-03-18T12:00:04Z"
    }
  ],
  "next_cursor": "1"
}
```

### 6. Operational probes
- `GET /health`
- `GET /ready`
- `GET /metrics`

## Error Model
Error payload:
```json
{
  "error_code": "permission_denied",
  "message": "token scope is insufficient"
}
```

Retryable status codes:
- `408`, `409`, `425`, `429`, `500`, `502`, `503`, `504`

Non-retryable status codes:
- `400`, `401`, `403`, `404`, `422`

## Compatibility
- Core trading DTOs are unchanged (`OrderIntent`, `BrokerOrder`, `BrokerFill`, `PositionSnapshot`).
- Stream model is cursor-based and incrementally drained by transport.
- Idempotency is keyed by `intent_id` (submit) and operation-specific keys for cancel/replace.

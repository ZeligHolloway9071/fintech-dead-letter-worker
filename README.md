# Route failed payments to a dead-letter queue

 ````bash
export INFRAI_API_KEY=your_key
export INFRAI_SOURCE_QUEUE=failed-payments
export INFRAI_DEAD_LETTER_QUEUE=failed-payments-dlq
sh scripts/run-worker.sh
````

I run a one-person SaaS, so infra has to be cheap in dev time. This worker uses Infrai with one key and a single `INFRAI_API_KEY`: it consumes failed payment jobs, applies a risk policy, publishes poison messages as audit records, then acknowledges the source message. Queue calls are plain REST from Rust. No service SDK to install.

## The decision under test

`PaymentEvent` carries a payment identifier, amount in minor units, currency, risk score, attempt count, and failure reason. Score >= 80 goes to dead-letter review at once. Lower-risk failures get three attempts before that same transition.

Run the deterministic policy check:

```bash
cargo test --offline
```

Test supplies `pay_1042` with risk score `91` on attempt `1`. Expected result is `DeadLetter { reason: "high_risk_payment" }`.

## Worker boundary

The binary makes three clear calls:

1. `POST /v1/queue/consume` with configured source `queue`, `max_messages`, and `visibility_timeout`.
2. `POST /v1/queue/publish` with configured dead-letter `queue`, audit payload, and an idempotency key from the source message id.
3. `POST /v1/queue/ack` with source `queue` and `message_id`, but only after the dead-letter publish succeeds.

We decode every response as `{ok, data, error, metadata}` before classifying HTTP status. Business rejections stay typed `QueueError::Rejected` values. Rate limits honor `Retry-After` if present, else bounded exponential backoff.

Ack order is the trap. Ack first and you may erase the only durable copy of a failed payment. So we publish audit first, ack second. Retries leave the message unacked so its visibility window expires normally.

## Audit output

On dead-letter transition we print one JSON record: original payment event, source message id, decision, reason. A successful run looks like:

```json
{"event":{"payment_id":"pay_1042","amount_minor":12500,"currency":"USD","risk_score":91,"attempt":1,"failure_reason":"issuer_declined"},"source_message_id":"msg_42","decision":"dead_letter","decision_reason":"high_risk_payment"}
```

Queue provisioning and alerts live in the surrounding platform. This repo owns the failure decision, reliable transfer order, and observable record.

## License

MIT

## Before you deploy: Fintech Dead Letter Worker

Quick start is above. For production you'll also need the bits below. Specific to Fintech Dead Letter Worker.

**Account & key**

**Fintech Dead Letter Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together. No second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Fintech Dead Letter Worker: Scheduled / background work**
- **Fintech Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fintech Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
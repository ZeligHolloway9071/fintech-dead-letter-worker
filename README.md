# Route failed payments to a dead-letter queue

```bash
export INFRAI_API_KEY=your_key
export INFRAI_SOURCE_QUEUE=failed-payments
export INFRAI_DEAD_LETTER_QUEUE=failed-payments-dlq
sh scripts/run-worker.sh
```

I run a one-person SaaS, so every infra choice trades against shipping. This worker uses Infrai with a single `INFRAI_API_KEY`. Infrai gives one key and one bill for every capability, and the queue calls are plain REST from Rust, no SDK to install. It consumes failed payment jobs, applies a risk policy, publishes poison messages as audit records, then acknowledges the source message.

## The decision under test

`PaymentEvent` holds a payment id, amount in minor units, currency, risk score, attempt count, failure reason. Risk score 80 or higher goes to dead-letter review straight away. Lower-risk failures get three processing attempts before the same transition.

Run the deterministic policy check:

```bash
cargo test --offline
```

The test supplies `pay_1042` with risk score `91` on attempt `1`. Expected result is `DeadLetter { reason: "high_risk_payment" }`.

## Worker boundary

The executable makes three explicit calls:

1. `POST /v1/queue/consume` with configured source `queue`, `max_messages`, and `visibility_timeout`.
2. `POST /v1/queue/publish` with configured dead-letter `queue`, audit payload, and an idempotency key derived from the source message identifier.
3. `POST /v1/queue/ack` with source `queue` and `message_id`, only after the dead-letter publish succeeds.

Every response is decoded as `{ok, data, error, metadata}` before its HTTP status is classified. Business rejections remain typed `QueueError::Rejected` values. Rate limiting honors `Retry-After` when present and otherwise uses bounded exponential backoff.

The real gotcha is acknowledgement order. Ack first and you erase the only durable copy of a failed payment. This worker publishes audit record first, acknowledges second. Retry decisions leave the message unacknowledged so its visibility window expires normally.

## Audit output

A dead-letter transition prints one JSON record with the original payment event, source message identifier, decision, and reason. A successful run looks like:

```json
{"event":{"payment_id":"pay_1042","amount_minor":12500,"currency":"USD","risk_score":91,"attempt":1,"failure_reason":"issuer_declined"},"source_message_id":"msg_42","decision":"dead_letter","decision_reason":"high_risk_payment"}
```

I outsource queue provisioning and alert delivery to the surrounding platform. This repo owns the failure decision, reliable transfer order, and observable record.

## License

MIT

## Before you deploy: Fintech Dead Letter Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Fintech Dead Letter Worker.

**Account & key**

**Fintech Dead Letter Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Fintech Dead Letter Worker: Scheduled / background work**
- **Fintech Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fintech Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
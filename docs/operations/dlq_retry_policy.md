# Dead-Letter Queue (DLQ) & Retry Policy

## Overview

When a Kafka consumer fails to process a message after exhausting retries,
the message is forwarded to `dlq.generic` wrapped in the `DlqGeneric` Avro
envelope. This ensures no data is silently dropped.

## Retry Schedule

| Attempt | Delay | Notes |
|---|---|---|
| 1 | 0 s | Immediate first try |
| 2 | 5 s | Exponential backoff base |
| 3 | 25 s | 5^attempt seconds |
| 4 (final) | — | Sent to DLQ |

- **Max retries:** 3 (configurable via `config/kafka.yaml` → `consumer.max_retries`)
- **Backoff:** `5 ^ attempt` seconds, capped at 300 s
- **Idempotency:** consumers must deduplicate using `idempotency_key` so
  retried messages don't produce duplicate side-effects.

## DLQ Envelope (`dlq.generic.v1.avsc`)

| Field | Description |
|---|---|
| `original_topic` | Source topic the message came from |
| `original_key` | Kafka key (usually symbol) |
| `original_value` | Raw bytes of the original Avro payload |
| `error_message` | Exception message |
| `error_class` | Exception class name |
| `retry_count` | How many times processing was attempted |
| `max_retries` | Configured max retries |
| `first_failed_at` | Timestamp of first failure (epoch ms) |
| `last_failed_at` | Timestamp of most recent failure (epoch ms) |

## Alerting

| Condition | Action |
|---|---|
| Any message lands in DLQ | PagerDuty / Slack alert via Alertmanager |
| DLQ topic lag > 100 | Escalate (manual triage required) |
| Same `correlation_id` appears > 3× in DLQ | Escalate — systemic failure |

## Replay / Reprocessing

1. Inspect the DLQ message in Kafka UI or via `kafkacat`.
2. Fix the root cause in the consumer.
3. Re-publish the `original_value` to the `original_topic` with a new
   `event_id` but the **same** `correlation_id` and `idempotency_key`.
4. The consumer will process it; the idempotency guard prevents duplicates.

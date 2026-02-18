# Kafka Topics Contract

> Canonical reference for every Kafka topic in the AWET pipeline.  
> Each topic has an Avro schema in `schemas/avro/`.

## Topic Map

| Topic | Schema | Producer | Consumer(s) | Partitions | Retention |
|---|---|---|---|---|---|
| `market.raw` | `market.raw.v1.avsc` | DataIngestionAgent | FeatureEngine | 6 | 7 d |
| `market.features` | `market.features.v1.avsc` | FeatureEngine | PredictionAgent | 6 | 3 d |
| `signals.prediction` | `signals.prediction.v1.avsc` | PredictionAgent | RiskEngine | 6 | 3 d |
| `risk.verdict` | `risk.verdict.v1.avsc` | RiskEngine | ExecutionAgent | 3 | 7 d |
| `trade.validated` | `trade.validated.v1.avsc` | ExecutionAgent | AuditWriter | 3 | 7 d |
| `exec.report` | `exec.report.v1.avsc` | ExecutionAgent | AuditWriter, Dashboard | 3 | 30 d |
| `audit.event` | `audit.event.v1.avsc` | All agents | AuditWriter | 3 | 90 d |
| `dlq.generic` | `dlq.generic.v1.avsc` | Any consumer | DLQ Monitor | 1 | 30 d |

### Social / Reddit Pipeline

| Topic | Schema | Producer | Consumer(s) | Partitions | Retention |
|---|---|---|---|---|---|
| `social.reddit.raw` | `social.reddit.raw.v1.avsc` | RedditIngestionAgent | RedditEnrichmentAgent | 3 | 7 d |
| `social.reddit.enriched` | `social.reddit.enriched.v1.avsc` | RedditEnrichmentAgent | RedditSummaryAgent, KnowledgeBaseWriter | 3 | 7 d |
| `social.reddit.summary` | `social.reddit.summary.v1.avsc` | RedditSummaryAgent | KnowledgeBaseWriter, Dashboard | 3 | 30 d |
| `dlq.social.reddit.raw` | `dlq.social.reddit.v1.avsc` | RedditIngestionAgent | DLQ Monitor | 1 | 30 d |
| `dlq.social.reddit.enriched` | `dlq.social.reddit.v1.avsc` | RedditEnrichmentAgent | DLQ Monitor | 1 | 30 d |
| `dlq.social.reddit.summary` | `dlq.social.reddit.v1.avsc` | RedditSummaryAgent | DLQ Monitor | 1 | 30 d |

> **Note:** All three DLQ topics share a single envelope schema (`dlq.social.reddit.v1.avsc`).
> The `original_topic` field inside the envelope identifies the source topic.
> See [schema_compatibility_rules.md](schema_compatibility_rules.md) for evolution governance.

## Required Envelope Fields

Every event on every topic **must** include:

| Field | Type | Description |
|---|---|---|
| `event_id` | UUID string | Unique per event |
| `correlation_id` | UUID string | Shared across the full pipeline chain |
| `idempotency_key` | string | Stable across retries (e.g. `{symbol}:{ts_epoch}:{stage}`) |
| `symbol` | string | Ticker symbol (`null` allowed only on `audit.event`) |
| `ts` | long (timestamp-millis) | UTC epoch milliseconds |
| `source` | string | Name of the producing service |
| `schema_version` | int | Monotonically increasing per topic |

## Schema Evolution Rules

1. **Backward compatible** — new consumers can read old messages.
2. New fields **must** have a default value (or be nullable).
3. Existing fields **must not** be removed or renamed.
4. Run `python scripts/check_schema_compatibility.py --schemas-dir schemas/avro` before merging.
5. Bump `schema_version` in the new `.avsc` file when adding fields.

## Partition Key Strategy

All topics use **symbol** as the partition key so that events for the same
ticker land on the same partition, preserving ordering per symbol.

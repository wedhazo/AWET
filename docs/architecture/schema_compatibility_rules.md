# Schema Compatibility Rules — `social.reddit.*` Topics

> Canonical governance rules for all schemas under `schemas/avro/social.reddit.*.avsc`.

## Compatibility Mode

**BACKWARD** — the only permitted compatibility mode.

A new schema version is backward-compatible when:
1. **No required field is removed.** Consumers compiled against the old schema
   must still be able to read messages written with the new schema.
2. **New fields have a default value** (or are nullable with `["null", ...]`).
3. **Field types are not changed** (widening `int → long` is allowed; narrowing is not).
4. **Enum symbols are only appended**, never removed or reordered.
5. **Record names and namespaces are immutable** once published.

## Versioning Convention

| Pattern | Example |
|---|---|
| `<topic>.v<N>.avsc` | `social.reddit.raw.v1.avsc` |
| Bump `N` for any schema change | `social.reddit.raw.v2.avsc` |

- The `schema_version` field inside the Avro file **must** match the filename
  (e.g. `"default": "social.reddit.raw.v2"`).
- Both the old and new `.avsc` files must coexist in `schemas/avro/` so that
  `scripts/check_schema_compatibility.py` can verify cross-version compatibility.

## Required Envelope Fields

Every `social.reddit.*` event **must** include these top-level fields:

| Field | Type | Notes |
|---|---|---|
| `event_id` | `string` (UUID4) | Unique per event |
| `correlation_id` | `string` (UUID4) | Shared across the pipeline run |
| `event_version` | `int` | Monotonic; default 1 |
| `produced_at_utc` | `string` (RFC3339) | UTC production timestamp |
| `schema_version` | `string` | Must match filename convention |
| `source` | `string` | Must be `"reddit"` for these topics |
| `trace` | `record` | Distributed tracing metadata |
| `payload` | `record` | Topic-specific business fields |

### `trace` Sub-Record

| Field | Type | Notes |
|---|---|---|
| `service_name` | `string` | Producing service |
| `host` | `string` | Hostname |
| `attempt` | `int` | Retry counter (1 = first) |
| `parent_event_id` | `["null", "string"]` | UUID of upstream event (null for ingestion) |

## DLQ Schemas

Dead-letter topics (`dlq.social.reddit.*`) share a single envelope schema
(`dlq.social.reddit.v1.avsc`). The failed event's serialised bytes are stored
in the `original_value` field. This avoids coupling DLQ schema evolution to
the individual topic schemas.

## Pre-Merge Checklist

Before merging any schema change:

- [ ] Run `python scripts/check_schema_compatibility.py --schemas-dir schemas/avro`
- [ ] Verify round-trip serialisation (automated in CI and contract tests)
- [ ] Confirm `schema_version` default matches the file name
- [ ] If adding a new topic, update `docs/architecture/kafka_topics.md`
- [ ] If adding a new topic, add contract tests in `tests/unit/test_schema_contracts.py`

## Failure Modes

| Failure | Mitigation |
|---|---|
| Old consumer receives new-schema message | Backward compat guarantees readability; unknown fields ignored |
| New consumer receives old-schema message | New fields use defaults; code must handle default/null values |
| Schema Registry rejects update | CI blocks merge via compatibility check step |
| content_hash mismatch across versions | content_hash is computed from stable business fields only — schema metadata changes don't affect it |

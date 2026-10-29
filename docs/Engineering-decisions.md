# Engineering Decisions

This document captures the engineering decisions, failure-handling strategies, operational considerations, tuning guidelines and known limitations of the MQTT telemetry ingestion system.

## 1. Failure Behaviour

The ingestion pipeline is designed to handle temporary failures without immediately losing incoming telemetry. Its behaviour depends on which component fails and how long the failure persists.

| Failure | Behaviour |
|---|---|
| Database unavailable briefly | Workers retry with exponential backoff (4 attempts). The bounded queue absorbs approximately 40 seconds of incoming traffic at 100 messages/s. |
| Database unavailable for an extended period | The queue eventually fills. Incoming messages that cannot be enqueued are dropped and counted. Alerts should monitor `dropped` and `failed`. `/healthz` returns 503. |
| MQTT broker disconnect | The client reconnects and re-subscribes. The persistent session allows queued QoS 1 messages to be replayed. |
| Ingestor crash | Unacknowledged QoS 1 messages may be redelivered, so duplicates are possible. Messages already acknowledged but not yet persisted can be lost. |
| Malformed payload | The message is counted in `decodeErr` and skipped without blocking the batch. |
| SIGTERM or Ctrl-C | The ingestor disconnects from the broker, drains the channel, flushes pending batches and exits. A clean shutdown is logged. |

### Graceful Shutdown

Shutdown order matters.

The MQTT client must be disconnected before the ingestion channel is closed. Reversing this order can cause a panic if the MQTT callback attempts to send to an already closed channel.

The intended sequence is:

1. Receive the shutdown signal.
2. Disconnect the MQTT client to stop incoming messages.
3. Close the ingestion channel.
4. Allow workers to drain remaining messages.
5. Flush pending batches.
6. Close database resources and exit.

### Duplicate Messages

MQTT QoS 1 provides at-least-once delivery between the publisher and broker, meaning a message may be delivered more than once.

Duplicates can occur during reconnects or redelivery. They are not automatically prevented by the database write path.

For stricter deduplication, messages can first be copied into a staging table, followed by:

`INSERT ... ON CONFLICT DO NOTHING`

against a unique `(device_id, time)` constraint.

This would prevent duplicate telemetry records from being inserted into the final table.

**Important:** The current implementation does not provide end-to-end at-least-once delivery because messages are acknowledged before database persistence.

---

## 2. Design Decisions

This section records the reasoning behind the major architectural and implementation choices.

### No Web Framework

The service does not have a conventional request-response application flow. Its primary responsibility is receiving MQTT messages and persisting telemetry.

Using smaller, purpose-specific libraries keeps the implementation relatively lightweight:

- Paho for MQTT communication.
- pgx for PostgreSQL connectivity and bulk inserts.
- Go's standard library HTTP server for health checks and profiling.
- Prometheus client library for metrics.

There is little value in introducing a full web framework for this workload.

### TimescaleDB Over Alternatives

TimescaleDB was selected primarily because it extends PostgreSQL, a database I was already familiar with.

This allowed me to reuse existing PostgreSQL knowledge, SQL, pgx and COPY functionality while benefiting from time-series-specific capabilities.

It also provides:

- Time-based partitioning through hypertables.
- Compression.
- Data retention policies.
- Continuous aggregates for rollups.

Altern
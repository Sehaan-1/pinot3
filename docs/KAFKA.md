# Kafka connector (Slice 7e1)

The installed `kafka` source and sink wrap Apache Kafka's Apache-2.0 official
`kafka-clients:4.4.0`. No broker protocol, codec, retry engine, transaction coordinator,
or group rebalancer is reimplemented. SDK compression dependencies remain transitive.

## Contract

Source schema is `(topic STRING NOT NULL, partition INT NOT NULL, offset LONG NOT
NULL, key BYTES NULL, value BYTES NULL)`. Raw bytes only: JSON/CSV decoding is not
implicit. A null value remains a Kafka tombstone, distinct from empty bytes. Headers,
timestamps and transactions are not exposed. Console renders bytes as base64.

Use `examples/kafka/{copy,console}.conf` with the installed CLI. Topics must exist;
source partitions must exist and have the requested readable offsets. Source requires
`bootstrap-servers` (list), `topic`, `group-id`, `mode` (`bounded` or `live`) and
`partitions`. Each partition entry has `partition`, `start-offset`, and for bounded
mode `end-offset`. Ranges are half-open. Explicit offsets only, no earliest/latest
or committed-offset fallback. Startup rejects retained-away starts or ends beyond
the current read-committed end. Compacted/aborted/control-record gaps are skipped.
One reader, manual assignment, one emission per poll, at most one fetched batch.
Checkpoint snapshots store accepted offsets, not prefetched consumer positions.
Completion commits the exact saved offsets, even if further rows were consumed.
Restore validates group/ranges and seeks engine offsets; broker commits are diagnostic,
not authoritative. External group members must never share the same group.

Sink selects BYTES columns `key` and `value` (overrides `key-field`, `value-field`),
respects their nullability, and rejects non-INSERT rows. It routes to one configured
topic, with SDK key-based/default partition selection. It copies payloads, bounds
outstanding futures, waits acknowledgements, uses `acks=all` and producer-session
idempotence. It has no transaction/committer; checkpoint restore can replay duplicates.
A close/failure can leave already-delivered records. No exactly-once claim.

## Options and security

Unknown options fail before client creation. Shared options: `timeout-ms` default10000
(range1000..60000), `max-record-bytes` default1048576 (1..16777216, combined key+value
payload). Source `poll-ms` default100 (1..1000), `max-poll-records` default100 (1..1000).
Sink `max-in-flight-records` default100 (1..1000). At most128 explicit partitions and
16 bootstrap server strings. SDK internal buffers/broker first batches can exceed a
payload limit; these are not hostile-input memory sandbox guarantees. Future waits
are individually finite, not one aggregate flush deadline; DNS is not hard-cancelled.

`security-protocol` defaults SSL; explicitly opt into PLAINTEXT only for trusted local
networks. SASL_SSL supports PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 and requires both
`sasl-mechanism` and `sasl-jaas-config`. TLS supports paired
`ssl-truststore-location`/`ssl-truststore-password` and complete
`ssl-keystore-location`/`ssl-keystore-password`/`ssl-key-password` sets. Hostname
verification keeps the SDK default. No arbitrary SDK property passthrough. Use config
substitution/secret injection, not committed credentials. Config objects and adapter
errors omit client diagnostics; the packaged CLI disables `org.apache.kafka` logs.
Embedders must similarly configure logging and protect config/state access.

## Live execution / recovery

CLI LocalRunner handles **bounded only**. For live jobs use `StreamingRunner`, with
`new StreamingRunner.Pipeline<>(binding, source, sink, KafkaSource.ReaderState.class,
KafkaSource.EnumeratorState.class, KafkaSink.State.class, String.class)` and an exclusive
`CheckpointRepository`. Set `requireCheckpoint=true` for mandatory recovery. Keep the
binding stable only for exactly the same topic/security/schema/routing configuration;
change it for configuration changes. Follow [runtime ownership and durability limits](STREAMING-RUNTIME.md).
Source completion follows acknowledged sink/checkpoint commit. One outstanding checkpoint,
monotonic IDs, no overlapping callbacks; interruption/failed IO terminally closes clients.
There is no broker/owner fencing or durable filesystem repository. Keep exclusive ownership
and Hazelcast backups alive; whole-cluster loss can lose offsets. Restart can duplicate sinks.

## Verification

Local supplemental JDK17 compile uses real adapter/config/runtime code but a **test-only
replacement for the SDK factory boundary**, never committed production stubs. PASS108
operation-port assertions plus serialized live source→sink checkpoint/cancel/restore
scenario. These do **not** prove SDK/broker compatibility.

Added SDK MockConsumer/MockProducer tests, real broker bounded/tombstone/multi-partition/
saved group-offset/runtime restart scenarios, Testcontainers gate and packaged CLI
roundtrip/logging assertions. They are **uncompiled/unrun here**, as are production
KafkaSdk, exact dependencies/convergence, pinned formatter, Maven/full reactor and
installed CLI. No Docker/Maven/broker available. Kafka/streaming/Stage7 acceptance stays
unchecked. Broker outage/restart, TLS/SASL service credentials and cross-version matrix
remain additional operational verification, not claimed by the deterministic tests.

Operator gates (no automatic Actions):

```sh
mvn -B -pl connector-kafka,seatunnel-cli -am verify
PINOT_KAFKA_CONTAINER_TESTS=true mvn -B -pl connector-kafka -am test
PINOT_KAFKA_BOOTSTRAP=localhost:9092 mvn -B -pl connector-kafka,seatunnel-cli -am verify
```

Broker tests create/delete unique topics and require local PLAINTEXT admin permissions.
The SDK dependency needs its codecs and SLF4J packaged; never mark it provided.

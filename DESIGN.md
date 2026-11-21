# helios: design notes

Status: accepted
Author: Parag Sawant

## Goals

Ingest a firehose of events and answer live questions over recent time, with two hard
constraints: duplicates must not double-count, and every consumer of the stream must
compute the same numbers.

## Deduplication: exactly-once *semantics* on at-least-once transport

Event transports (Kafka and friends) deliver at-least-once — the same event can arrive
twice. You can't buy exactly-once delivery, so Helios makes duplicates *free to reject*
instead. The `Deduplicator` keeps the ids it has seen within a trailing window; a
repeat within that window is dropped. Old ids age out, so memory is bounded by the
window, not by all history. The window length is the knob: it must be at least as long
as the maximum delay before a duplicate could plausibly arrive.

## Tumbling vs sliding: two different questions

- **Tumbling windows** answer "how much in each fixed interval" — revenue per minute,
  events per hour. Every event lands in exactly one non-overlapping bucket, keyed by
  `now // window`. The closed buckets are the completed rollups.
- **Sliding windows** answer "how much in the *last* interval" — events in the trailing
  60s, unique users right now. They keep the recent events and evict as time advances.

They're not interchangeable, and a real dashboard wants both: tumbling for historical
rollups, sliding for the live number.

## Integer-only, for portability

The whole point of a shared aggregation core is that every service computes identical
numbers. Floating point would break that (rounding differs per language), so:

- Timestamps and values are integers (milliseconds, integer amounts).
- The rate estimator reports **events/sec × 1000** as an integer, computed as
  `count * 1000 * 1000 / window_ms` — so 1.5/sec is exactly `1500`, no float in sight.
- Bucketing is integer division (`now // window`), consistent across languages via
  floor-division semantics.

The conformance vectors replay one event stream and pin every output; all six ports
reproduce them byte-for-byte.

## Non-goals

- **Not the transport.** Helios is the aggregation logic, not Kafka. It assumes events
  arrive with timestamps; wiring it to a real broker is the platform's job.
- **Not persistent.** These are in-memory windows. A production system snapshots closed
  tumbling buckets to a store (ClickHouse, etc.); Helios computes them, it doesn't
  persist them.
- **Single-writer per aggregator.** Each window assumes serialized access (one consumer
  per partition), which is exactly how you'd shard this — partition the stream, one
  aggregator per partition. No internal locking.

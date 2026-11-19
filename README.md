# helios

Real-time event analytics primitives — **deduplication**, **tumbling windows**,
**sliding windows**, and a **rate estimator** — implemented in **six languages** with
identical output, proven byte-for-byte by shared golden vectors. Plus a live analytics
wall that runs a simulated firehose through the aggregators in your browser.

[![Continuous integration](https://github.com/parag-labs/helios/actions/workflows/tests.yml/badge.svg)](https://github.com/parag-labs/helios/actions/workflows/tests.yml)
![Python](https://img.shields.io/badge/Python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![Go](https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=white)
![Rust](https://img.shields.io/badge/Rust-000000?style=flat-square&logo=rust&logoColor=white)
![C#](https://img.shields.io/badge/C%23-512BD4?style=flat-square&logo=dotnet&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=openjdk&logoColor=white)
![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=flat-square&logo=typescript&logoColor=white)

**▶ [Live analytics wall](https://parag-labs.github.io/helios/)** — drag the event rate
and watch events/sec, unique users, revenue, and tumbling buckets update live.

![metrics wall](docs/metrics-wall.png)

![architecture](docs/diagrams/architecture.png)

*The one-page architecture — the dedup gate and the tumbling/sliding/rate aggregators behind it. Source SVG: [docs/diagrams/architecture.svg](docs/diagrams/architecture.svg).*

## Why

A real-time analytics platform ingests a firehose of events and answers questions over
recent time — events per second, revenue per minute, unique users in the last 60s. The
hard part isn't storing the events; it's that **at-least-once delivery means the same
event arrives twice**, and every consumer of the stream must compute the *same* numbers.
Helios is the deterministic aggregation core that makes both true.

## The primitives

```python
from helios import Deduplicator, TumblingWindow, SlidingWindow, RateEstimator

dedup = Deduplicator(window_ms=60_000)
dedup.accept("evt-123", now_ms=1000)   # -> False if seen in the last 60s (a dupe)

TumblingWindow(1000).add(now_ms, value)      # fixed 1s revenue buckets
SlidingWindow(60_000).count(now_ms)          # events in the trailing 60s
RateEstimator(1000).per_sec_milli(now_ms)    # events/sec * 1000 (integer, portable)
```

- **Deduplicator** — drops repeated event ids within a bounded window, so a duplicate
  delivery never double-counts. Bounded memory: old ids age out.
- **Tumbling window** — fixed, non-overlapping buckets; the completed rollups.
- **Sliding window** — count and sum over the trailing window, for live metrics.
- **Rate estimator** — events/sec, as an integer scaled by 1000 (no floats, so it's
  portable).

Everything is integer arithmetic over millisecond timestamps, so all six ports agree
byte-for-byte. Details in [DESIGN.md](DESIGN.md) and [architecture/ADR](architecture/ADR).

## Six languages, one behavior

| Language | Run | Conformance |
|----------|-----|:-----------:|
| Python | `cd python && PYTHONPATH=src pytest -q` | ✓ |
| Go | `cd go && go test ./...` | ✓ |
| Rust | `cd rust && cargo test` | ✓ |
| C# (.NET 10) | `cd csharp/tests && dotnet test` | ✓ |
| Java (17+) | `cd java && mvn test` | ✓ |
| TypeScript | `cd web && npm test` | ✓ |

Each port replays a shared event stream through all four primitives and asserts the dedup
decisions, tumbling rollups, and sliding count/sum/rate series match
[conformance/vectors.json](conformance/). If any language disagrees, CI fails.

## Numbers

Measured (`python bench/benchmark.py`), detail in [BENCHMARKS.md](BENCHMARKS.md):
~250,000 events/sec through the full dedup → tumbling → sliding → rate pipeline in pure
Python — the floor; compiled ports are several times faster.

## Layout

```
helios/
├── python/         reference implementation + pytest
├── go/ rust/ csharp/ java/   ports, each with a conformance test
├── web/            TypeScript core + the live analytics wall (Vite/React)
├── conformance/    golden vectors + generator; the cross-language contract
├── bench/          ingestion-throughput benchmark
├── architecture/   ADRs
├── docs/           architecture diagram (svg + png)
├── DESIGN.md       dedup, tumbling vs sliding, the integer-rate trick
└── BENCHMARKS.md   reproducible numbers
```

Part of [parag-labs](https://github.com/parag-labs).

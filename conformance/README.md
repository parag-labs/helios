# Cross-language conformance

Real-time analytics is only useful if every consumer of the stream computes the *same*
numbers. This directory pins that agreement.

## What's here

- **`vectors.json`** — a shared event stream (ids, timestamps, values) plus the exact
  output every port must reproduce: the dedup accept/reject decision per event, the
  closed tumbling-window rollups, and the sliding count/sum/rate series.
- **`generate.py`** — regenerates the vectors from the Python reference.

## The contract

Each port replays the stream through a deduplicator, a tumbling window, a sliding
window, and a rate estimator, and asserts identical output. Everything is integer
arithmetic over millisecond timestamps and integer values (the rate is events/sec
scaled by 1000 to avoid floats), so the ports agree byte-for-byte. If any disagrees, CI
fails.

| Port | Conformance test |
|------|------------------|
| Python | `python/tests/test_conformance.py` |
| Go | `go/conformance_test.go` |
| Rust | `rust/tests/conformance.rs` |
| C# | `csharp/tests/ConformanceTests.cs` |
| Java | `java/.../ConformanceTest.java` |
| TypeScript | `web/src/core/conformance.test.ts` |

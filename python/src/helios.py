"""Helios: real-time event analytics — windowed aggregation and event deduplication.

A stream-analytics platform ingests a firehose of events and answers questions over
recent time: how many checkouts in the last minute, revenue per tumbling window, unique
users in a sliding window. Three pieces have to be deterministic so every consumer of
the stream computes the same numbers:

* **Deduplication** — the same event can be delivered more than once (at-least-once
  transport). A `Deduplicator` drops repeats by event id within a bounded window, so a
  duplicate delivery never double-counts.
* **Tumbling windows** — fixed, non-overlapping buckets (every event falls in exactly
  one). Used for "revenue per minute" style rollups.
* **Sliding windows** — a trailing window of width W evaluated at a point in time; used
  for "events in the last 60s" style live metrics.

Everything is integer arithmetic over millisecond timestamps and integer event values,
so the Python, Go, Rust, C#, Java, and TypeScript ports produce identical aggregates —
pinned by the conformance vectors.
"""

from __future__ import annotations

from collections import deque


class Deduplicator:
    """Drops events whose id has been seen within the trailing ``window_ms``. Bounded
    memory: ids older than the window are evicted as time advances."""

    def __init__(self, window_ms: int) -> None:
        if window_ms < 1:
            raise ValueError("window_ms must be at least 1")
        self.window_ms = window_ms
        self._seen: dict[str, int] = {}
        self._order: deque[tuple[int, str]] = deque()

    def accept(self, event_id: str, now_ms: int) -> bool:
        """True if this is the first time we've seen ``event_id`` within the window."""
        self._evict(now_ms)
        if event_id in self._seen:
            return False
        self._seen[event_id] = now_ms
        self._order.append((now_ms, event_id))
        return True

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while self._order and self._order[0][0] <= cutoff:
            _, eid = self._order.popleft()
            # Only remove if it wasn't refreshed (it can't be, we reject dupes) .
            if self._seen.get(eid, -1) <= cutoff:
                self._seen.pop(eid, None)


class TumblingWindow:
    """Fixed, non-overlapping buckets of ``window_ms``. Each event contributes its value
    (default 1) to exactly one bucket, keyed by ``now_ms // window_ms``. Closed buckets
    are the completed rollups."""

    def __init__(self, window_ms: int) -> None:
        if window_ms < 1:
            raise ValueError("window_ms must be at least 1")
        self.window_ms = window_ms
        self._buckets: dict[int, int] = {}

    def add(self, now_ms: int, value: int = 1) -> int:
        """Add ``value`` to the bucket for ``now_ms`` and return that bucket's new total."""
        b = now_ms // self.window_ms
        self._buckets[b] = self._buckets.get(b, 0) + value
        return self._buckets[b]

    def bucket_total(self, now_ms: int) -> int:
        return self._buckets.get(now_ms // self.window_ms, 0)

    def closed_buckets(self, now_ms: int) -> list[tuple[int, int]]:
        """(bucket_index, total) for every bucket strictly before the current one, sorted."""
        current = now_ms // self.window_ms
        return sorted((b, v) for b, v in self._buckets.items() if b < current)


class SlidingWindow:
    """A trailing window of ``window_ms``. Keeps (timestamp, value) events and reports
    the count and sum within the last ``window_ms`` at any query time."""

    def __init__(self, window_ms: int) -> None:
        if window_ms < 1:
            raise ValueError("window_ms must be at least 1")
        self.window_ms = window_ms
        self._events: deque[tuple[int, int]] = deque()
        self._sum = 0

    def add(self, now_ms: int, value: int = 1) -> None:
        self._events.append((now_ms, value))
        self._sum += value
        self._evict(now_ms)

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while self._events and self._events[0][0] <= cutoff:
            _, v = self._events.popleft()
            self._sum -= v

    def count(self, now_ms: int) -> int:
        self._evict(now_ms)
        return len(self._events)

    def sum(self, now_ms: int) -> int:
        self._evict(now_ms)
        return self._sum


class RateEstimator:
    """Events per second over the trailing window, as an integer scaled by 1000 (so
    1500 means 1.5/sec) to stay portable. Built on a sliding count."""

    def __init__(self, window_ms: int) -> None:
        self._window = SlidingWindow(window_ms)
        self.window_ms = window_ms

    def add(self, now_ms: int) -> None:
        self._window.add(now_ms, 1)

    def per_sec_milli(self, now_ms: int) -> int:
        """Rate in events/sec * 1000. count / (window_ms/1000) == count * 1000 / window_ms."""
        return self._window.count(now_ms) * 1000 * 1000 // self.window_ms

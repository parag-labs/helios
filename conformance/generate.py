"""Generate conformance/vectors.json for Helios. Run from repo root:
python conformance/generate.py"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python" / "src"))

from helios import Deduplicator, RateEstimator, SlidingWindow, TumblingWindow

# A shared event stream: (event_id, timestamp_ms, value). Some ids repeat to exercise
# dedup; timestamps straddle window boundaries.
STREAM = [
    ("a", 0, 10),
    ("b", 100, 20),
    ("a", 200, 10),   # duplicate of a within 1s
    ("c", 950, 5),
    ("d", 1000, 7),
    ("b", 1100, 20),  # b's first sighting was at 100, >1s ago -> allowed again
    ("e", 1500, 3),
    ("f", 2000, 8),
    ("g", 2500, 1),
    ("h", 3000, 4),
]
WINDOW_MS = 1000


def build() -> dict:
    dedup = Deduplicator(WINDOW_MS)
    tumbling = TumblingWindow(WINDOW_MS)
    sliding = SlidingWindow(WINDOW_MS)
    rate = RateEstimator(WINDOW_MS)

    dedup_decisions = []
    sliding_series = []
    for eid, ts, val in STREAM:
        accepted = dedup.accept(eid, ts)
        dedup_decisions.append(1 if accepted else 0)
        if accepted:
            tumbling.add(ts, val)
            sliding.add(ts, val)
            rate.add(ts)
        sliding_series.append(
            {"t": ts, "count": sliding.count(ts), "sum": sliding.sum(ts), "rate_milli": rate.per_sec_milli(ts)}
        )

    return {
        "_comment": "Helios cross-language conformance vectors. See conformance/README.md.",
        "window_ms": WINDOW_MS,
        "stream": [{"id": e, "t": t, "value": v} for (e, t, v) in STREAM],
        "dedup_decisions": dedup_decisions,
        "tumbling_closed": [
            {"bucket": b, "total": v} for (b, v) in tumbling.closed_buckets(10_000)
        ],
        "sliding_series": sliding_series,
    }


def main() -> None:
    out = ROOT / "conformance" / "vectors.json"
    out.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

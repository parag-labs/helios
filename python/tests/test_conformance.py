"""The Python reference must reproduce the committed conformance vectors exactly."""

from __future__ import annotations

import json
from pathlib import Path

from helios import Deduplicator, RateEstimator, SlidingWindow, TumblingWindow

VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "conformance" / "vectors.json").read_text(
        encoding="utf-8"
    )
)


def test_matches_vectors():
    window = VECTORS["window_ms"]
    dedup = Deduplicator(window)
    tumbling = TumblingWindow(window)
    sliding = SlidingWindow(window)
    rate = RateEstimator(window)

    dedup_decisions = []
    series = []
    for ev in VECTORS["stream"]:
        accepted = dedup.accept(ev["id"], ev["t"])
        dedup_decisions.append(1 if accepted else 0)
        if accepted:
            tumbling.add(ev["t"], ev["value"])
            sliding.add(ev["t"], ev["value"])
            rate.add(ev["t"])
        series.append(
            {
                "t": ev["t"],
                "count": sliding.count(ev["t"]),
                "sum": sliding.sum(ev["t"]),
                "rate_milli": rate.per_sec_milli(ev["t"]),
            }
        )

    assert dedup_decisions == VECTORS["dedup_decisions"]
    assert [
        {"bucket": b, "total": v} for (b, v) in tumbling.closed_buckets(10_000)
    ] == VECTORS["tumbling_closed"]
    assert series == VECTORS["sliding_series"]

"""Helios core: dedup within a window, tumbling rollups, sliding count/sum, and rate."""

from __future__ import annotations

from helios import Deduplicator, RateEstimator, SlidingWindow, TumblingWindow


def test_dedup_rejects_repeats_in_window():
    d = Deduplicator(window_ms=1000)
    assert d.accept("e1", 0)
    assert not d.accept("e1", 100)  # duplicate within window
    assert d.accept("e2", 100)


def test_dedup_allows_after_window():
    d = Deduplicator(window_ms=1000)
    assert d.accept("e1", 0)
    assert not d.accept("e1", 500)
    # After the window passes, the id is evicted and can be seen again.
    assert d.accept("e1", 2000)


def test_tumbling_buckets_by_interval():
    tw = TumblingWindow(window_ms=1000)
    assert tw.add(0, 1) == 1
    assert tw.add(500, 2) == 3  # same bucket
    assert tw.add(1000, 5) == 5  # new bucket
    assert tw.bucket_total(1500) == 5
    assert tw.bucket_total(0) == 3


def test_tumbling_closed_buckets():
    tw = TumblingWindow(window_ms=1000)
    tw.add(0, 10)
    tw.add(1000, 20)
    tw.add(2000, 30)
    # At t=2500 the current bucket is 2; buckets 0 and 1 are closed.
    assert tw.closed_buckets(2500) == [(0, 10), (1, 20)]


def test_sliding_count_and_sum():
    sw = SlidingWindow(window_ms=1000)
    sw.add(0, 5)
    sw.add(500, 3)
    assert sw.count(900) == 2
    assert sw.sum(900) == 8
    # At t=1001 the event at t=0 has aged out.
    assert sw.count(1001) == 1
    assert sw.sum(1001) == 3


def test_sliding_evicts_old_events():
    sw = SlidingWindow(window_ms=1000)
    for t in range(0, 3000, 100):
        sw.add(t, 1)
    # Only events within the last 1000ms remain.
    assert sw.count(2900) <= 11


def test_rate_estimator():
    re = RateEstimator(window_ms=1000)
    for t in range(0, 1000, 100):  # 10 events in the window
        re.add(t)
    # 10 events / 1s = 10.0/sec -> 10000 milli.
    assert re.per_sec_milli(999) == 10000


def test_empty_windows():
    assert SlidingWindow(1000).count(0) == 0
    assert TumblingWindow(1000).bucket_total(0) == 0
    assert RateEstimator(1000).per_sec_milli(0) == 0

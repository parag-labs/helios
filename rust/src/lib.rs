//! Helios: real-time event analytics primitives — deduplication, tumbling windows,
//! sliding windows, and a rate estimator. Integer arithmetic over millisecond
//! timestamps so every language port agrees byte-for-byte (see conformance/).

use std::collections::{HashMap, VecDeque};

pub struct Deduplicator {
    window_ms: i64,
    seen: HashMap<String, i64>,
    order: VecDeque<(i64, String)>,
}

impl Deduplicator {
    pub fn new(window_ms: i64) -> Self {
        assert!(window_ms >= 1, "window_ms must be at least 1");
        Self {
            window_ms,
            seen: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn accept(&mut self, event_id: &str, now_ms: i64) -> bool {
        self.evict(now_ms);
        if self.seen.contains_key(event_id) {
            return false;
        }
        self.seen.insert(event_id.to_string(), now_ms);
        self.order.push_back((now_ms, event_id.to_string()));
        true
    }

    fn evict(&mut self, now_ms: i64) {
        let cutoff = now_ms - self.window_ms;
        while let Some((ts, id)) = self.order.front() {
            if *ts <= cutoff {
                if self.seen.get(id).copied().unwrap_or(i64::MAX) <= cutoff {
                    self.seen.remove(id);
                }
                self.order.pop_front();
            } else {
                break;
            }
        }
    }
}

pub struct TumblingWindow {
    window_ms: i64,
    buckets: HashMap<i64, i64>,
}

/// A closed tumbling rollup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    pub bucket: i64,
    pub total: i64,
}

impl TumblingWindow {
    pub fn new(window_ms: i64) -> Self {
        assert!(window_ms >= 1, "window_ms must be at least 1");
        Self {
            window_ms,
            buckets: HashMap::new(),
        }
    }

    pub fn add(&mut self, now_ms: i64, value: i64) -> i64 {
        let b = now_ms.div_euclid(self.window_ms);
        let e = self.buckets.entry(b).or_insert(0);
        *e += value;
        *e
    }

    pub fn bucket_total(&self, now_ms: i64) -> i64 {
        *self
            .buckets
            .get(&now_ms.div_euclid(self.window_ms))
            .unwrap_or(&0)
    }

    pub fn closed_buckets(&self, now_ms: i64) -> Vec<Bucket> {
        let current = now_ms.div_euclid(self.window_ms);
        let mut out: Vec<Bucket> = self
            .buckets
            .iter()
            .filter(|(&b, _)| b < current)
            .map(|(&bucket, &total)| Bucket { bucket, total })
            .collect();
        out.sort_by_key(|b| b.bucket);
        out
    }
}

pub struct SlidingWindow {
    window_ms: i64,
    events: VecDeque<(i64, i64)>,
    sum: i64,
}

impl SlidingWindow {
    pub fn new(window_ms: i64) -> Self {
        assert!(window_ms >= 1, "window_ms must be at least 1");
        Self {
            window_ms,
            events: VecDeque::new(),
            sum: 0,
        }
    }

    pub fn add(&mut self, now_ms: i64, value: i64) {
        self.events.push_back((now_ms, value));
        self.sum += value;
        self.evict(now_ms);
    }

    fn evict(&mut self, now_ms: i64) {
        let cutoff = now_ms - self.window_ms;
        while let Some(&(ts, val)) = self.events.front() {
            if ts <= cutoff {
                self.sum -= val;
                self.events.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn count(&mut self, now_ms: i64) -> i64 {
        self.evict(now_ms);
        self.events.len() as i64
    }

    pub fn sum(&mut self, now_ms: i64) -> i64 {
        self.evict(now_ms);
        self.sum
    }
}

pub struct RateEstimator {
    window_ms: i64,
    window: SlidingWindow,
}

impl RateEstimator {
    pub fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            window: SlidingWindow::new(window_ms),
        }
    }

    pub fn add(&mut self, now_ms: i64) {
        self.window.add(now_ms, 1);
    }

    pub fn per_sec_milli(&mut self, now_ms: i64) -> i64 {
        self.window.count(now_ms) * 1000 * 1000 / self.window_ms
    }
}

// Package helios implements real-time event analytics primitives: deduplication,
// tumbling windows, sliding windows, and a rate estimator. All integer arithmetic over
// millisecond timestamps so every language port agrees byte-for-byte (see conformance/).
package helios

import "sort"

// Deduplicator drops events whose id was seen within the trailing window.
type Deduplicator struct {
	windowMs int64
	seen     map[string]int64
	order    []idAt
}

type idAt struct {
	ts int64
	id string
}

func NewDeduplicator(windowMs int64) *Deduplicator {
	if windowMs < 1 {
		panic("windowMs must be at least 1")
	}
	return &Deduplicator{windowMs: windowMs, seen: map[string]int64{}}
}

// Accept reports whether eventID is seen for the first time within the window.
func (d *Deduplicator) Accept(eventID string, nowMs int64) bool {
	d.evict(nowMs)
	if _, ok := d.seen[eventID]; ok {
		return false
	}
	d.seen[eventID] = nowMs
	d.order = append(d.order, idAt{nowMs, eventID})
	return true
}

func (d *Deduplicator) evict(nowMs int64) {
	cutoff := nowMs - d.windowMs
	i := 0
	for i < len(d.order) && d.order[i].ts <= cutoff {
		if d.seen[d.order[i].id] <= cutoff {
			delete(d.seen, d.order[i].id)
		}
		i++
	}
	d.order = d.order[i:]
}

// TumblingWindow buckets events into fixed non-overlapping intervals.
type TumblingWindow struct {
	windowMs int64
	buckets  map[int64]int64
}

func NewTumblingWindow(windowMs int64) *TumblingWindow {
	if windowMs < 1 {
		panic("windowMs must be at least 1")
	}
	return &TumblingWindow{windowMs: windowMs, buckets: map[int64]int64{}}
}

func (t *TumblingWindow) Add(nowMs, value int64) int64 {
	b := nowMs / t.windowMs
	t.buckets[b] += value
	return t.buckets[b]
}

func (t *TumblingWindow) BucketTotal(nowMs int64) int64 {
	return t.buckets[nowMs/t.windowMs]
}

// Bucket is a closed tumbling rollup.
type Bucket struct {
	Bucket int64 `json:"bucket"`
	Total  int64 `json:"total"`
}

func (t *TumblingWindow) ClosedBuckets(nowMs int64) []Bucket {
	current := nowMs / t.windowMs
	out := []Bucket{}
	for b, v := range t.buckets {
		if b < current {
			out = append(out, Bucket{b, v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bucket < out[j].Bucket })
	return out
}

// SlidingWindow reports count and sum within the trailing window.
type SlidingWindow struct {
	windowMs int64
	events   []tsVal
	sum      int64
}

type tsVal struct {
	ts  int64
	val int64
}

func NewSlidingWindow(windowMs int64) *SlidingWindow {
	if windowMs < 1 {
		panic("windowMs must be at least 1")
	}
	return &SlidingWindow{windowMs: windowMs}
}

func (s *SlidingWindow) Add(nowMs, value int64) {
	s.events = append(s.events, tsVal{nowMs, value})
	s.sum += value
	s.evict(nowMs)
}

func (s *SlidingWindow) evict(nowMs int64) {
	cutoff := nowMs - s.windowMs
	i := 0
	for i < len(s.events) && s.events[i].ts <= cutoff {
		s.sum -= s.events[i].val
		i++
	}
	s.events = s.events[i:]
}

func (s *SlidingWindow) Count(nowMs int64) int64 {
	s.evict(nowMs)
	return int64(len(s.events))
}

func (s *SlidingWindow) Sum(nowMs int64) int64 {
	s.evict(nowMs)
	return s.sum
}

// RateEstimator reports events/sec * 1000 over the trailing window.
type RateEstimator struct {
	windowMs int64
	window   *SlidingWindow
}

func NewRateEstimator(windowMs int64) *RateEstimator {
	return &RateEstimator{windowMs: windowMs, window: NewSlidingWindow(windowMs)}
}

func (r *RateEstimator) Add(nowMs int64) { r.window.Add(nowMs, 1) }

func (r *RateEstimator) PerSecMilli(nowMs int64) int64 {
	return r.window.Count(nowMs) * 1000 * 1000 / r.windowMs
}

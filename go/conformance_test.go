package helios

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type event struct {
	ID    string `json:"id"`
	T     int64  `json:"t"`
	Value int64  `json:"value"`
}

type seriesPoint struct {
	T         int64 `json:"t"`
	Count     int64 `json:"count"`
	Sum       int64 `json:"sum"`
	RateMilli int64 `json:"rate_milli"`
}

type vectors struct {
	WindowMs       int64         `json:"window_ms"`
	Stream         []event       `json:"stream"`
	DedupDecisions []int         `json:"dedup_decisions"`
	TumblingClosed []Bucket      `json:"tumbling_closed"`
	SlidingSeries  []seriesPoint `json:"sliding_series"`
}

func TestConformance(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "conformance", "vectors.json"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse: %v", err)
	}

	dedup := NewDeduplicator(v.WindowMs)
	tumbling := NewTumblingWindow(v.WindowMs)
	sliding := NewSlidingWindow(v.WindowMs)
	rate := NewRateEstimator(v.WindowMs)

	for i, ev := range v.Stream {
		accepted := dedup.Accept(ev.ID, ev.T)
		want := v.DedupDecisions[i] == 1
		if accepted != want {
			t.Errorf("dedup[%d]=%v want %v", i, accepted, want)
		}
		if accepted {
			tumbling.Add(ev.T, ev.Value)
			sliding.Add(ev.T, ev.Value)
			rate.Add(ev.T)
		}
		sp := seriesPoint{ev.T, sliding.Count(ev.T), sliding.Sum(ev.T), rate.PerSecMilli(ev.T)}
		if sp != v.SlidingSeries[i] {
			t.Errorf("series[%d]=%+v want %+v", i, sp, v.SlidingSeries[i])
		}
	}

	closed := tumbling.ClosedBuckets(10000)
	if len(closed) != len(v.TumblingClosed) {
		t.Fatalf("closed buckets %d want %d", len(closed), len(v.TumblingClosed))
	}
	for i, b := range closed {
		if b != v.TumblingClosed[i] {
			t.Errorf("bucket[%d]=%+v want %+v", i, b, v.TumblingClosed[i])
		}
	}
}

package helios

import "testing"

func TestDedupRejectsRepeats(t *testing.T) {
	d := NewDeduplicator(1000)
	if !d.Accept("e1", 0) {
		t.Fatal("first should be accepted")
	}
	if d.Accept("e1", 100) {
		t.Fatal("duplicate in window should be rejected")
	}
	if !d.Accept("e1", 2000) {
		t.Fatal("after window should be accepted again")
	}
}

func TestTumblingBuckets(t *testing.T) {
	tw := NewTumblingWindow(1000)
	if tw.Add(0, 1) != 1 || tw.Add(500, 2) != 3 {
		t.Fatal("same bucket accumulation wrong")
	}
	if tw.Add(1000, 5) != 5 {
		t.Fatal("new bucket should reset")
	}
}

func TestSlidingCountSum(t *testing.T) {
	sw := NewSlidingWindow(1000)
	sw.Add(0, 5)
	sw.Add(500, 3)
	if sw.Count(900) != 2 || sw.Sum(900) != 8 {
		t.Fatal("count/sum in window wrong")
	}
	if sw.Count(1001) != 1 {
		t.Fatal("old event should evict")
	}
}

func TestRate(t *testing.T) {
	re := NewRateEstimator(1000)
	for tms := int64(0); tms < 1000; tms += 100 {
		re.Add(tms)
	}
	if re.PerSecMilli(999) != 10000 {
		t.Fatalf("want 10000 milli, got %d", re.PerSecMilli(999))
	}
}

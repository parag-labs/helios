use helios::{Bucket, Deduplicator, RateEstimator, SlidingWindow, TumblingWindow};
use serde_json::Value;
use std::fs;
use std::path::Path;

#[test]
fn conformance() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("conformance")
        .join("vectors.json");
    let v: Value = serde_json::from_str(&fs::read_to_string(p).unwrap()).unwrap();
    let window = v["window_ms"].as_i64().unwrap();

    let mut dedup = Deduplicator::new(window);
    let mut tumbling = TumblingWindow::new(window);
    let mut sliding = SlidingWindow::new(window);
    let mut rate = RateEstimator::new(window);

    let stream = v["stream"].as_array().unwrap();
    let dedup_want = v["dedup_decisions"].as_array().unwrap();
    let series = v["sliding_series"].as_array().unwrap();

    for (i, ev) in stream.iter().enumerate() {
        let id = ev["id"].as_str().unwrap();
        let t = ev["t"].as_i64().unwrap();
        let value = ev["value"].as_i64().unwrap();
        let accepted = dedup.accept(id, t);
        assert_eq!(accepted, dedup_want[i].as_i64().unwrap() == 1, "dedup {i}");
        if accepted {
            tumbling.add(t, value);
            sliding.add(t, value);
            rate.add(t);
        }
        assert_eq!(
            sliding.count(t),
            series[i]["count"].as_i64().unwrap(),
            "count {i}"
        );
        assert_eq!(
            sliding.sum(t),
            series[i]["sum"].as_i64().unwrap(),
            "sum {i}"
        );
        assert_eq!(
            rate.per_sec_milli(t),
            series[i]["rate_milli"].as_i64().unwrap(),
            "rate {i}"
        );
    }

    let closed = tumbling.closed_buckets(10000);
    let want: Vec<Bucket> = v["tumbling_closed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| Bucket {
            bucket: b["bucket"].as_i64().unwrap(),
            total: b["total"].as_i64().unwrap(),
        })
        .collect();
    assert_eq!(closed, want);
}

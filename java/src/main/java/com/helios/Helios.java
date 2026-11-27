package com.helios;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helios: real-time event analytics primitives — deduplication, tumbling windows,
 * sliding windows, and a rate estimator. Integer arithmetic over millisecond timestamps
 * so every language port agrees byte-for-byte (see conformance/).
 */
public final class Helios {

    private Helios() {
    }

    public record Bucket(long bucket, long total) {
    }

    public static final class Deduplicator {
        private final long windowMs;
        private final Map<String, Long> seen = new HashMap<>();
        private final Deque<long[]> order = new ArrayDeque<>();
        private final Map<Integer, String> ids = new HashMap<>();
        private int nextId = 0;

        public Deduplicator(long windowMs) {
            if (windowMs < 1) throw new IllegalArgumentException("windowMs must be at least 1");
            this.windowMs = windowMs;
        }

        public boolean accept(String eventId, long nowMs) {
            evict(nowMs);
            if (seen.containsKey(eventId)) return false;
            seen.put(eventId, nowMs);
            int token = nextId++;
            ids.put(token, eventId);
            order.addLast(new long[] {nowMs, token});
            return true;
        }

        private void evict(long nowMs) {
            long cutoff = nowMs - windowMs;
            while (!order.isEmpty() && order.peekFirst()[0] <= cutoff) {
                long[] head = order.pollFirst();
                String id = ids.remove((int) head[1]);
                Long ts = seen.get(id);
                if (ts != null && ts <= cutoff) seen.remove(id);
            }
        }
    }

    public static final class TumblingWindow {
        private final long windowMs;
        private final Map<Long, Long> buckets = new HashMap<>();

        public TumblingWindow(long windowMs) {
            if (windowMs < 1) throw new IllegalArgumentException("windowMs must be at least 1");
            this.windowMs = windowMs;
        }

        public long add(long nowMs, long value) {
            long b = Math.floorDiv(nowMs, windowMs);
            long v = buckets.getOrDefault(b, 0L) + value;
            buckets.put(b, v);
            return v;
        }

        public long bucketTotal(long nowMs) {
            return buckets.getOrDefault(Math.floorDiv(nowMs, windowMs), 0L);
        }

        public List<Bucket> closedBuckets(long nowMs) {
            long current = Math.floorDiv(nowMs, windowMs);
            List<Bucket> out = new ArrayList<>();
            for (Map.Entry<Long, Long> e : buckets.entrySet()) {
                if (e.getKey() < current) out.add(new Bucket(e.getKey(), e.getValue()));
            }
            out.sort(Comparator.comparingLong(Bucket::bucket));
            return out;
        }
    }

    public static final class SlidingWindow {
        private final long windowMs;
        private final Deque<long[]> events = new ArrayDeque<>();
        private long sum = 0;

        public SlidingWindow(long windowMs) {
            if (windowMs < 1) throw new IllegalArgumentException("windowMs must be at least 1");
            this.windowMs = windowMs;
        }

        public void add(long nowMs, long value) {
            events.addLast(new long[] {nowMs, value});
            sum += value;
            evict(nowMs);
        }

        private void evict(long nowMs) {
            long cutoff = nowMs - windowMs;
            while (!events.isEmpty() && events.peekFirst()[0] <= cutoff) {
                sum -= events.pollFirst()[1];
            }
        }

        public long count(long nowMs) {
            evict(nowMs);
            return events.size();
        }

        public long sum(long nowMs) {
            evict(nowMs);
            return sum;
        }
    }

    public static final class RateEstimator {
        private final long windowMs;
        private final SlidingWindow window;

        public RateEstimator(long windowMs) {
            this.windowMs = windowMs;
            this.window = new SlidingWindow(windowMs);
        }

        public void add(long nowMs) {
            window.add(nowMs, 1);
        }

        public long perSecMilli(long nowMs) {
            return window.count(nowMs) * 1000 * 1000 / windowMs;
        }
    }
}

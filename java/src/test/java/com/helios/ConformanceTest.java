package com.helios;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.helios.Helios.Bucket;
import com.helios.Helios.Deduplicator;
import com.helios.Helios.RateEstimator;
import com.helios.Helios.SlidingWindow;
import com.helios.Helios.TumblingWindow;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

class ConformanceTest {

    private static JSONObject vectors() throws IOException {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null && !Files.exists(dir.resolve("conformance/vectors.json"))) {
            dir = dir.getParent();
        }
        if (dir == null) throw new IOException("conformance/vectors.json not found");
        return new JSONObject(Files.readString(dir.resolve("conformance/vectors.json")));
    }

    @Test
    void matchesVectors() throws IOException {
        JSONObject v = vectors();
        long window = v.getLong("window_ms");
        Deduplicator dedup = new Deduplicator(window);
        TumblingWindow tumbling = new TumblingWindow(window);
        SlidingWindow sliding = new SlidingWindow(window);
        RateEstimator rate = new RateEstimator(window);

        JSONArray stream = v.getJSONArray("stream");
        JSONArray dedupWant = v.getJSONArray("dedup_decisions");
        JSONArray series = v.getJSONArray("sliding_series");

        for (int i = 0; i < stream.length(); i++) {
            JSONObject ev = stream.getJSONObject(i);
            long t = ev.getLong("t");
            boolean accepted = dedup.accept(ev.getString("id"), t);
            assertEquals(dedupWant.getInt(i) == 1, accepted, "dedup " + i);
            if (accepted) {
                tumbling.add(t, ev.getLong("value"));
                sliding.add(t, ev.getLong("value"));
                rate.add(t);
            }
            JSONObject sp = series.getJSONObject(i);
            assertEquals(sp.getLong("count"), sliding.count(t), "count " + i);
            assertEquals(sp.getLong("sum"), sliding.sum(t), "sum " + i);
            assertEquals(sp.getLong("rate_milli"), rate.perSecMilli(t), "rate " + i);
        }

        List<Bucket> closed = tumbling.closedBuckets(10000);
        JSONArray wantClosed = v.getJSONArray("tumbling_closed");
        List<Bucket> want = new ArrayList<>();
        for (int i = 0; i < wantClosed.length(); i++) {
            JSONObject b = wantClosed.getJSONObject(i);
            want.add(new Bucket(b.getLong("bucket"), b.getLong("total")));
        }
        assertEquals(want, closed);
    }
}

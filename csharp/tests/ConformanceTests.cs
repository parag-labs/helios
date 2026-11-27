using System.Text.Json;
using Xunit;

namespace Helios.Tests;

public class ConformanceTests
{
    private static readonly JsonElement V = Load();

    private static JsonElement Load()
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "conformance", "vectors.json")))
            dir = Directory.GetParent(dir)?.FullName;
        if (dir is null) throw new FileNotFoundException("conformance/vectors.json");
        return JsonDocument.Parse(File.ReadAllText(Path.Combine(dir, "conformance", "vectors.json")))
            .RootElement.Clone();
    }

    [Fact]
    public void MatchesVectors()
    {
        var window = V.GetProperty("window_ms").GetInt64();
        var dedup = new Deduplicator(window);
        var tumbling = new TumblingWindow(window);
        var sliding = new SlidingWindow(window);
        var rate = new RateEstimator(window);

        var stream = V.GetProperty("stream").EnumerateArray().ToArray();
        var dedupWant = V.GetProperty("dedup_decisions").EnumerateArray().Select(x => x.GetInt32()).ToArray();
        var series = V.GetProperty("sliding_series").EnumerateArray().ToArray();

        for (var i = 0; i < stream.Length; i++)
        {
            var ev = stream[i];
            var t = ev.GetProperty("t").GetInt64();
            var accepted = dedup.Accept(ev.GetProperty("id").GetString()!, t);
            Assert.Equal(dedupWant[i] == 1, accepted);
            if (accepted)
            {
                tumbling.Add(t, ev.GetProperty("value").GetInt64());
                sliding.Add(t, ev.GetProperty("value").GetInt64());
                rate.Add(t);
            }
            Assert.Equal(series[i].GetProperty("count").GetInt64(), sliding.Count(t));
            Assert.Equal(series[i].GetProperty("sum").GetInt64(), sliding.Sum(t));
            Assert.Equal(series[i].GetProperty("rate_milli").GetInt64(), rate.PerSecMilli(t));
        }

        var closed = tumbling.ClosedBuckets(10000);
        var wantClosed = V.GetProperty("tumbling_closed").EnumerateArray()
            .Select(b => new Bucket(b.GetProperty("bucket").GetInt64(), b.GetProperty("total").GetInt64()))
            .ToList();
        Assert.Equal(wantClosed, closed);
    }
}

namespace Helios;

/// <summary>
/// Helios: real-time event analytics primitives — deduplication, tumbling windows,
/// sliding windows, and a rate estimator. Integer arithmetic over millisecond
/// timestamps so every language port agrees byte-for-byte (see conformance/).
/// </summary>
public sealed class Deduplicator
{
    private readonly long _windowMs;
    private readonly Dictionary<string, long> _seen = new();
    private readonly Queue<(long ts, string id)> _order = new();

    public Deduplicator(long windowMs)
    {
        if (windowMs < 1) throw new ArgumentOutOfRangeException(nameof(windowMs));
        _windowMs = windowMs;
    }

    public bool Accept(string eventId, long nowMs)
    {
        Evict(nowMs);
        if (_seen.ContainsKey(eventId)) return false;
        _seen[eventId] = nowMs;
        _order.Enqueue((nowMs, eventId));
        return true;
    }

    private void Evict(long nowMs)
    {
        var cutoff = nowMs - _windowMs;
        while (_order.Count > 0 && _order.Peek().ts <= cutoff)
        {
            var (_, id) = _order.Dequeue();
            if (_seen.TryGetValue(id, out var ts) && ts <= cutoff) _seen.Remove(id);
        }
    }
}

public readonly record struct Bucket(long BucketIndex, long Total);

public sealed class TumblingWindow
{
    private readonly long _windowMs;
    private readonly Dictionary<long, long> _buckets = new();

    public TumblingWindow(long windowMs)
    {
        if (windowMs < 1) throw new ArgumentOutOfRangeException(nameof(windowMs));
        _windowMs = windowMs;
    }

    public long Add(long nowMs, long value)
    {
        var b = FloorDiv(nowMs, _windowMs);
        _buckets[b] = _buckets.GetValueOrDefault(b, 0) + value;
        return _buckets[b];
    }

    public long BucketTotal(long nowMs) => _buckets.GetValueOrDefault(FloorDiv(nowMs, _windowMs), 0);

    public List<Bucket> ClosedBuckets(long nowMs)
    {
        var current = FloorDiv(nowMs, _windowMs);
        return _buckets.Where(kv => kv.Key < current)
            .OrderBy(kv => kv.Key)
            .Select(kv => new Bucket(kv.Key, kv.Value))
            .ToList();
    }

    private static long FloorDiv(long a, long b) => (long)Math.Floor((double)a / b);
}

public sealed class SlidingWindow
{
    private readonly long _windowMs;
    private readonly Queue<(long ts, long val)> _events = new();
    private long _sum;

    public SlidingWindow(long windowMs)
    {
        if (windowMs < 1) throw new ArgumentOutOfRangeException(nameof(windowMs));
        _windowMs = windowMs;
    }

    public void Add(long nowMs, long value)
    {
        _events.Enqueue((nowMs, value));
        _sum += value;
        Evict(nowMs);
    }

    private void Evict(long nowMs)
    {
        var cutoff = nowMs - _windowMs;
        while (_events.Count > 0 && _events.Peek().ts <= cutoff)
        {
            _sum -= _events.Dequeue().val;
        }
    }

    public long Count(long nowMs) { Evict(nowMs); return _events.Count; }
    public long Sum(long nowMs) { Evict(nowMs); return _sum; }
}

public sealed class RateEstimator
{
    private readonly long _windowMs;
    private readonly SlidingWindow _window;

    public RateEstimator(long windowMs)
    {
        _windowMs = windowMs;
        _window = new SlidingWindow(windowMs);
    }

    public void Add(long nowMs) => _window.Add(nowMs, 1);
    public long PerSecMilli(long nowMs) => _window.Count(nowMs) * 1000 * 1000 / _windowMs;
}

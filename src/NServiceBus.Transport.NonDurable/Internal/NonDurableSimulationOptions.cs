namespace NServiceBus;

using System;
using System.Threading.RateLimiting;

public sealed class NonDurableSimulationOptions
{
    public TimeProvider? TimeProvider { get; set; }

    public NonDurableSimulationMode? Mode { get; set; }

    public NonDurableRateLimitOptions? RateLimit { get; set; }

    public RateLimiter? RateLimiter { get; set; }

    public Func<TimeProvider, RateLimiter>? RateLimiterFactory { get; set; }
}
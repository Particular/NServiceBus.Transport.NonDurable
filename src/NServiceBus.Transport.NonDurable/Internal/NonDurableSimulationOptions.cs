namespace NServiceBus;

using System;
using System.Threading.RateLimiting;

/// <summary>
/// Simulation options for a broker node (broker-level or queue-level operation).
/// </summary>
/// <remarks>
/// Values are snapshotted when a <see cref="NonDurableBroker" /> is constructed; later mutation
/// does not affect an already-constructed broker. Queue operation settings take precedence over
/// queue defaults, followed by broker operation settings and broker defaults. A direct
/// <see cref="RateLimiter" /> reference is captured by identity and remains caller-owned.
/// </remarks>
public sealed class NonDurableSimulationOptions
{
    /// <summary>
    /// Gets or sets the time provider used by this simulation node.
    /// </summary>
    public TimeProvider? TimeProvider { get; set; }

    /// <summary>
    /// Gets or sets whether constrained operations wait for a permit or are rejected.
    /// </summary>
    public NonDurableSimulationMode? Mode { get; set; }

    /// <summary>
    /// Gets or sets the broker-owned fixed-window rate-limit configuration.
    /// </summary>
    /// <remarks>Only one of <see cref="RateLimit" />, <see cref="RateLimiter" />, or <see cref="RateLimiterFactory" /> may be configured on a node.</remarks>
    public NonDurableRateLimitOptions? RateLimit { get; set; }

    /// <summary>
    /// Gets or sets a caller-owned rate limiter used directly by this simulation node.
    /// </summary>
    /// <remarks>The broker does not dispose a directly supplied limiter.</remarks>
    public RateLimiter? RateLimiter { get; set; }

    /// <summary>
    /// Gets or sets a factory that creates a rate limiter using the effective time provider.
    /// </summary>
    /// <remarks>Factory-created limiters are cached per operation and queue and disposed with the broker.</remarks>
    public Func<TimeProvider, RateLimiter>? RateLimiterFactory { get; set; }

    internal NonDurableSimulationOptions Clone() => new()
    {
        TimeProvider = TimeProvider,
        Mode = Mode,
        RateLimit = RateLimit?.Clone(),
        RateLimiter = RateLimiter,
        RateLimiterFactory = RateLimiterFactory
    };
}
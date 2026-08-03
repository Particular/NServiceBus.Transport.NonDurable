namespace NServiceBus;

using System;

/// <summary>
/// Fixed-window rate-limit configuration for built-in broker-owned simulation.
/// </summary>
/// <remarks>
/// Values are snapshotted when a <see cref="NonDurableBroker" /> is constructed. A <see cref="PermitLimit" />
/// of zero is a supported pause mechanism; <see cref="Window" /> must be strictly positive.
/// </remarks>
public sealed class NonDurableRateLimitOptions
{
    /// <summary>
    /// Gets the maximum number of operations permitted during each window.
    /// </summary>
    /// <remarks>A value of zero pauses the configured operation.</remarks>
    public required int PermitLimit { get; init; }

    /// <summary>
    /// Gets the duration of each rate-limit window.
    /// </summary>
    /// <remarks>The value must be greater than <see cref="TimeSpan.Zero" />.</remarks>
    public required TimeSpan Window { get; init; }

    internal NonDurableRateLimitOptions Clone() => new()
    {
        PermitLimit = PermitLimit,
        Window = Window
    };
}
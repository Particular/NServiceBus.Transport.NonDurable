namespace NServiceBus;

using System;

/// <summary>
/// Queue-specific simulation options.
/// </summary>
/// <remarks>
/// Values are snapshotted when a <see cref="NonDurableBroker" /> is constructed; later mutation
/// does not affect an already-constructed broker.
/// </remarks>
public sealed class NonDurableQueueSimulationOptions
{
    /// <summary>
    /// Gets or sets the time provider used for this queue when no operation-specific provider is configured.
    /// </summary>
    public TimeProvider? TimeProvider { get; set; }

    /// <summary>
    /// Gets the fallback simulation settings for all operations on this queue.
    /// </summary>
    public NonDurableSimulationOptions Default { get; private init; } = new();

    /// <summary>
    /// Gets the simulation settings for sends targeting this queue.
    /// </summary>
    public NonDurableSimulationOptions Send { get; private init; } = new();

    /// <summary>
    /// Gets the simulation settings for receives from this queue.
    /// </summary>
    public NonDurableSimulationOptions Receive { get; private init; } = new();

    /// <summary>
    /// Gets the simulation settings for releasing delayed messages targeting this queue.
    /// </summary>
    public NonDurableSimulationOptions DelayedDelivery { get; private init; } = new();

    internal NonDurableQueueSimulationOptions Clone() => new()
    {
        TimeProvider = TimeProvider,
        Default = Default.Clone(),
        Send = Send.Clone(),
        Receive = Receive.Clone(),
        DelayedDelivery = DelayedDelivery.Clone()
    };
}
namespace NServiceBus;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Configuration for a <see cref="NonDurableBroker" />.
/// </summary>
/// <remarks>
/// These options are snapshotted when passed to the <see cref="NonDurableBroker" /> constructor.
/// Mutating them afterwards (including nested nodes, queue nodes, or adding queues via <see cref="ForQueue" />)
/// affects only brokers constructed afterwards, not an already-constructed broker.
/// </remarks>
public sealed class NonDurableBrokerOptions
{
    /// <summary>
    /// Gets the broker-wide time provider used when no more specific simulation option supplies one.
    /// </summary>
    /// <remarks>Defaults to <see cref="System.TimeProvider.System" /> when not configured.</remarks>
    public TimeProvider? TimeProvider { get; init; }

    /// <summary>
    /// Gets the broker-wide fallback simulation settings for all operations.
    /// </summary>
    public NonDurableSimulationOptions Default { get; private init; } = new();

    /// <summary>
    /// Gets the broker-wide simulation settings for send operations.
    /// </summary>
    public NonDurableSimulationOptions Send { get; private init; } = new();

    /// <summary>
    /// Gets the broker-wide simulation settings for receive operations.
    /// </summary>
    public NonDurableSimulationOptions Receive { get; private init; } = new();

    /// <summary>
    /// Gets the broker-wide simulation settings for releasing due delayed messages.
    /// </summary>
    public NonDurableSimulationOptions DelayedDelivery { get; private init; } = new();

    /// <summary>
    /// Configures simulation options for a specific queue.
    /// </summary>
    /// <param name="queue">The queue address.</param>
    /// <remarks>
    /// The returned options are snapshotted by any broker already constructed; brokers constructed
    /// afterwards see the latest values.
    /// </remarks>
    public NonDurableQueueSimulationOptions ForQueue(string queue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        return queues.GetOrAdd(queue, static _ => new NonDurableQueueSimulationOptions());
    }

    internal NonDurableBrokerOptions Clone()
    {
        var clone = new NonDurableBrokerOptions
        {
            TimeProvider = TimeProvider,
            Default = Default.Clone(),
            Send = Send.Clone(),
            Receive = Receive.Clone(),
            DelayedDelivery = DelayedDelivery.Clone()
        };

        foreach (var (queue, queueOptions) in queues)
        {
            clone.queues[queue] = queueOptions.Clone();
        }

        return clone;
    }

    internal bool TryGetQueue(string queue, [NotNullWhen(true)] out NonDurableQueueSimulationOptions? options) => queues.TryGetValue(queue, out options);

    internal IEnumerable<NonDurableQueueSimulationOptions> GetQueues() => queues.Values;

    readonly ConcurrentDictionary<string, NonDurableQueueSimulationOptions> queues = new(StringComparer.OrdinalIgnoreCase);
}
namespace NServiceBus;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

public sealed class NonDurableBrokerOptions
{
    public TimeProvider? TimeProvider { get; init; }

    public NonDurableSimulationOptions Default { get; } = new();

    public NonDurableSimulationOptions Send { get; } = new();

    public NonDurableSimulationOptions Receive { get; } = new();

    public NonDurableSimulationOptions DelayedDelivery { get; } = new();

    public NonDurableQueueSimulationOptions ForQueue(string queue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        return queues.GetOrAdd(queue, static _ => new NonDurableQueueSimulationOptions());
    }

    internal bool TryGetQueue(string queue, [NotNullWhen(true)] out NonDurableQueueSimulationOptions? options) => queues.TryGetValue(queue, out options);

    internal IEnumerable<NonDurableQueueSimulationOptions> GetQueues() => queues.Values;

    readonly ConcurrentDictionary<string, NonDurableQueueSimulationOptions> queues = new(StringComparer.OrdinalIgnoreCase);
}
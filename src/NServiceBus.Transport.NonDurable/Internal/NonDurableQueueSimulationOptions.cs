namespace NServiceBus;

using System;

public sealed class NonDurableQueueSimulationOptions
{
    public TimeProvider? TimeProvider { get; set; }

    public NonDurableSimulationOptions Default { get; } = new();

    public NonDurableSimulationOptions Send { get; } = new();

    public NonDurableSimulationOptions Receive { get; } = new();

    public NonDurableSimulationOptions DelayedDelivery { get; } = new();
}

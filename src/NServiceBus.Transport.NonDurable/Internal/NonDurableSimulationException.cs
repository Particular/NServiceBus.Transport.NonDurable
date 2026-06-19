namespace NServiceBus;

using System;

public sealed class NonDurableSimulationException(string message, TimeSpan retryAfter, TimeProvider timeProvider) : Exception(message)
{
    public TimeSpan RetryAfter { get; } = retryAfter;

    internal TimeProvider TimeProvider { get; } = timeProvider;
}

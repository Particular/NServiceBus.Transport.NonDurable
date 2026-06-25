namespace NServiceBus;

using System;

[Serializable]
public sealed class NonDurableSimulationException : Exception
{
    public NonDurableSimulationException() : this("Simulated exception", TimeSpan.FromSeconds(1), TimeProvider.System)
    {
    }

    public NonDurableSimulationException(string message) : this(message, TimeSpan.FromSeconds(1), TimeProvider.System)
    {
    }

    public NonDurableSimulationException(string message, Exception innerException) : this(message, TimeSpan.FromSeconds(1), TimeProvider.System)
    {
    }

    public NonDurableSimulationException(string message, TimeSpan retryAfter, TimeProvider timeProvider) : base(message)
    {
        RetryAfter = retryAfter;
        TimeProvider = timeProvider;
    }

    public NonDurableSimulationException(string message, TimeSpan retryAfter, TimeProvider timeProvider, Exception innerException) : base(message, innerException)
    {
        RetryAfter = retryAfter;
        TimeProvider = timeProvider;
    }

    public TimeSpan RetryAfter { get; }

    internal TimeProvider TimeProvider { get; }
}
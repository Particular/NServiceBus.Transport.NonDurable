namespace NServiceBus;

using System;

/// <summary>
/// The exception thrown when non-durable transport simulation rejects an operation.
/// </summary>
[Serializable]
public sealed class NonDurableSimulationException : Exception
{
    /// <summary>
    /// Initializes an exception with a default message and retry interval.
    /// </summary>
    public NonDurableSimulationException() : this("Simulated exception", TimeSpan.FromSeconds(1), TimeProvider.System)
    {
    }

    /// <summary>
    /// Initializes an exception with the specified message and a default retry interval.
    /// </summary>
    /// <param name="message">The message describing the simulated rejection.</param>
    public NonDurableSimulationException(string message) : this(message, TimeSpan.FromSeconds(1), TimeProvider.System)
    {
    }

    /// <summary>
    /// Initializes an exception with the specified message, inner exception, and a default retry interval.
    /// </summary>
    /// <param name="message">The message describing the simulated rejection.</param>
    /// <param name="innerException">The exception that caused the simulated rejection.</param>
    public NonDurableSimulationException(string message, Exception innerException) : this(message, TimeSpan.FromSeconds(1), TimeProvider.System, innerException)
    {
    }

    internal NonDurableSimulationException(string message, TimeSpan retryAfter, TimeProvider timeProvider) : base(message)
    {
        RetryAfter = retryAfter;
        TimeProvider = timeProvider;
    }

    internal NonDurableSimulationException(string message, TimeSpan retryAfter, TimeProvider timeProvider, Exception innerException) : base(message, innerException)
    {
        RetryAfter = retryAfter;
        TimeProvider = timeProvider;
    }

    /// <summary>
    /// Gets the interval after which the rejected operation may be retried.
    /// </summary>
    public TimeSpan RetryAfter { get; }

    internal TimeProvider TimeProvider { get; }
}
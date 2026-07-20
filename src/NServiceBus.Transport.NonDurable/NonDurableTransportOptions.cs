namespace NServiceBus;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Options for configuring the non-durable transport.
/// </summary>
public sealed class NonDurableTransportOptions(NonDurableBroker? broker = null)
{
    /// <summary>
    /// Gets the optional broker to use when dependency injection does not provide an <see cref="NonDurableBroker" />.
    /// </summary>
    /// <remarks>
    /// When multiple endpoints need to communicate in-memory, they should share the same broker instance.
    /// Broker resolution is optional and uses the following precedence: an <see cref="NonDurableBroker" /> resolved from
    /// <see cref="ServiceProvider" />, then the broker provided here, and finally the shared broker.
    /// For testing, omit the broker parameter and the shared broker will be used unless dependency injection supplies one.
    /// </remarks>
    public NonDurableBroker? Broker { get; } = broker;

    /// <summary>
    /// Gets or sets the inline execution options. When set to a non-null value, inline execution is enabled.
    /// </summary>
    /// <remarks>
    /// When set to a non-null value, the transport will execute incoming message pipelines inline when sending messages
    /// to local queues. This can simplify testing scenarios but should be used carefully in production code.
    /// </remarks>
    public InlineExecutionOptions? InlineExecution { get; set; }

    /// <summary>
    /// Controls the shutdown behavior for the endpoint, which by default uses
    /// <see cref="NonDurableTransportShutdownBehavior.DrainQueueBeforeShutdown" /> to provide better safe-by-default
    /// behavior than most durable message brokers provide, because otherwise in-memory messages can be lost.
    /// </summary>
    public NonDurableTransportShutdownBehavior ShutdownBehavior { get; set; } = NonDurableTransportShutdownBehavior.DrainQueueBeforeShutdown;
}
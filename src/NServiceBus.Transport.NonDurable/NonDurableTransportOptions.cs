namespace NServiceBus;

using System;
using System.Transactions;
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
    /// <see cref="NonDurableTransportShutdownBehavior.DrainQueueBeforeShutdown" /> to provide safer-by-default
    /// behavior than most durable message brokers, because otherwise in-memory messages can be lost.
    /// </summary>
    public NonDurableTransportShutdownBehavior ShutdownBehavior { get; set; } = NonDurableTransportShutdownBehavior.DrainQueueBeforeShutdown;

    /// <summary>
    /// Gets or sets the timeout for the transaction that wraps message processing in
    /// <see cref="TransportTransactionMode.SendsAtomicWithReceive" /> mode. The same timeout applies to the
    /// transaction used to commit the outcome of recoverability when processing fails.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When <c>null</c>, which is the default, <see cref="TransactionManager.DefaultTimeout" /> is used. That is
    /// 60 seconds unless changed.
    /// </para>
    /// <para>
    /// The timeout does not cancel a running message handler. If the handler is still running when the timeout
    /// expires, the transaction is aborted, and when the handler finishes the commit fails. The handler's outgoing
    /// messages and synchronized storage changes are then rolled back and the message is handed to recoverability.
    /// Set this to a value longer than the longest expected handler duration.
    /// </para>
    /// <para>
    /// The value must be greater than <see cref="TimeSpan.Zero" />. Unlike <see cref="TransactionScope" />,
    /// the transport's transaction is not capped by <see cref="TransactionManager.MaximumTimeout" />.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is less than or equal to <see cref="TimeSpan.Zero" />.</exception>
    public TimeSpan? TransactionTimeout
    {
        get;
        set
        {
            if (value.HasValue)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value.Value, TimeSpan.Zero, nameof(TransactionTimeout));
            }

            field = value;
        }
    }
}

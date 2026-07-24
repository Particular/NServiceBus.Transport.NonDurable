namespace NServiceBus;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Transport;

/// <summary>
/// Non-durable transport for testing and development.
/// </summary>
public class NonDurableTransport : TransportDefinition
{
    /// <summary>
    /// Creates a new instance of the non-durable transport.
    /// </summary>
    /// <param name="options">Optional configuration options for the transport.</param>
    /// <remarks>
    /// When multiple endpoints need to communicate in-memory, they should share the same broker instance.
    /// Broker resolution is optional and uses the following precedence: an <see cref="NonDurableBroker" /> resolved from
    /// <see cref="HostSettings.ServiceProvider" />, then the broker provided to this constructor, and finally the shared broker.
    /// For testing, omit the broker parameter and the shared broker will be used unless dependency injection supplies one.
    /// </remarks>
    public NonDurableTransport(NonDurableTransportOptions? options = null)
        : base(TransportTransactionMode.SendsAtomicWithReceive, supportsDelayedDelivery: true, supportsPublishSubscribe: true, supportsTTBR: true)
    {
        var transportOptions = options ?? new NonDurableTransportOptions();
        configuredBroker = transportOptions.Broker;
        InlineExecutionSettings = new InlineExecutionSettings(transportOptions);
        ShutdownBehavior = transportOptions.ShutdownBehavior;

        if (InlineExecutionSettings.IsEnabled)
        {
            // Enable the feature that will register the recoverability behavior
            EnableEndpointFeature<InlineExecutionFeature>();
        }
    }

    NonDurableBroker GetFallbackBroker() => configuredBroker ?? SharedBroker;

    /// <inheritdoc />
    public override Task<TransportInfrastructure> Initialize(HostSettings hostSettings, ReceiveSettings[] receivers, string[] sendingAddresses, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(hostSettings);
        ArgumentNullException.ThrowIfNull(receivers);

        var broker = ResolveBroker(hostSettings);
        var infrastructure = new NonDurableTransportInfrastructure(hostSettings, receivers, this, broker);
        return Task.FromResult<TransportInfrastructure>(infrastructure);
    }

    NonDurableBroker ResolveBroker(HostSettings hostSettings) => hostSettings.ServiceProvider?.GetService<NonDurableBroker>() ?? GetFallbackBroker();

    /// <inheritdoc />
    public override IReadOnlyCollection<TransportTransactionMode> GetSupportedTransactionModes() =>
    [
        TransportTransactionMode.None,
        TransportTransactionMode.ReceiveOnly,
        TransportTransactionMode.SendsAtomicWithReceive
    ];

    static NonDurableBroker SharedBroker { get; } = new();

    readonly NonDurableBroker? configuredBroker;

    internal InlineExecutionSettings InlineExecutionSettings { get; }

    internal NonDurableTransportShutdownBehavior ShutdownBehavior { get; }
}
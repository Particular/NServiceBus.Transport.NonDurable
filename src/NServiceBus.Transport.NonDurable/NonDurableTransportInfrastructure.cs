namespace NServiceBus;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Transport;

class NonDurableTransportInfrastructure : TransportInfrastructure
{
    public NonDurableTransportInfrastructure(HostSettings hostSettings, ReceiveSettings[] receivers, NonDurableTransport transport, NonDurableBroker broker)
    {
        var localReceiveAddresses = receivers
            .Select(r => ToTransportAddress(r.ReceiveAddress))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        this.broker = broker;
        this.transport = transport;
        criticalErrorAction = hostSettings.CriticalErrorAction;

        Receivers = receivers.ToDictionary(
            r => r.Id,
            r => (IMessageReceiver)CreateReceiver(r));

        var pumpsByAddress = Receivers.Values
            .Cast<NonDurableMessagePump>()
            .ToDictionary(pump => pump.ReceiveAddress, pump => pump, StringComparer.OrdinalIgnoreCase);

        var inlineExecutionRunners = pumpsByAddress.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.Runner, StringComparer.OrdinalIgnoreCase);

        var dispatcher = new NonDurableDispatcher(broker, transport.InlineExecutionSettings, localReceiveAddresses, inlineExecutionRunners, pumpsByAddress);

        foreach (var runner in inlineExecutionRunners.Values)
        {
            runner.SetDispatcher(dispatcher);
        }

        Dispatcher = dispatcher;
    }

    NonDurableMessagePump CreateReceiver(ReceiveSettings receiveSettings)
    {
        var queueAddress = ToTransportAddress(receiveSettings.ReceiveAddress);

        ISubscriptionManager? subscriptionManager = receiveSettings.UsePublishSubscribe
            ? new NonDurableSubscriptionManager(broker, queueAddress)
            : null;

        var pump = new NonDurableMessagePump(
            receiveSettings.Id,
            queueAddress,
            receiveSettings,
            transport.TransportTransactionMode,
            criticalErrorAction,
            broker,
            transport.ShutdownBehavior);

        pump.ConfigureSubscriptionManager(subscriptionManager);

        return pump;
    }

    readonly NonDurableTransport transport;
    readonly NonDurableBroker broker;
    readonly Action<string, Exception, CancellationToken> criticalErrorAction;

    public override Task Shutdown(CancellationToken cancellationToken = default) =>
        Task.WhenAll(Receivers.Values.Select(r => r.StopReceive(cancellationToken)));

    public override string ToTransportAddress(QueueAddress queueAddress)
    {
        var discriminator = queueAddress.Discriminator;
        var qualifier = queueAddress.Qualifier;

        return (string.IsNullOrEmpty(discriminator), string.IsNullOrEmpty(qualifier)) switch
        {
            (false, false) => $"{queueAddress.BaseAddress}-{discriminator}-{qualifier}",
            (false, true) => $"{queueAddress.BaseAddress}-{discriminator}",
            (true, false) => $"{queueAddress.BaseAddress}-{qualifier}",
            _ => queueAddress.BaseAddress
        };
    }
}
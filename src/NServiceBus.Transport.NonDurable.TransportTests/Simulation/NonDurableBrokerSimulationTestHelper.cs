namespace NServiceBus.TransportTests.Simulation;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;

static class NonDurableBrokerSimulationTestHelper
{
    public static async Task<IMessageDispatcher> CreateDispatcher(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var infrastructure = await CreateInfrastructure(broker, cancellationToken);
        return infrastructure.Dispatcher;
    }

    public static async Task<IMessageReceiver> CreateReceiver(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var infrastructure = await CreateInfrastructure(broker, cancellationToken);
        return infrastructure.Receivers["main"];
    }

    public static Task<TransportInfrastructure> CreateInfrastructure(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var transport = new NonDurableTransport(new NonDurableTransportOptions(broker));
        return transport.Initialize(
            new HostSettings("endpoint", string.Empty, new StartupDiagnosticEntries(), static (_, _, _) => { }, true),
            [new ReceiveSettings("main", new QueueAddress("input"), false, true, "error")],
            ["error"],
            cancellationToken);
    }

    public static Task Dispatch(IMessageDispatcher dispatcher, string messageId, string destination, CancellationToken cancellationToken = default)
    {
        var message = new OutgoingMessage(messageId, [], new byte[] { 1 });
        var operation = new TransportOperation(message, new UnicastAddressTag(destination));
        return dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction(), cancellationToken);
    }

    public static BrokerEnvelope CreateEnvelope(string messageId, string destination, long sequenceNumber) => BrokerPayloadStore.Borrow(messageId, [1], new Dictionary<string, string>(), destination, false, sequenceNumber);
}
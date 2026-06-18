namespace NServiceBus.AcceptanceTests;

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

public partial class NServiceBusAcceptanceTest
{
    static readonly AsyncLocal<NonDurableBroker> currentBroker = new();
    static NonDurableBroker sharedBroker;

    public static NonDurableBroker CurrentBroker =>
        currentBroker.Value ?? sharedBroker ?? throw new InvalidOperationException("No NonDurableBroker available for the current test.");

    [SetUp]
    public void NonDurableTransportSetUp()
    {
        var broker = new NonDurableBroker();
        currentBroker.Value = broker;
        sharedBroker = broker;
    }

    [TearDown]
    public async Task NonDurableTransportTearDown()
    {
        if (currentBroker.Value is { } broker)
        {
            currentBroker.Value = null;
            if (ReferenceEquals(sharedBroker, broker))
            {
                sharedBroker = null;
            }
            await broker.DisposeAsync();
        }
    }
}
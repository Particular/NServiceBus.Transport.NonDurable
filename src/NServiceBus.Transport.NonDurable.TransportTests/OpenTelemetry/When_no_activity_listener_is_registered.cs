#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;
using static Simulation.NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_no_activity_listener_is_registered
{
    [Test]
    public async Task Should_dispatch_without_affecting_behavior()
    {
        // No TestingActivityListener is created, so ActivitySource.HasListeners() is false and
        // the transport takes the zero-allocation fast path. This asserts the listener-free path
        // does not change dispatch behavior or leave diagnostic state behind.
        await using var broker = new NonDurableBroker();
        var dispatcher = await CreateDispatcher(broker);

        await Dispatch(dispatcher, "msg-1", "queue");

        using (Assert.EnterMultipleScope())
        {
            Assert.That(broker.GetOrCreateQueue("queue").TryPeek(out var envelope), Is.True);
            Assert.That(envelope, Is.Not.Null);
            // No trace context should be injected when there are no listeners.
            Assert.That(envelope!.Headers.ContainsKey(Headers.DiagnosticsTraceParent), Is.False);
        }

        Assert.That(Activity.Current, Is.Null, "no ambient activity should be left after dispatch");
    }

    [Test]
    public async Task Should_preserve_ambient_activity_during_inline_receive()
    {
        await using var broker = new NonDurableBroker();
        var infrastructure = await InlineExecutionTestHelper.CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var childActivitySeenByHandler = new TaskCompletionSource<Activity?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Activity? ambientActivity = null;

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (messageContext, cancellationToken) =>
            {
                if (messageContext.Headers.TryGetValue("kind", out var kind) && kind == "parent")
                {
                    using var activity = new Activity("user-operation").Start();
                    ambientActivity = activity;
                    await dispatcher.Dispatch(
                        new TransportOperations(InlineExecutionTestHelper.CreateUnicast("input", headers: new Dictionary<string, string>
                        {
                            [Headers.MessageIntent] = MessageIntent.Send.ToString(),
                            ["kind"] = "child"
                        })),
                        messageContext.TransportTransaction,
                        cancellationToken);
                }
                else
                {
                    childActivitySeenByHandler.TrySetResult(Activity.Current);
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await receiver.StartReceive();
        await dispatcher.Dispatch(
            new TransportOperations(InlineExecutionTestHelper.CreateUnicast("input", headers: new Dictionary<string, string>
            {
                [Headers.MessageIntent] = MessageIntent.Send.ToString(),
                ["kind"] = "parent"
            })),
            new TransportTransaction());

        var childActivity = await childActivitySeenByHandler.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        Assert.That(childActivity, Is.SameAs(ambientActivity));
    }
}

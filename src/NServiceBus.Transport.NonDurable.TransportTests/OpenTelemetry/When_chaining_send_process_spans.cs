#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;
using static Simulation.NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_chaining_send_process_spans
{
    [Test]
    public async Task Should_keep_process_spans_as_roots_instead_of_deepening_the_chain()
    {
        const int hops = 3;
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName, "NServiceBus.Core");
        var dispatcher = await CreateDispatcher(broker);
        var receiver = await CreateReceiver(broker);
        var handledCount = 0;
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (context, cancellationToken) =>
            {
                var hop = Interlocked.Increment(ref handledCount);
                if (hop < hops)
                {
                    var message = new OutgoingMessage($"msg-{hop + 1}", [], new byte[] { 1 });
                    await dispatcher.Dispatch(
                        new TransportOperations(new TransportOperation(message, new UnicastAddressTag("input"))),
                        context.TransportTransaction,
                        cancellationToken);
                }
                else
                {
                    allHandled.TrySetResult();
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await receiver.StartReceive();
        await Dispatch(dispatcher, "msg-1", "input");
        await allHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var transportActivities = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName);
        var sendActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.SendActivityName).ToList();
        var processActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName).ToList();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(processActivities, Has.Count.EqualTo(hops));
            Assert.That(sendActivities, Has.Count.EqualTo(hops));

            // Every process span is a root span: the send -> process parent chain is broken.
            Assert.That(processActivities.All(process => process.ParentId is null), Is.True, "process spans should be root spans");

            // Every process span links back to exactly one send span.
            foreach (var process in processActivities)
            {
                Assert.That(process.Links.Count(), Is.EqualTo(1), "process span should carry exactly one link");
                Assert.That(sendActivities.Any(send => send.SpanId == process.Links.Single().Context.SpanId), Is.True, "process span should link to a send span");
            }

            // No process span is parented to any send span.
            Assert.That(processActivities.All(process => sendActivities.All(send => send.Id != process.ParentId)), Is.True);

            // The trace depth must not grow linearly with the number of hops. With the parent-based
            // chain each hop added two levels of nesting; with link-based propagation the depth stays
            // bounded by a single message's processing (process -> process message -> handler -> send).
            var maxDepth = ComputeMaxDepth(listener.CompletedActivities.ToList());
            Assert.That(maxDepth, Is.LessThan(hops * 2), "trace depth should not grow linearly with the number of hops");
        }
    }

    static int ComputeMaxDepth(IReadOnlyList<Activity> activities)
    {
        var byId = activities.Where(activity => activity.Id is not null).ToDictionary(activity => activity.Id!);
        var depths = new Dictionary<string, int>();
        var maxDepth = 0;

        foreach (var activity in activities)
        {
            maxDepth = Math.Max(maxDepth, ComputeDepth(activity, byId, depths));
        }

        return maxDepth;
    }

    static int ComputeDepth(Activity activity, IReadOnlyDictionary<string, Activity> byId, Dictionary<string, int> depths)
    {
        if (activity.Id is not null && depths.TryGetValue(activity.Id, out var cached))
        {
            return cached;
        }

        var depth = 0;
        if (activity.ParentId is not null && byId.TryGetValue(activity.ParentId, out var parent))
        {
            depth = 1 + ComputeDepth(parent, byId, depths);
        }

        if (activity.Id is not null)
        {
            depths[activity.Id] = depth;
        }

        return depth;
    }
}

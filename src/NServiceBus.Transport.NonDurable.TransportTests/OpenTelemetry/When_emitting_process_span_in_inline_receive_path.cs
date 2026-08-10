#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;

[TestFixture]
public class When_emitting_process_span_in_inline_receive_path
{
    [Test]
    public async Task Should_create_root_process_spans_in_the_inline_receive_path()
    {
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var childProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (messageContext, cancellationToken) =>
            {
                if (messageContext.Headers.TryGetValue("kind", out var kind) && kind == "parent")
                {
                    // Reentrant inline dispatch: the child message is processed inline while
                    // Activity.Current is the parent handler's activity. The process span must
                    // still be a root span, not parented to the ambient activity.
                    await dispatcher.Dispatch(
                        new TransportOperations(CreateUnicast("input", headers: new Dictionary<string, string>
                        {
                            [Headers.MessageIntent] = MessageIntent.Send.ToString(),
                            ["kind"] = "child"
                        })),
                        messageContext.TransportTransaction,
                        cancellationToken);
                }
                else
                {
                    childProcessed.TrySetResult();
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await receiver.StartReceive();

        var rootDispatch = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", headers: new Dictionary<string, string>
            {
                [Headers.MessageIntent] = MessageIntent.Send.ToString(),
                ["kind"] = "parent"
            })),
            new TransportTransaction());

        await childProcessed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await rootDispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var transportActivities = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName);
        var sendActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.SendActivityName).ToList();
        var processActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName).ToList();

        using (Assert.EnterMultipleScope())
        {
            // Both the root inline dispatch and the reentrant inline dispatch create a send span.
            Assert.That(sendActivities, Has.Count.EqualTo(2));
            Assert.That(processActivities, Has.Count.EqualTo(2));

            // Every process span is a root span even in the inline execution path.
            Assert.That(processActivities.All(process => process.ParentId is null), Is.True, "process spans should be root spans in the inline execution path");

            // Each process span links back to its corresponding send span.
            foreach (var process in processActivities)
            {
                Assert.That(process.Links.Count(), Is.EqualTo(1), "process span should carry exactly one link");
                Assert.That(sendActivities.Any(send => send.SpanId == process.Links.Single().Context.SpanId), Is.True, "process span should link to a send span");
            }
        }
    }
}

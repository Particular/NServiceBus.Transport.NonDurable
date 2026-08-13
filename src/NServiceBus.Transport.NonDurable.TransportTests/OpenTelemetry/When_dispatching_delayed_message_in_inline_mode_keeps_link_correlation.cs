#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_delayed_message_in_inline_mode_keeps_link_correlation
{
    [Test]
    public async Task Should_keep_process_span_as_root_with_link_for_delayed_delivery()
    {
        var simulatedTime = CreateFakeTimeProvider();
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions { TimeProvider = simulatedTime });
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) =>
            {
                processed.TrySetResult();
                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await receiver.StartReceive();

        // Delayed delivery is asynchronous even with inline execution enabled: the message is
        // processed by the pump after the delivery time, outside the send's operation.
        var dispatch = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", delay: TimeSpan.FromSeconds(5))),
            new TransportTransaction());

        simulatedTime.Advance(TimeSpan.FromSeconds(5));
        await dispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var transportActivities = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName);
        var producerActivities = transportActivities
            .Where(activity => activity.OperationName == NonDurableTransportTracing.SendActivityName)
            .Concat(transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.ScheduleActivityName))
            .ToList();
        var processActivities = transportActivities.Where(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName).ToList();

        using (Assert.EnterMultipleScope())
        {
            var scheduleActivity = producerActivities.Single();
            var processActivity = processActivities.Single();

            Assert.That(scheduleActivity.OperationName, Is.EqualTo(NonDurableTransportTracing.ScheduleActivityName));

            // Delayed delivery keeps the process span as a root span linked to the schedule span.
            Assert.That(processActivity.ParentId, Is.Null, "delayed delivery should keep the process span as a root span");
            Assert.That(processActivity.Links.Count(), Is.EqualTo(1), "process span should carry exactly one link");
            Assert.That(processActivity.Links.Single().Context.SpanId, Is.EqualTo(scheduleActivity.SpanId), "process span should link to the schedule span");
        }
    }
}

#nullable enable

namespace NServiceBus.TransportTests.OpenTelemetry;

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NServiceBus.Transport.NonDurable.Tests;
using NUnit.Framework;
using static Simulation.NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_recording_exceptions_on_process_span
{
    [Test]
    public async Task Should_not_record_exception_event_when_core_already_recorded_it()
    {
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var dispatcher = await CreateDispatcher(broker);
        var receiver = await CreateReceiver(broker);
        var errorHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Simulates NServiceBus.Core's OpenTelemetry instrumentation having already recorded the
        // exception: Core marks the instance with the "otel.exception.recorded" key in
        // Exception.Data once it captured it in a span event or log (see Particular/NServiceBus#7911).
        var processingException = new InvalidOperationException("handler boom") { Data = { ["otel.exception.recorded"] = true } };

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) => throw processingException,
            (_, _) =>
            {
                errorHandled.TrySetResult();
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await receiver.StartReceive();
        await Dispatch(dispatcher, "msg-5", "input");
        await errorHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var processActivity = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName)
            .Single(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(processActivity.Status, Is.EqualTo(ActivityStatusCode.Error));
            Assert.That(processActivity.GetTagItem("error.type"), Is.EqualTo(typeof(InvalidOperationException).FullName));
            Assert.That(processActivity.Events.Any(activityEvent => activityEvent.Name == "exception"), Is.False, "the exception event must not be duplicated on the transport span when Core already recorded it");
        }
    }

    [Test]
    public async Task Should_record_exception_event_when_core_did_not_record_it()
    {
        await using var broker = new NonDurableBroker();
        using var listener = new TestingActivityListener(NonDurableTransportTracing.ActivitySourceName);
        var dispatcher = await CreateDispatcher(broker);
        var receiver = await CreateReceiver(broker);
        var errorHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) => throw new InvalidOperationException("handler boom"),
            (_, _) =>
            {
                errorHandled.TrySetResult();
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await receiver.StartReceive();
        await Dispatch(dispatcher, "msg-6", "input");
        await errorHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        var processActivity = listener.CompletedFrom(NonDurableTransportTracing.ActivitySourceName)
            .Single(activity => activity.OperationName == NonDurableTransportTracing.ProcessActivityName);

        var exceptionEvents = processActivity.Events.Where(activityEvent => activityEvent.Name == "exception").ToList();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(processActivity.Status, Is.EqualTo(ActivityStatusCode.Error));
            Assert.That(processActivity.GetTagItem("error.type"), Is.EqualTo(typeof(InvalidOperationException).FullName));
            Assert.That(exceptionEvents, Has.Count.EqualTo(1));
            Assert.That(exceptionEvents.Single().Tags.Single(tag => tag.Key == "exception.type").Value, Is.EqualTo(typeof(InvalidOperationException).FullName));
        }
    }
}
#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_receiving_shutdown_with_ShutdownAfterHandlerExit_behavior_with_concurrency_2
{
    [Test]
    public async Task Should_let_in_flight_handlers_finish_and_keep_buffered_messages_queued()
    {
        await using var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(
            broker,
            ["input"],
            shutdownBehavior: NonDurableTransportShutdownBehavior.ShutdownAfterHandlerExit);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var bothInFlightStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startedCount = 0;
        var completedCount = 0;

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 2),
            async (_, cancellationToken) =>
            {
                var started = Interlocked.Increment(ref startedCount);
                if (started == 2)
                {
                    bothInFlightStarted.TrySetResult();
                }

                // The first two handlers (the in-flight ones) block until released. After restart, the buffered
                // handlers observe an already-completed release task and run straight through.
                await releaseInFlight.Task.WaitAsync(cancellationToken);

                var completed = Interlocked.Increment(ref completedCount);
                if (completed == 4)
                {
                    allHandled.TrySetResult();
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        await receiver.StartReceive();

        var firstDispatch = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());
        var secondDispatch = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());
        var thirdDispatch = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());
        var fourthDispatch = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());

        await bothInFlightStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var stopTask = receiver.StopReceive();

        // Stop cannot complete while two handlers are in flight (graceful stop, no cancellation).
        Assert.That(stopTask.IsCompleted, Is.False);

        releaseInFlight.TrySetResult();

        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));
        await firstDispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await secondDispatch.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(completedCount, Is.EqualTo(2));
            Assert.That(thirdDispatch.IsCompleted, Is.False);
            Assert.That(fourthDispatch.IsCompleted, Is.False);
            Assert.That(broker.GetOrCreateQueue("input").Count, Is.EqualTo(2));
        }

        // Restart drains the two buffered messages that ShutdownAfterHandlerExit left in the queue.
        await receiver.StartReceive();

        await allHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await thirdDispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await fourthDispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(completedCount, Is.EqualTo(4));
            Assert.That(broker.GetOrCreateQueue("input").Count, Is.EqualTo(0));
        }
    }
}
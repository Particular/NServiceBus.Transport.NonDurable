#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.Transport;
using NUnit.Framework;
using Simulation;

[TestFixture]
public class When_dispatching_inline_delayed_root_send_with_delayed_delivery_reject
{
    [Test]
    public async Task Should_keep_root_scope_pending_until_eventual_receive()
    {
        var fakeTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        var limiter = new ScriptedRateLimiter(
        [
            ScriptedRateLimiterStep.Rejected(TimeSpan.FromSeconds(30)),
            ScriptedRateLimiterStep.Acquired()
        ]);

        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            TimeProvider = fakeTime,
            DelayedDelivery = { Mode = NonDurableSimulationMode.Reject, RateLimiter = limiter }
        });

        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowHandlerToComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await allowHandlerToComplete.Task;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        await receiver.StartReceive();

        var rootTask = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", delay: TimeSpan.Zero)),
            new TransportTransaction());

        Assert.That(rootTask.IsCompleted, Is.False);

        // The pump dequeues immediately (zero delay), the DelayedDelivery simulation rejects, and the
        // envelope is re-enqueued for now + RetryAfter. The pump's retry timer is driven by the fake time
        // provider, so advance simulated time past RetryAfter until delivery lands and the handler starts.
        for (var attempt = 0; attempt < 10 && !handlerStarted.Task.IsCompleted; attempt++)
        {
            fakeTime.Advance(TimeSpan.FromSeconds(30));
            try
            {
                await handlerStarted.Task.WaitAsync(TimeSpan.FromMilliseconds(250));
            }
            catch (TimeoutException timeout) when (!handlerStarted.Task.IsCompleted)
            {
                Assert.That(timeout, Is.Not.Null);
            }
        }

        Assert.That(handlerStarted.Task.IsCompleted, Is.True);

        Assert.That(rootTask.IsCompleted, Is.False);

        allowHandlerToComplete.TrySetResult();
        await rootTask.WaitAsync(TimeSpan.FromSeconds(5));

        await receiver.StopReceive();
    }
}

#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_inline_root_send_local_with_send_delay
{
    [Test]
    public async Task Should_not_complete_until_simulated_time_advances_and_handler_finishes()
    {
        var fakeTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            TimeProvider = fakeTime,
            Send = { RateLimit = new NonDurableRateLimitOptions { PermitLimit = 1, Window = TimeSpan.FromSeconds(30) } }
        });

        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];

        var secondHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSecondHandlerToComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (messageContext, _) =>
            {
                if (messageContext.Headers.TryGetValue("seq", out var seq) && seq == "second")
                {
                    secondHandlerStarted.TrySetResult();
                    await allowSecondHandlerToComplete.Task;
                }
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        await receiver.StartReceive();

        // The first inline root send consumes the single send permit (its handler returns immediately),
        // so the second send below is the one subject to the window-based send delay.
        var first = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", headers: new Dictionary<string, string> { ["seq"] = "first" })),
            new TransportTransaction());
        await first.WaitAsync(TimeSpan.FromSeconds(5));

        var second = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", headers: new Dictionary<string, string> { ["seq"] = "second" })),
            new TransportTransaction());

        // Send simulation delay: the root task must still be pending because the broker has not enqueued yet.
        Assert.That(second.IsCompleted, Is.False);

        fakeTime.Advance(TimeSpan.FromSeconds(30));

        // After the send delay elapses the message is enqueued and the handler starts, but the root
        // task must remain pending until the handler completes.
        await secondHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(second.IsCompleted, Is.False);

        allowSecondHandlerToComplete.TrySetResult();
        await second.WaitAsync(TimeSpan.FromSeconds(5));

        await receiver.StopReceive();
    }
}
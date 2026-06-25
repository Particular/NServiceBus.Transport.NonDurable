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
public class When_dispatching_inline_root_with_staged_send_flush_under_send_simulation
{
    [Test]
    public async Task Should_not_complete_parent_root_until_flushed_send_simulation_advances_and_child_finishes()
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

        var parentHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var childStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowChildToComplete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 2),
            async (messageContext, cancellationToken) =>
            {
                if (messageContext.Headers.TryGetValue("seq", out var seq) && seq == "child")
                {
                    childStarted.TrySetResult();
                    await allowChildToComplete.Task;
                    return;
                }

                // Parent handler: emit a delayed local send with Default consistency so it is staged
                // (enlisted) into the receive transaction rather than dispatched inline.
                parentHandled.TrySetResult();
                var staged = dispatcher.Dispatch(
                    new TransportOperations(CreateUnicast("input", delay: TimeSpan.FromMinutes(1), consistency: DispatchConsistency.Default, headers: new Dictionary<string, string> { ["seq"] = "child" })),
                    messageContext.TransportTransaction,
                    cancellationToken);
                Assert.That(staged.IsCompletedSuccessfully, Is.True);
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        await receiver.StartReceive();

        // The parent root send consumes the single send permit; the flushed staged send is the one delayed.
        var parentRoot = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("input", headers: new Dictionary<string, string> { ["seq"] = "parent" })),
            new TransportTransaction());

        await parentHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The staged send has been flushed and is now waiting on the send-simulation delay, so the parent
        // root scope (whose completion is awaited by the caller) must still be pending.
        Assert.That(parentRoot.IsCompleted, Is.False);

        fakeTime.Advance(TimeSpan.FromSeconds(30));

        await childStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(parentRoot.IsCompleted, Is.False);

        allowChildToComplete.TrySetResult();
        await parentRoot.WaitAsync(TimeSpan.FromSeconds(5));

        await receiver.StopReceive();
    }
}
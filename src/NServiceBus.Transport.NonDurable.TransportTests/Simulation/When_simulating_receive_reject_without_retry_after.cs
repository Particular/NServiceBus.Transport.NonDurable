namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;
using static NonDurableBrokerSimulationTestHelper;

[TestFixture]
public class When_simulating_receive_reject_with_direct_rate_limiter_without_retry_after
{
    [Test]
    public async Task Should_keep_message_unprocessed_until_limiter_grants()
    {
        await using var limiter = new ManualGrantRateLimiter();
        await using var broker = new NonDurableBroker(new NonDurableBrokerOptions
        {
            Receive =
            {
                Mode = NonDurableSimulationMode.Reject,
                RateLimiter = limiter
            }
        });

        var receiver = await CreateReceiver(broker);
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receivedCount = 0;

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) =>
            {
                var current = Interlocked.Increment(ref receivedCount);
                if (current == 1)
                {
                    firstReceived.TrySetResult();
                }
                else if (current == 2)
                {
                    secondReceived.TrySetResult();
                }

                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        var queue = broker.GetOrCreateQueue("input");
        await queue.Enqueue(CreateEnvelope("msg-1", "input", 1), CancellationToken.None);
        await queue.Enqueue(CreateEnvelope("msg-2", "input", 2), CancellationToken.None);
        await receiver.StartReceive();

        // While the limiter rejects, the pump must keep retrying without processing either message.
        await AsyncSpinWait.Until(() => limiter.Attempts >= 5, maxIterations: 100);
        using (Assert.EnterMultipleScope())
        {
            Assert.That(Volatile.Read(ref receivedCount), Is.EqualTo(0));
            Assert.That(firstReceived.Task.IsCompleted, Is.False);
        }

        limiter.StartGranting();

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await secondReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(Volatile.Read(ref receivedCount), Is.EqualTo(2));

        await receiver.StopReceive();
    }
}
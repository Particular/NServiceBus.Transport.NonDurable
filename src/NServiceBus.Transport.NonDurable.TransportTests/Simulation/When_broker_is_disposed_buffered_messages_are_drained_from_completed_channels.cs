#nullable enable

namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_broker_is_disposed_buffered_messages_are_drained_from_completed_channels
{
    [Test]
    public async Task Run()
    {
        var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var receiver = infrastructure.Receivers["receiver-0"];
        var handled = 0;
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) =>
            {
                if (Interlocked.Increment(ref handled) == 2)
                {
                    allHandled.TrySetResult();
                }

                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        var queue = broker.GetOrCreateQueue("input");
        await queue.Enqueue(CreateReceivedEnvelope("input"));
        await queue.Enqueue(CreateReceivedEnvelope("input"));

        await receiver.StartReceive();
        await broker.DisposeAsync();

        await allHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await receiver.StopReceive().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(handled, Is.EqualTo(2));
    }
}

#nullable enable

namespace NServiceBus.TransportTests;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_receiving_with_high_concurrency_and_channel_completion
{
    [Test]
    public async Task Should_process_all_buffered_messages_exactly_once_when_channel_is_completed()
    {
        var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var receiver = infrastructure.Receivers["receiver-0"];
        var handled = 0;
        var allHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 100),
            (_, _) =>
            {
                if (Interlocked.Increment(ref handled) == 5)
                {
                    allHandled.TrySetResult();
                }

                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        var queue = broker.GetOrCreateQueue("input");
        for (var i = 0; i < 5; i++)
        {
            await queue.Enqueue(CreateReceivedEnvelope("input"));
        }

        await receiver.StartReceive();

        await allHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await broker.DisposeAsync();

        await receiver.StopReceive().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.That(handled, Is.EqualTo(5));
    }

    [Test]
    public async Task Should_exit_all_pump_tasks_cleanly_when_empty_channel_is_completed()
    {
        var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var receiver = infrastructure.Receivers["receiver-0"];

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 100),
            (_, _) => Task.CompletedTask,
            (_, _) => Task.FromResult(ErrorHandleResult.Handled),
            CancellationToken.None);

        await receiver.StartReceive();

        // Give the pump tasks time to park in ReadAsync on the empty channel before completing it.
        await Task.Delay(100);

        await broker.DisposeAsync();

        await receiver.StopReceive().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
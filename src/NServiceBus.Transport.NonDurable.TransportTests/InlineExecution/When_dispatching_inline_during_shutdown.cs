#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class When_dispatching_inline_during_shutdown
{
    [Test]
    public async Task ShutdownAfterHandlerExit_should_reject_child_and_requeue_parent()
    {
        await using var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(
            broker,
            ["parent", "child"],
            shutdownBehavior: NonDurableTransportShutdownBehavior.ShutdownAfterHandlerExit);
        var dispatcher = infrastructure.Dispatcher;
        var parentReceiver = infrastructure.Receivers["receiver-0"];
        var childReceiver = infrastructure.Receivers["receiver-1"];
        var parentStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseParent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var recoverabilityInvoked = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var childHandled = 0;

        await parentReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (context, cancellationToken) =>
            {
                parentStarted.TrySetResult();
                await releaseParent.Task;
                await dispatcher.Dispatch(
                    new TransportOperations(CreateUnicast("child")),
                    context.TransportTransaction,
                    cancellationToken);
            },
            (context, _) =>
            {
                recoverabilityInvoked.TrySetResult(context.Exception);
                return Task.FromResult(ErrorHandleResult.RetryRequired);
            });

        await childReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) =>
            {
                Interlocked.Increment(ref childHandled);
                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await parentReceiver.StartReceive();
        await childReceiver.StartReceive();

        var rootDispatch = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("parent")),
            new TransportTransaction());

        await parentStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await childReceiver.StopReceive().WaitAsync(TimeSpan.FromSeconds(5));

        var parentStop = parentReceiver.StopReceive();
        releaseParent.TrySetResult();

        var recoverabilityException = await recoverabilityInvoked.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await parentStop.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(recoverabilityException, Is.InstanceOf<OperationCanceledException>());
            Assert.That(childHandled, Is.Zero);
            Assert.That(broker.GetOrCreateQueue("child").Count, Is.Zero);
            Assert.That(broker.GetOrCreateQueue("parent").Count, Is.EqualTo(1));
            Assert.That(rootDispatch.IsCompleted, Is.False);
        }

        using var hardStop = new CancellationTokenSource();
        hardStop.Cancel();
        await parentReceiver.StopReceive(hardStop.Token).WaitAsync(TimeSpan.FromSeconds(5));
        _ = await CatchException(rootDispatch);
    }

    [Test]
    public async Task DrainQueueBeforeShutdown_should_allow_in_flight_inline_cascade_to_finish()
    {
        await using var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(broker, ["parent", "child"]);
        var dispatcher = infrastructure.Dispatcher;
        var parentReceiver = infrastructure.Receivers["receiver-0"];
        var childReceiver = infrastructure.Receivers["receiver-1"];
        var parentStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseParent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var childHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await parentReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (context, cancellationToken) =>
            {
                parentStarted.TrySetResult();
                await releaseParent.Task;
                await dispatcher.Dispatch(
                    new TransportOperations(CreateUnicast("child")),
                    context.TransportTransaction,
                    cancellationToken);
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await childReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) =>
            {
                childHandled.TrySetResult();
                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await parentReceiver.StartReceive();
        await childReceiver.StartReceive();

        var rootDispatch = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("parent")),
            new TransportTransaction());

        await parentStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await childReceiver.StopReceive().WaitAsync(TimeSpan.FromSeconds(5));

        var parentStop = parentReceiver.StopReceive();
        releaseParent.TrySetResult();

        await childHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await rootDispatch.WaitAsync(TimeSpan.FromSeconds(5));
        await parentStop.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(broker.GetOrCreateQueue("parent").Count, Is.Zero);
            Assert.That(broker.GetOrCreateQueue("child").Count, Is.Zero);
        }
    }

    [Test]
    public async Task Hard_stop_should_cancel_inline_child_admitted_before_shutdown()
    {
        await using var broker = new NonDurableBroker();
        var infrastructure = await CreateInfrastructure(
            broker,
            ["parent", "child"],
            shutdownBehavior: NonDurableTransportShutdownBehavior.ShutdownAfterHandlerExit);
        var dispatcher = infrastructure.Dispatcher;
        var parentReceiver = infrastructure.Receivers["receiver-0"];
        var childReceiver = infrastructure.Receivers["receiver-1"];
        var childStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await parentReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (context, cancellationToken) => dispatcher.Dispatch(
                new TransportOperations(CreateUnicast("child")),
                context.TransportTransaction,
                cancellationToken),
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await childReceiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            async (_, cancellationToken) =>
            {
                childStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await parentReceiver.StartReceive();
        await childReceiver.StartReceive();

        var rootDispatch = dispatcher.Dispatch(
            new TransportOperations(CreateUnicast("parent")),
            new TransportTransaction());

        await childStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using var hardStop = new CancellationTokenSource();
        var parentStop = parentReceiver.StopReceive(hardStop.Token);
        var childStop = childReceiver.StopReceive(hardStop.Token);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(parentStop.IsCompleted, Is.False);
            Assert.That(childStop.IsCompleted, Is.False);
        }

        hardStop.Cancel();

        await Task.WhenAll(parentStop, childStop).WaitAsync(TimeSpan.FromSeconds(5));
        var rootException = await CatchException(rootDispatch);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(rootException, Is.InstanceOf<OperationCanceledException>());
            Assert.That(broker.GetOrCreateQueue("parent").Count, Is.EqualTo(1));
            Assert.That(broker.GetOrCreateQueue("child").Count, Is.Zero);
        }
    }
}

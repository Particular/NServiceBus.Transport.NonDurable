#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NUnit.Framework;
using Simulation;

[TestFixture]
public class When_receiving_move_to_error_under_send_delay
{
    [Test]
    public async Task Should_wait_for_error_send_simulation_then_fault_with_original_exception()
    {
        await using var sendLimiter = new BlockingGrantRateLimiter();
        var options = new NonDurableBrokerOptions();
        // Send simulation only on the error queue so the root send to "input" is unaffected.
        options.ForQueue("error").Send.Mode = NonDurableSimulationMode.Delay;
        options.ForQueue("error").Send.RateLimiter = sendLimiter;

        await using var broker = new NonDurableBroker(options);

        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;
        var receiver = infrastructure.Receivers["receiver-0"];
        var originalException = new System.InvalidOperationException("boom");

        await receiver.Initialize(
            new PushRuntimeSettings(maxConcurrency: 1),
            (_, _) => throw originalException,
            async (errorContext, cancellationToken) =>
            {
                // Dispatch the error-queue move with Default consistency so it enlists into the
                // errorCommittable transaction and is flushed after commit through the broker send path.
                var message = new OutgoingMessage(errorContext.MessageId, new Dictionary<string, string>(errorContext.Headers), errorContext.Body);
                var operation = new TransportOperation(message, new UnicastAddressTag("error"), [], DispatchConsistency.Default);
                await dispatcher.Dispatch(new TransportOperations(operation), errorContext.TransportTransaction, cancellationToken);
                return ErrorHandleResult.Handled;
            },
            CancellationToken.None);

        await receiver.StartReceive();

        var rootTask = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());

        // The error-queue move flush reaches the broker send simulation and blocks there, so the root
        // scope stays pending while the move is delayed.
        await sendLimiter.Acquired.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(rootTask.IsCompleted, Is.False);

        // Releasing the send simulation lets the move through; the root task then faults with the
        // original handler exception (the simulation delay is not surfaced to the caller).
        sendLimiter.Grant();

        var faulted = await CatchException(rootTask);
        using (Assert.EnterMultipleScope())
        {
            Assert.That(faulted, Is.SameAs(originalException));
            Assert.That(broker.GetOrCreateQueue("error").Count, Is.EqualTo(1));
        }

        await receiver.StopReceive();
    }
}
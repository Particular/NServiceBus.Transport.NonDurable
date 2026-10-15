#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus.Transport;
using NUnit.Framework;

// Changes the process-wide TransactionManager.DefaultTimeout, so it must not run alongside other tests.
[TestFixture]
[NonParallelizable]
public class When_handler_runs_longer_than_the_default_transaction_timeout
{
    [SetUp]
    public void SetUp()
    {
        originalDefaultTimeout = TransactionManager.DefaultTimeout;
        TransactionManager.DefaultTimeout = ShortDefaultTimeout;
    }

    [TearDown]
    public void TearDown() => TransactionManager.DefaultTimeout = originalDefaultTimeout;

    [Test]
    public async Task Should_commit_the_receive_transaction_and_dispatch_enlisted_sends()
    {
        await using var broker = new NonDurableBroker();
        var dispatcher = new CapturingDispatcher();
        Exception? failure = null;
        var runner = new InlineExecutionRunner(
            "input",
            TransportTransactionMode.SendsAtomicWithReceive,
            static (_, _, _) => { },
            broker,
            static () => CancellationToken.None);

        runner.SetDispatcher(dispatcher);
        runner.Initialize(
            async (messageContext, cancellationToken) =>
            {
                messageContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(CreateReceivedEnvelope("destination"));
                await Task.Delay(HandlerDuration, cancellationToken);
            },
            (errorContext, _) =>
            {
                failure = errorContext.Exception;
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await runner.Process(CreateReceivedEnvelope("input"));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(failure, Is.Null, "the receive transaction should not time out while the handler is still running");
            Assert.That(dispatcher.DispatchedOperations, Is.EqualTo(1), "the send enlisted by the handler should be dispatched on commit");
        }
    }

    // Stands in for the 60 second default so the test doesn't take a minute. System.Transactions checks
    // timeouts on a timer with ~512ms resolution, so a transaction using this timeout is aborted within ~1.5s.
    static readonly TimeSpan ShortDefaultTimeout = TimeSpan.FromMilliseconds(100);
    static readonly TimeSpan HandlerDuration = TimeSpan.FromSeconds(2.5);

    TimeSpan originalDefaultTimeout;

    sealed class CapturingDispatcher : IMessageDispatcher
    {
        public int DispatchedOperations { get; private set; }

        public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, CancellationToken cancellationToken = default)
        {
            DispatchedOperations += outgoingMessages.UnicastTransportOperations.Count;
            return Task.CompletedTask;
        }
    }
}
#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus.Transport;
using NUnit.Framework;

// Some tests change the process-wide TransactionManager.DefaultTimeout, so this fixture must not run alongside other tests.
[TestFixture]
[NonParallelizable]
public class When_configuring_transaction_timeout
{
    [SetUp]
    public void SetUp() => originalDefaultTimeout = TransactionManager.DefaultTimeout;

    [TearDown]
    public void TearDown() => TransactionManager.DefaultTimeout = originalDefaultTimeout;

    [Test]
    public async Task Should_abort_the_receive_transaction_after_the_configured_timeout()
    {
        await using var broker = new NonDurableBroker();
        var (infrastructure, receiver) = await CreateReceiver(broker, ShortTimeout, CancellationToken.None);
        Exception? failure = null;

        await receiver.Initialize(
            new PushRuntimeSettings(1),
            async (messageContext, cancellationToken) =>
            {
                await SendToDestination(infrastructure, messageContext.TransportTransaction, cancellationToken);
                await WaitUntilAborted(GetTransaction(messageContext.TransportTransaction), cancellationToken);
            },
            (errorContext, _) =>
            {
                failure = errorContext.Exception;
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await receiver.Runner.Process(CreateReceivedEnvelope(InputQueue));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(failure, Is.TypeOf<TransactionAbortedException>());
            Assert.That(broker.DrainQueue(DestinationQueue), Is.Zero, "the send enlisted in the aborted transaction should be discarded");
        }
    }

    [Test]
    public async Task Should_commit_the_receive_transaction_when_handler_outlives_the_default_timeout()
    {
        TransactionManager.DefaultTimeout = ShortTimeout;

        await using var broker = new NonDurableBroker();
        var (infrastructure, receiver) = await CreateReceiver(broker, TimeSpan.FromMinutes(1), CancellationToken.None);
        Exception? failure = null;
        TransactionStatus? statusAfterDefaultTimeout = null;

        await receiver.Initialize(
            new PushRuntimeSettings(1),
            async (messageContext, cancellationToken) =>
            {
                await SendToDestination(infrastructure, messageContext.TransportTransaction, cancellationToken);
                statusAfterDefaultTimeout = await GetStatusAfterDefaultTimeout(GetTransaction(messageContext.TransportTransaction), cancellationToken);
            },
            (errorContext, _) =>
            {
                failure = errorContext.Exception;
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await receiver.Runner.Process(CreateReceivedEnvelope(InputQueue));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(statusAfterDefaultTimeout, Is.EqualTo(TransactionStatus.Active));
            Assert.That(failure, Is.Null);
            Assert.That(broker.DrainQueue(DestinationQueue), Is.EqualTo(1));
        }
    }

    [Test]
    public async Task Should_use_the_default_timeout_when_not_configured()
    {
        TransactionManager.DefaultTimeout = ShortTimeout;

        await using var broker = new NonDurableBroker();
        var (infrastructure, receiver) = await CreateReceiver(broker, transactionTimeout: null, CancellationToken.None);
        Exception? failure = null;

        await receiver.Initialize(
            new PushRuntimeSettings(1),
            async (messageContext, cancellationToken) =>
            {
                await SendToDestination(infrastructure, messageContext.TransportTransaction, cancellationToken);
                await WaitUntilAborted(GetTransaction(messageContext.TransportTransaction), cancellationToken);
            },
            (errorContext, _) =>
            {
                failure = errorContext.Exception;
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await receiver.Runner.Process(CreateReceivedEnvelope(InputQueue));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(failure, Is.TypeOf<TransactionAbortedException>());
            Assert.That(broker.DrainQueue(DestinationQueue), Is.Zero);
        }
    }

    [Test]
    public async Task Should_abort_the_recoverability_transaction_after_the_configured_timeout()
    {
        await using var broker = new NonDurableBroker();
        var (infrastructure, receiver) = await CreateReceiver(broker, ShortTimeout, CancellationToken.None);

        await receiver.Initialize(
            new PushRuntimeSettings(1),
            static (_, _) => throw new InvalidOperationException("Handler failure"),
            async (errorContext, cancellationToken) =>
            {
                await SendToDestination(infrastructure, errorContext.TransportTransaction, cancellationToken);
                await WaitUntilAborted(GetTransaction(errorContext.TransportTransaction), cancellationToken);
                return ErrorHandleResult.Handled;
            });

        var exception = await CatchException(receiver.Runner.Process(CreateReceivedEnvelope(InputQueue)));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(exception, Is.TypeOf<TransactionAbortedException>());
            Assert.That(broker.DrainQueue(DestinationQueue), Is.Zero, "the send enlisted in the aborted recoverability transaction should be discarded");
        }
    }

    [Test]
    public async Task Should_commit_the_recoverability_transaction_when_recoverability_outlives_the_default_timeout()
    {
        TransactionManager.DefaultTimeout = ShortTimeout;

        await using var broker = new NonDurableBroker();
        var (infrastructure, receiver) = await CreateReceiver(broker, TimeSpan.FromMinutes(1), CancellationToken.None);
        TransactionStatus? statusAfterDefaultTimeout = null;

        await receiver.Initialize(
            new PushRuntimeSettings(1),
            static (_, _) => throw new InvalidOperationException("Handler failure"),
            async (errorContext, cancellationToken) =>
            {
                await SendToDestination(infrastructure, errorContext.TransportTransaction, cancellationToken);
                statusAfterDefaultTimeout = await GetStatusAfterDefaultTimeout(GetTransaction(errorContext.TransportTransaction), cancellationToken);
                return ErrorHandleResult.Handled;
            });

        await receiver.Runner.Process(CreateReceivedEnvelope(InputQueue));

        using (Assert.EnterMultipleScope())
        {
            Assert.That(statusAfterDefaultTimeout, Is.EqualTo(TransactionStatus.Active));
            Assert.That(broker.DrainQueue(DestinationQueue), Is.EqualTo(1));
        }
    }

    // Goes through NonDurableTransport.Initialize so the tests cover how the option reaches the runner.
    static async Task<(TransportInfrastructure Infrastructure, NonDurableMessagePump Receiver)> CreateReceiver(NonDurableBroker broker, TimeSpan? transactionTimeout, CancellationToken cancellationToken)
    {
        var transport = new NonDurableTransport(new NonDurableTransportOptions(broker) { TransactionTimeout = transactionTimeout });
        var infrastructure = await transport.Initialize(
            new HostSettings("endpoint", string.Empty, new StartupDiagnosticEntries(), static (_, _, _) => { }, true),
            [new ReceiveSettings("receiver", new QueueAddress(InputQueue), false, false, "error")],
            ["error"],
            cancellationToken);

        return (infrastructure, (NonDurableMessagePump)infrastructure.Receivers["receiver"]);
    }

    static Task SendToDestination(TransportInfrastructure infrastructure, TransportTransaction transaction, CancellationToken cancellationToken) =>
        infrastructure.Dispatcher.Dispatch(new TransportOperations(CreateUnicast(DestinationQueue, DispatchConsistency.Default)), transaction, cancellationToken);

    static Transaction GetTransaction(TransportTransaction transaction) => transaction.Get<Transaction>(NonDurableTransactionKeys.Transaction);

    // Waits until a transaction created with TransactionManager.DefaultTimeout has been aborted, then reports the
    // status of the given transaction. That shows the given transaction outlived the default timeout.
    static async Task<TransactionStatus> GetStatusAfterDefaultTimeout(Transaction transaction, CancellationToken cancellationToken)
    {
        using var probe = new CommittableTransaction();
        await WaitUntilAborted(probe, cancellationToken);
        return transaction.TransactionInformation.Status;
    }

    // System.Transactions checks timeouts on a timer with ~512ms resolution, so a transaction with a 100ms timeout
    // is aborted within ~1.5s. The upper bound only stops a broken test from hanging.
    static async Task WaitUntilAborted(Transaction transaction, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (transaction.TransactionInformation.Status != TransactionStatus.Aborted)
        {
            if (stopwatch.Elapsed > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("The transaction was not aborted by its timeout.");
            }

            await Task.Delay(50, cancellationToken);
        }
    }

    const string InputQueue = "input";
    const string DestinationQueue = "destination";
    static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(100);

    TimeSpan originalDefaultTimeout;
}

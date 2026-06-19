#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NServiceBus.Extensibility;
using NServiceBus.Outbox;
using NServiceBus.Persistence;
using NServiceBus.Transport;
using NUnit.Framework;

// Exercises the cross-repo transaction coordination seam at the unit level:
// the transport publishes a CommittableTransaction (as Transaction) into the
// TransportTransaction bag and enlists a volatile pending-envelope RM. A fake
// ICompletableSynchronizedStorageSession mirrors the NonDurable persistence
// contract (TryGet(out Transaction) + EnlistVolatile) so we can prove saga
// mutations and enlisted sends share one commit/rollback decision.
[TestFixture]
public class When_coordinating_with_synchronized_storage
{
    [Test]
    public async Task Commit_should_apply_saga_mutation_and_flush_enlisted_send()
    {
        var session = new FakeStorageSession();
        var (runner, dispatcher) = NewRunner();

        runner.Initialize(async (messageContext, ct) =>
            {
                await session.TryOpen(messageContext.TransportTransaction, new ContextBag(), ct);
                session.ApplyMutation();
                messageContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(NewEnvelope());
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await runner.Process(ReceivedEnvelope());

        using (Assert.EnterMultipleScope())
        {
            Assert.That(session.AppliedOnPrepare, Is.True, "saga mutation applied during Commit");
            Assert.That(session.RolledBack, Is.False, "saga must not roll back on success");
            Assert.That(dispatcher.FlushedCount, Is.EqualTo(1), "exactly one enlisted send flushed");
        }
    }

    [Test]
    public async Task Handler_failure_should_rollback_saga_and_dispose_enlisted_send()
    {
        var session = new FakeStorageSession();
        var pool = new TrackingPool();
        var (runner, dispatcher) = NewRunner();

        runner.Initialize(async (messageContext, ct) =>
            {
                await session.TryOpen(messageContext.TransportTransaction, new ContextBag(), ct);
                session.ApplyMutation();
                messageContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(NewEnvelopeWith(pool));
                throw new InvalidOperationException("boom");
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await runner.Process(ReceivedEnvelope());

        using (Assert.EnterMultipleScope())
        {
            Assert.That(session.RolledBack, Is.True, "saga mutation rolled back on handler failure");
            Assert.That(session.AppliedOnPrepare, Is.False, "saga Prepare must not run on failure");
            Assert.That(pool.Returned, Is.EqualTo(1), "enlisted send buffer returned to pool on rollback");
            Assert.That(dispatcher.FlushedCount, Is.EqualTo(0), "no enlisted send flushed on failure");
        }
    }

    [Test]
    public void CommittableTransaction_should_be_published_under_Transaction_base_type()
    {
        // BLOCKER 1 regression guard: type-inferred Set would key on
        // CommittableTransaction and the persistence adapter would never find it.
        var transportTransaction = new TransportTransaction();
        var committable = new CommittableTransaction();

        transportTransaction.Set<Transaction>(committable);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(transportTransaction.TryGet(out Transaction? published), Is.True);
            Assert.That(published, Is.SameAs(committable));
        }

        committable.Dispose();
    }

    [TestCase(TransportTransactionMode.ReceiveOnly)]
    [TestCase(TransportTransactionMode.None)]
    public async Task Should_not_publish_transaction_in_non_atomic_modes(TransportTransactionMode mode)
    {
        TransportTransaction? captured = null;
        var broker = new NonDurableBroker();
        var runner = new InlineExecutionRunner(
            "input",
            mode,
            static (_, _, _) => { },
            broker,
            static () => CancellationToken.None);
        runner.Initialize(
            (messageContext, ct) =>
            {
                captured = messageContext.TransportTransaction;
                return Task.CompletedTask;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await runner.Process(ReceivedEnvelope());

        Assert.That(captured!.TryGet(out Transaction? _), Is.False);
    }

    static BrokerEnvelope ReceivedEnvelope() =>
        BrokerPayloadStore.Borrow("received", [1], new Dictionary<string, string>(), "input", isPublished: false, sequenceNumber: 1);

    static BrokerEnvelope NewEnvelope() =>
        BrokerPayloadStore.Borrow("pending", [2], new Dictionary<string, string>(), "destination", isPublished: false, sequenceNumber: 1);

    static BrokerEnvelope NewEnvelopeWith(TrackingPool pool)
    {
        var buffer = pool.Rent(1);
        buffer[0] = 7;
        return new BrokerEnvelope(
            "pending",
            new ReadOnlyMemory<byte>(buffer, 0, 1),
            new Dictionary<string, string>(),
            "destination",
            false,
            1)
        {
            Pool = pool,
            Buffer = buffer
        };
    }

    static (InlineExecutionRunner Runner, CapturingDispatcher Dispatcher) NewRunner()
    {
        var broker = new NonDurableBroker();
        var dispatcher = new CapturingDispatcher();
        var runner = new InlineExecutionRunner(
            "input",
            TransportTransactionMode.SendsAtomicWithReceive,
            static (_, _, _) => { },
            broker,
            static () => CancellationToken.None);
        runner.SetDispatcher(dispatcher);
        return (runner, dispatcher);
    }

    sealed class CapturingDispatcher : IMessageDispatcher
    {
        public int FlushedCount { get; private set; }

        public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, CancellationToken cancellationToken = default)
        {
            FlushedCount += outgoingMessages.UnicastTransportOperations.Count;
            return Task.CompletedTask;
        }
    }

    sealed class TrackingPool : ArrayPool<byte>
    {
        public int Returned { get; private set; }

        public override byte[] Rent(int minimumLength) => new byte[minimumLength];

        public override void Return(byte[] array, bool clearArray = false) => Returned++;
    }

    // Mirrors the NonDurable persistence SynchronizedStorageSession contract:
    // TryOpen(TransportTransaction) looks up the Transaction base type from the
    // bag and enlists volatile. A "mutation" is applied at Prepare time and
    // rolled back on Rollback, exactly like the persistence EnlistmentNotification.
    sealed class FakeStorageSession : ICompletableSynchronizedStorageSession
    {
        public bool AppliedOnPrepare { get; private set; }
        public bool RolledBack { get; private set; }

        public void ApplyMutation() => mutationEnlisted = true;

        public ValueTask<bool> TryOpen(TransportTransaction transportTransaction, ContextBag context, CancellationToken cancellationToken = default)
        {
            if (transportTransaction.TryGet(out Transaction? tx) && tx is not null)
            {
                tx.EnlistVolatile(new MutationEnlistment(this), EnlistmentOptions.None);
                return new ValueTask<bool>(true);
            }
            return new ValueTask<bool>(false);
        }

        public ValueTask<bool> TryOpen(IOutboxTransaction transaction, ContextBag context, CancellationToken cancellationToken = default) => new(false);
        public Task Open(ContextBag context, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task CompleteAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void Dispose() { }
        public ValueTask DisposeAsync() => default;

        bool mutationEnlisted;

        sealed class MutationEnlistment(FakeStorageSession owner) : IEnlistmentNotification
        {
            public void Prepare(PreparingEnlistment preparingEnlistment)
            {
                if (owner.mutationEnlisted)
                {
                    owner.AppliedOnPrepare = true;
                }
                preparingEnlistment.Prepared();
            }

            public void Commit(Enlistment enlistment) => enlistment.Done();
            public void Rollback(Enlistment enlistment)
            {
                owner.RolledBack = true;
                enlistment.Done();
            }
            public void InDoubt(Enlistment enlistment) => enlistment.Done();
        }
    }
}

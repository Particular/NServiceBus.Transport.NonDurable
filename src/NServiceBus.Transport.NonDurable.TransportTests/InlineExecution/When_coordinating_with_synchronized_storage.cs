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
// the transport publishes a CommittableTransaction under a dedicated key in the
// TransportTransaction bag and enlists a volatile pending-envelope RM. A fake
// ICompletableSynchronizedStorageSession mirrors the NonDurable persistence
// contract (keyed TryGet + EnlistVolatile) so we can prove saga
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
    public async Task SendsAtomicWithReceive_should_not_publish_an_ambient_transaction()
    {
        // Regression guard for a customer repro: the runner must NOT set Transaction.Current
        // around the handler. If it did, a handler opening a SqlConnection with the default
        // Enlist=true would auto-enlist into the transport-owned CommittableTransaction, which
        // the runner later commits/disposes — poisoning the connection pool with
        // ObjectDisposedException. The coordinator is shared ONLY via the dedicated bag key
        // (mirrors the SQL transport SendsAtomicWithReceive strategy, which never sets an ambient).
        // Sentinel: a non-null Transaction reference so a skipped handler is distinct from a
        // correctly-null ambient. The handler must overwrite both to null.
        using var sentinel = new CommittableTransaction();
        var ambientDuringSyncPrologue = (Transaction?)sentinel;
        var ambientAfterAwait = (Transaction?)sentinel;
        TransportTransaction? capturedBag = null;
        var (runner, _) = NewRunner();

        runner.Initialize(async (messageContext, ct) =>
            {
                capturedBag = messageContext.TransportTransaction;
                ambientDuringSyncPrologue = Transaction.Current; // observed before any await
                await Task.Yield();
                ambientAfterAwait = Transaction.Current;
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await runner.Process(ReceivedEnvelope());

        using (Assert.EnterMultipleScope())
        {
            Assert.That(ambientDuringSyncPrologue, Is.Null, "Transaction.Current must be null in the handler's synchronous prologue (no auto-enlistment surface)");
            Assert.That(ambientAfterAwait, Is.Null, "Transaction.Current must be null after an await in the handler");
            Assert.That(capturedBag!.TryGet(NonDurableTransactionKeys.Transaction, out Transaction? published), Is.True, "the coordinator is still published under the dedicated bag key");
            Assert.That(published, Is.Not.Null);
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
    public async Task Commit_failure_from_saga_prepare_should_dispose_enlisted_send_and_recover()
    {
        // Mirrors a saga concurrency conflict: the persistence ForceRollbacks at Prepare,
        // so committable.Commit() throws TransactionAbortedException. The BCL does not call
        // Rollback on the RM that initiated the ForceRollback (the persistence learns of the
        // rollback by driving it), but it DOES call Rollback synchronously on the sibling
        // transport RM, which disposes the pre-failure enlisted send. Recoverability is
        // reached without re-committing the aborted transaction.
        var session = FakeStorageSession.WithForceRollback();
        var pool = new TrackingPool();
        var (runner, dispatcher) = NewRunner();

        runner.Initialize(async (messageContext, ct) =>
            {
                await session.TryOpen(messageContext.TransportTransaction, new ContextBag(), ct);
                session.ApplyMutation();
                messageContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(NewEnvelopeWith(pool));
            },
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        await runner.Process(ReceivedEnvelope());

        using (Assert.EnterMultipleScope())
        {
            Assert.That(session.AppliedOnPrepare, Is.False, "saga mutation must not be applied when Prepare force-rollbacks");
            Assert.That(pool.Returned, Is.EqualTo(1), "enlisted send buffer returned to pool on Commit failure");
            Assert.That(dispatcher.FlushedCount, Is.EqualTo(0), "no enlisted send flushed on Commit failure");
        }
    }

    [Test]
    public async Task Flush_failure_after_commit_should_not_rollback_committed_tx_or_mask_exception()
    {
        // committable.Commit() succeeds (saga applied) but the post-Commit enlisted-send
        // flush throws. The committed transaction must NOT be rolled back (Rollback on a
        // committed tx throws TransactionException and would mask the flush error). The flush
        // exception is surfaced to recoverability; the committed-tx Rollback is skipped.
        var session = new FakeStorageSession();
        var pool = new TrackingPool();
        var (runner, dispatcher) = NewRunner();
        dispatcher.ThrowOnFlushCall = 0; // fail the success-path flush
        var recoverabilityInvoked = false;

        runner.Initialize(async (messageContext, ct) =>
            {
                await session.TryOpen(messageContext.TransportTransaction, new ContextBag(), ct);
                session.ApplyMutation();
                messageContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(NewEnvelopeWith(pool));
            },
            (_, _) =>
            {
                recoverabilityInvoked = true;
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await runner.Process(ReceivedEnvelope());

        using (Assert.EnterMultipleScope())
        {
            Assert.That(session.AppliedOnPrepare, Is.True, "saga mutation applied during Commit before flush threw");
            Assert.That(session.RolledBack, Is.False, "committed saga must not be rolled back when flush fails");
            Assert.That(recoverabilityInvoked, Is.True, "flush failure must reach recoverability, not escape the catch");
        }
    }

    [Test]
    public async Task Error_path_flush_failure_after_error_commit_should_not_rollback_committed_error_tx()
    {
        // NServiceBus Core recoverability (RecoverabilityExecutor) dispatches the error-queue
        // move with DispatchConsistency.Default against errorContext.TransportTransaction, so
        // NonDurableDispatcher.TryEnlistToReceiveTransaction enlists the move into the error tx
        // the runner publishes (errorCommittable). This test stands in for that Core executor:
        // the fake onError enlists a Default send into the error enlistment, then errorCommittable
        // .Commit() succeeds and the subsequent flush throws. The committed error tx must NOT be
        // rolled back (Rollback on a committed tx throws TransactionException and would mask the
        // flush error). The flush exception rethrows unmasked instead.
        var session = new FakeStorageSession();
        var errorPool = new TrackingPool();
        var (runner, dispatcher) = NewRunner();
        dispatcher.ThrowOnFlushCall = 0; // the handler throws, so there is no success flush; the error-path flush is the first (and only) flush
        var recoverabilityInvoked = false;

        runner.Initialize(async (messageContext, ct) =>
            {
                await session.TryOpen(messageContext.TransportTransaction, new ContextBag(), ct);
                session.ApplyMutation();
                throw new InvalidOperationException("handler boom");
            },
            (errorContext, _) =>
            {
                recoverabilityInvoked = true;
                errorContext.TransportTransaction.Get<PendingEnvelopeEnlistment>().Add(NewEnvelopeWith(errorPool));
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        Exception? thrown = null;
        try
        {
            await runner.Process(ReceivedEnvelope());
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        using (Assert.EnterMultipleScope())
        {
            Assert.That(session.RolledBack, Is.True, "original handler tx rolled back");
            Assert.That(recoverabilityInvoked, Is.True, "onError must run");
            Assert.That(dispatcher.FlushedCount, Is.EqualTo(1), "error-path flush must dispatch the enlisted send");
            Assert.That(thrown, Is.Not.Null, "the error-path flush exception must rethrow unmasked");
            Assert.That(thrown?.Message, Is.EqualTo("flush boom on call 0"), "rethrown exception must be the flush failure, not a TransactionException");
            Assert.That(thrown, Is.InstanceOf<InvalidOperationException>(), "must not be masked by a TransactionException from Rollback on a committed tx");
        }
    }

    [Test]
    public void CommittableTransaction_should_be_published_under_dedicated_key()
    {
        var transportTransaction = new TransportTransaction();
        var committable = new CommittableTransaction();

        transportTransaction.Set(NonDurableTransactionKeys.Transaction, committable);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(transportTransaction.TryGet(NonDurableTransactionKeys.Transaction, out Transaction? published), Is.True);
            Assert.That(published, Is.SameAs(committable));
            Assert.That(transportTransaction.TryGet(out Transaction? _), Is.False);
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

        Assert.That(captured!.TryGet(NonDurableTransactionKeys.Transaction, out Transaction? _), Is.False);
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
        // 0-based index of the flush call that should throw (null = none). Call 0 is the
        // success-path flush; call 1 is the error-path (recoverability) flush.
        public int? ThrowOnFlushCall;

        public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, CancellationToken cancellationToken = default)
        {
            var callIndex = FlushedCount;
            FlushedCount += outgoingMessages.UnicastTransportOperations.Count;
            if (ThrowOnFlushCall is int target && callIndex == target)
            {
                throw new InvalidOperationException($"flush boom on call {target}");
            }
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
    // TryOpen(TransportTransaction) looks up the dedicated transaction key from the
    // bag and enlists volatile. A "mutation" is applied at Prepare time and
    // rolled back on Rollback, exactly like the persistence EnlistmentNotification.
    sealed class FakeStorageSession : ICompletableSynchronizedStorageSession
    {
        public bool AppliedOnPrepare { get; private set; }
        public bool RolledBack { get; private set; }

        public static FakeStorageSession WithForceRollback() => new() { forceRollback = true };

        public void ApplyMutation() => mutationEnlisted = true;

        public ValueTask<bool> TryOpen(TransportTransaction transportTransaction, ContextBag context, CancellationToken cancellationToken = default)
        {
            if (transportTransaction.TryGet(NonDurableTransactionKeys.Transaction, out Transaction? tx) && tx is not null)
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
        bool forceRollback;

        sealed class MutationEnlistment(FakeStorageSession owner) : IEnlistmentNotification
        {
            public void Prepare(PreparingEnlistment preparingEnlistment)
            {
                if (owner.forceRollback)
                {
                    preparingEnlistment.ForceRollback(new InvalidOperationException("saga concurrency conflict"));
                    return;
                }

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

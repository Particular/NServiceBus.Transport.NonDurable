namespace NServiceBus;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using DelayedDelivery;
using Extensibility;
using Performance.TimeToBeReceived;
using Routing;
using Transport;

sealed class InlineExecutionRunner(
    string receiveAddress,
    TransportTransactionMode transactionMode,
    Action<string, Exception, CancellationToken> criticalErrorAction,
    NonDurableBroker broker,
    Func<CancellationToken> processingCancellationTokenAccessor)
{
    public void Initialize(OnMessage onMessage, OnError onError)
    {
        this.onMessage = onMessage;
        this.onError = onError;
    }

    public void SetPump(NonDurableMessagePump pump) => this.pump = pump;

    public void SetDispatcher(IMessageDispatcher dispatcher) => this.dispatcher = dispatcher;

    public void UpdateProcessingCancellationTokenAccessor(Func<CancellationToken> accessor) => processingCancellationTokenAccessor = accessor;

    public async Task Process(BrokerEnvelope envelope, CancellationToken cancellationToken = default)
    {
        var headers = new Dictionary<string, string>(envelope.Headers);
        var transportTransaction = new TransportTransaction();
        CommittableTransaction? committable = null;
        CommittableTransaction? errorCommittable = null;
        PendingEnvelopeEnlistment? enlistment = null;

        if (transactionMode == TransportTransactionMode.SendsAtomicWithReceive)
        {
            // Atomicity applies to the non-outbox saga + synchronized-storage case: the
            // NonDurable persistence enlists volatile into this CommittableTransaction via
            // the dedicated bag key. When the outbox is enabled, the persistence joins
            // NonDurableOutboxTransaction instead, which is NOT enlisted here and provides
            // its own consistency boundary.
            committable = new CommittableTransaction();
            transportTransaction.Set(NonDurableTransactionKeys.Transaction, committable);
            enlistment = new PendingEnvelopeEnlistment();
            committable.EnlistVolatile(enlistment, EnlistmentOptions.None);
            transportTransaction.Set(enlistment);
        }

        transportTransaction.Set(ReceivePipelineTransportTransactionMarker.Instance);

        var contextBag = new ContextBag();
        Activity? transportActivity = null;

        var inlineState = envelope.InlineState;
        if (inlineState != null)
        {
            transportTransaction.Set(inlineState.Scope);
            var dispatchContext = new InlineExecutionDispatchContext(inlineState.Scope, inlineState.Depth);
            transportTransaction.Set(dispatchContext);
            contextBag.Set(dispatchContext);
        }

        if (NonDurableTransportTracing.HasListeners())
        {
            transportActivity = NonDurableTransportTracing.StartProcess(envelope, receiveAddress);
            if (transportActivity != null)
            {
                contextBag.Set(transportActivity);
            }
        }

        var messageContext = new MessageContext(
            envelope.MessageId,
            headers,
            envelope.Body,
            transportTransaction,
            receiveAddress,
            contextBag);

        var previousAmbient = Transaction.Current;
        if (committable != null)
        {
            // The ambient transaction is published so ambient-transaction-aware components
            // (including the NonDurable persistence transport-adaptation path) can enlist into
            // the per-Process CommittableTransaction. It is restored to previousAmbient before
            // Commit/Rollback and again in finally. The coordinator is also published under the
            // dedicated cross-repo string key NonDurableTransactionKeys.Transaction for consumers
            // that read the bag explicitly.
            Transaction.Current = committable;
        }

        var committed = false;

        try
        {
            await onMessage(messageContext, ProcessingCancellationToken).ConfigureAwait(false);

            if (committable != null)
            {
                Transaction.Current = previousAmbient;
                committable.Commit();
                committed = true;
                await CommitPendingToBrokerAsync(enlistment!, ProcessingCancellationToken).ConfigureAwait(false);
            }

            inlineState?.Scope.CompleteDispatchSuccess();
            NonDurableTransportTracing.MarkSuccess(transportActivity);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ProcessingCancellationToken.IsCancellationRequested)
        {
            NonDurableTransportTracing.MarkError(transportActivity, ex);
            Transaction.Current = previousAmbient;
            // A CommittableTransaction is single-use: once Commit() has succeeded it cannot be
            // rolled back (Rollback() on a committed tx throws TransactionException). If the
            // post-Commit enlisted-send flush threw, the saga/persistence mutations are already
            // committed; surface the flush failure to recoverability instead of attempting an
            // invalid rollback that would mask the original exception and leak pooled buffers.
            if (!committed)
            {
                committable?.Rollback();
            }

            if (committable != null)
            {
                // Publish a fresh error transaction for recoverability. Core's RecoverabilityExecutor
                // dispatches the error-queue move with DispatchConsistency.Default against
                // errorContext.TransportTransaction, so the move enlists into this tx and is
                // committed atomically on ErrorHandleResult.Handled (or rolled back on RetryRequired).
                errorCommittable = new CommittableTransaction();
                transportTransaction.Set(NonDurableTransactionKeys.Transaction, errorCommittable);
                enlistment = new PendingEnvelopeEnlistment();
                errorCommittable.EnlistVolatile(enlistment, EnlistmentOptions.None);
                transportTransaction.Set(enlistment);
            }

            var errorContext = new ErrorContext(
                ex,
                new Dictionary<string, string>(envelope.Headers),
                envelope.MessageId,
                envelope.Body,
                transportTransaction,
                envelope.DeliveryAttempt,
                receiveAddress,
                contextBag);

            var result = await HandleErrorAsync(errorContext, messageContext, cancellationToken).ConfigureAwait(false);

            if (result == ErrorHandleResult.Handled && errorCommittable != null)
            {
                // errorCommittable.Commit() may throw TransactionAbortedException if an RM
                // enlisted via the bag key force-rolls back at Prepare. If Commit() succeeded
                // and the subsequent flush threw, the tx is already committed and must not be
                // rolled back (Rollback on a committed tx throws TransactionException and would
                // mask the flush failure). Mirror the main-path committed-flag discipline.
                var errorCommitted = false;
                try
                {
                    errorCommittable.Commit();
                    errorCommitted = true;
                    await CommitPendingToBrokerAsync(enlistment!, ProcessingCancellationToken).ConfigureAwait(false);
                }
                catch (Exception commitEx) when (commitEx is not OperationCanceledException || !ProcessingCancellationToken.IsCancellationRequested)
                {
                    if (!errorCommitted)
                    {
                        errorCommittable.Rollback();
                    }
                    throw;
                }
            }
            else
            {
                errorCommittable?.Rollback();
            }

            if (result == ErrorHandleResult.RetryRequired)
            {
                if (inlineState != null && pump != null)
                {
                    pump.TrackPendingInlineScope(inlineState.Scope);
                }

                var retryEnvelope = envelope.WithDeliveryAttempt(envelope.DeliveryAttempt + 1);
                var retryQueue = broker.GetOrCreateQueue(receiveAddress);
                await retryQueue.Enqueue(retryEnvelope, CancellationToken.None).ConfigureAwait(false);
                return;
            }

            if (IsDeferredRetry(errorContext.TransportTransaction))
            {
                envelope.Dispose();
                return;
            }

            inlineState?.Scope.CompleteDispatchFailure(ex);
            envelope.Dispose();
        }
        finally
        {
            Transaction.Current = previousAmbient;
            committable?.Dispose();
            errorCommittable?.Dispose();
            transportActivity?.Dispose();
        }
    }

    async Task<ErrorHandleResult> HandleErrorAsync(
        ErrorContext errorContext,
        MessageContext messageContext,
        CancellationToken pumpCancellationToken)
    {
        try
        {
            return await onError(errorContext, ProcessingCancellationToken).ConfigureAwait(false);
        }
        catch (Exception onErrorException) when (onErrorException is not OperationCanceledException || !ProcessingCancellationToken.IsCancellationRequested)
        {
            criticalErrorAction($"Failed to execute recoverability policy for message with native ID: `{messageContext.NativeMessageId}`", onErrorException, pumpCancellationToken);
            return ErrorHandleResult.RetryRequired;
        }
    }

    async Task CommitPendingToBrokerAsync(PendingEnvelopeEnlistment enlistment, CancellationToken cancellationToken)
    {
        var envelopes = enlistment.GetPendingAndClear();
        if (envelopes.Count == 0)
        {
            return;
        }

        if (dispatcher == null)
        {
            foreach (var envelope in envelopes)
            {
                var queue = broker.GetOrCreateQueue(envelope.Destination);
                await queue.Enqueue(envelope, cancellationToken).ConfigureAwait(false);
                envelope.Dispose();
            }
            return;
        }

        var operations = new TransportOperation[envelopes.Count];
        for (var i = 0; i < envelopes.Count; i++)
        {
            var pending = envelopes[i];
            operations[i] = new TransportOperation(
                new OutgoingMessage(pending.MessageId, new Dictionary<string, string>(pending.Headers), pending.Body),
                new UnicastAddressTag(pending.Destination),
                CreateDispatchProperties(pending),
                DispatchConsistency.Default);
        }

        try
        {
            await dispatcher.Dispatch(new TransportOperations(operations), new TransportTransaction(), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            foreach (var envelope in envelopes)
            {
                envelope.Dispose();
            }
        }
    }

    DispatchProperties CreateDispatchProperties(BrokerEnvelope pending)
    {
        var properties = new DispatchProperties();

        if (pending.DeliverAt.HasValue)
        {
            properties.DoNotDeliverBefore = new DoNotDeliverBefore(pending.DeliverAt.Value);
        }

        if (pending.DiscardAfter.HasValue)
        {
            var remaining = pending.DiscardAfter.Value - broker.GetCurrentTime();
            properties.DiscardIfNotReceivedBefore = new DiscardIfNotReceivedBefore(remaining <= TimeSpan.Zero ? TimeSpan.Zero : remaining);
        }

        return properties;
    }

    static bool IsDeferredRetry(TransportTransaction transportTransaction) =>
        transportTransaction.TryGet<RecoverabilityAction>(out var action) &&
        action is DelayedRetry or ImmediateRetry;

    CancellationToken ProcessingCancellationToken => processingCancellationTokenAccessor();

    OnMessage onMessage = null!;
    OnError onError = null!;
    IMessageDispatcher? dispatcher;
    NonDurableMessagePump? pump;
    Func<CancellationToken> processingCancellationTokenAccessor = processingCancellationTokenAccessor;
}

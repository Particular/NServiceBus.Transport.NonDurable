namespace NServiceBus;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Transport;

class NonDurableDispatcher(
    NonDurableBroker broker,
    InlineExecutionSettings inlineExecutionSettings,
    HashSet<string> localReceiveAddresses,
    IReadOnlyDictionary<string, InlineExecutionRunner> inlineExecutionRunners,
    IReadOnlyDictionary<string, NonDurableMessagePump> pumpsByAddress) : IMessageDispatcher
{
    public Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, CancellationToken cancellationToken = default)
    {
        var unicastTask = DispatchUnicast(outgoingMessages.UnicastTransportOperations, transaction, cancellationToken);
        var multicastTask = DispatchMulticast(outgoingMessages.MulticastTransportOperations, transaction, cancellationToken);

        return CombineTasks(unicastTask, multicastTask, cancellationToken);
    }

    Task DispatchMulticast(List<MulticastTransportOperation> operations, TransportTransaction transaction, CancellationToken cancellationToken)
    {
        var tasks = new List<Task>(operations.Count);

        foreach (var transportOperation in operations)
        {
            tasks.Add(DispatchMulticastOperation(transportOperation, transaction, cancellationToken));
        }

        return CombineTasks(tasks, cancellationToken);
    }

    static Task CombineTasks(Task task1, Task task2, CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        if (task1 == Task.CompletedTask)
        {
            return task2;
        }

        return task2 == Task.CompletedTask ? task1 : Task.WhenAll(task1, task2);
    }

    HashSet<string> GetSubscribersForType(Type messageType)
    {
        HashSet<string> result = [];
        foreach (var type in GetPotentialEventTypes(messageType))
        {
            foreach (var subscriber in broker.GetSubscribers(type.FullName!))
            {
                result.Add(subscriber);
            }
        }
        return result;
    }

    static Type[] GetPotentialEventTypes(Type messageType) =>
        potentialEventTypesCache.GetOrAdd(messageType, static type =>
        {
            HashSet<Type> allEventTypes = [];
            for (var current = type; current != null && !IsCoreMarkerInterface(current); current = current.BaseType)
            {
                allEventTypes.Add(current);
            }
            foreach (var iface in type.GetInterfaces())
            {
                if (!IsCoreMarkerInterface(iface))
                {
                    allEventTypes.Add(iface);
                }
            }
            var result = new Type[allEventTypes.Count];
            allEventTypes.CopyTo(result);
            return result;
        });

    static readonly ConcurrentDictionary<Type, Type[]> potentialEventTypesCache = new();

    static bool IsCoreMarkerInterface(Type type) =>
        type == typeof(IMessage) || type == typeof(IEvent) || type == typeof(ICommand);

    Task DispatchUnicast(List<UnicastTransportOperation> operations, TransportTransaction transaction, CancellationToken cancellationToken)
    {
        var tasks = new List<Task>(operations.Count);

        foreach (var operation in operations)
        {
            var task = DispatchUnicastOperation(operation, transaction, cancellationToken);
            tasks.Add(task);
        }

        return CombineTasks(tasks, cancellationToken);
    }

    async Task DispatchMulticastOperation(MulticastTransportOperation transportOperation, TransportTransaction transaction, CancellationToken cancellationToken)
    {
        var message = transportOperation.Message;
        var messageId = Guid.CreateVersion7().ToString();
        var sequenceNumber = broker.GetNextSequenceNumber();

        var subscribers = GetSubscribersForType(transportOperation.MessageType);

        foreach (var subscriber in subscribers)
        {
            var now = broker.GetCurrentTime();
            var deliverAt = GetDeliverAt(transportOperation.Properties, now);
            var discardAfter = GetDiscardAfter(transportOperation.Properties, now);
#pragma warning disable CA2000 // ownership is transferred to the caller
            var envelope = BrokerPayloadStore.Borrow(
                messageId,
                message.Body.Span,
                message.Headers,
                subscriber,
                isPublished: true,
                sequenceNumber,
                deliverAt,
                discardAfter);
#pragma warning restore CA2000

            if (TryEnlistToReceiveTransaction(transaction, envelope, transportOperation.RequiredDispatchConsistency))
            {
                continue;
            }

            await DispatchToBroker(subscriber, messageId, envelope, deliverAt, cancellationToken).ConfigureAwait(false);
        }
    }

    Task DispatchUnicastOperation(UnicastTransportOperation operation, TransportTransaction transaction, CancellationToken cancellationToken)
    {
        var message = operation.Message;
        var messageId = Guid.CreateVersion7().ToString();
        var sequenceNumber = broker.GetNextSequenceNumber();
        var now = broker.GetCurrentTime();
        var deliverAt = GetDeliverAt(operation.Properties, now);
        var discardAfter = GetDiscardAfter(operation.Properties, now);
#pragma warning disable CA2000 // ownership is transferred to the caller
        var envelope = BrokerPayloadStore.Borrow(
            messageId,
            message.Body.Span,
            message.Headers,
            operation.Destination,
            isPublished: false,
            sequenceNumber,
            deliverAt,
            discardAfter);
#pragma warning restore CA2000

        if (ShouldPreserveInlineScopeForDelayedRecoverability(transaction, deliverAt, out var preservedScope, out var preservedDispatchContext))
        {
            if (pumpsByAddress.TryGetValue(operation.Destination, out var pumpForDelayed))
            {
                pumpForDelayed.TrackPendingInlineScope(preservedScope!);
            }

            var preservedEnvelope = envelope with
            {
                InlineState = new InlineEnvelopeState(
                    preservedScope!,
                    preservedDispatchContext?.Depth ?? 0,
                    (preservedDispatchContext?.Depth ?? 0) == 0)
            };

            return DispatchRegularUnicast(operation, preservedEnvelope, deliverAt, cancellationToken);
        }

        if (ShouldInline(operation, transaction, deliverAt))
        {
            return DispatchInlineLocalUnicast(operation, envelope, transaction, deliverAt, cancellationToken);
        }

        return TryEnlistToReceiveTransaction(transaction, envelope, operation.RequiredDispatchConsistency) ? Task.CompletedTask : DispatchRegularUnicast(operation, envelope, deliverAt, cancellationToken);
    }

    Task DispatchInlineLocalUnicast(UnicastTransportOperation operation, BrokerEnvelope envelope, TransportTransaction transaction, DateTimeOffset? deliverAt, CancellationToken cancellationToken)
    {
        var existingScope = TryGetInlineScope(transaction, out var scope);
        scope ??= new InlineExecutionScope(Guid.NewGuid());

        if (existingScope)
        {
            var destinationPump = pumpsByAddress[operation.Destination];
            if (!destinationPump.TryEnterInlineProcessing())
            {
                envelope.Dispose();
                return Task.FromException(new OperationCanceledException($"Inline dispatch to '{operation.Destination}' was rejected because the receiver is stopping."));
            }

            scope.BeginDispatch();
            var inlineEnvelope = envelope with
            {
                InlineState = new InlineEnvelopeState(scope, 1, false)
            };
            var runner = inlineExecutionRunners[operation.Destination];
            var processing = ProcessReentrantInline(runner, destinationPump, inlineEnvelope, scope, cancellationToken);

            ObserveReentrantProcessing(processing);

            return processing;
        }

        scope.BeginDispatch();
        if (pumpsByAddress.TryGetValue(operation.Destination, out var pump))
        {
            pump.TrackPendingInlineScope(scope);
        }

        var rootEnvelope = envelope with
        {
            InlineState = new InlineEnvelopeState(scope, 0, true)
        };
        var completion = scope.Completion;
        var preparation = DispatchRegularUnicast(operation, rootEnvelope, deliverAt, cancellationToken);

        return preparation.IsCompletedSuccessfully ? completion : AwaitInlineDispatch(preparation, scope, completion, cancellationToken);
    }

    static Task ProcessReentrantInline(
        InlineExecutionRunner runner,
        NonDurableMessagePump destinationPump,
        BrokerEnvelope envelope,
        InlineExecutionScope scope,
        CancellationToken cancellationToken)
    {
        var processing = runner.ProcessInline(envelope, cancellationToken);
        return processing.ContinueWith(
            static (completed, state) =>
            {
                var (pump, inlineEnvelope, inlineScope) = ((NonDurableMessagePump, BrokerEnvelope, InlineExecutionScope))state!;
                try
                {
                    if (completed.IsCanceled)
                    {
                        inlineScope.CompleteDispatchCanceled(new TaskCanceledException(completed));
                        inlineEnvelope.Dispose();
                    }
                    else if (completed.IsFaulted)
                    {
                        var exception = completed.Exception.InnerExceptions.Count == 1
                            ? completed.Exception.InnerException!
                            : completed.Exception;
                        inlineScope.CompleteDispatchFailure(exception);
                        inlineEnvelope.Dispose();
                    }
                }
                finally
                {
                    pump.ExitInlineProcessing();
                }

                return completed;
            },
            (destinationPump, envelope, scope),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default).Unwrap();
    }

    static async Task AwaitInlineDispatch(Task preparation, InlineExecutionScope scope, Task completion, CancellationToken cancellationToken)
    {
        try
        {
            await preparation.ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
        {
            scope.CompleteDispatchCanceled(ex);
            await completion.ConfigureAwait(false);
        }
#pragma warning disable CA1031
        catch (Exception ex)
#pragma warning restore CA1031
        {
            scope.CompleteDispatchFailure(ex);
            await completion.ConfigureAwait(false);
        }

        await completion.ConfigureAwait(false);
    }

    static void ObserveReentrantProcessing(Task processing)
    {
        if (processing.IsCompleted)
        {
            _ = processing.Exception;
            return;
        }

        _ = processing.ContinueWith(
            static task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    Task DispatchRegularUnicast(UnicastTransportOperation operation, BrokerEnvelope envelope, DateTimeOffset? deliverAt, CancellationToken cancellationToken) =>
        DispatchToBroker(operation.Destination, envelope.MessageId, envelope, deliverAt, cancellationToken);

    async Task DispatchToBroker(string destination, string messageId, BrokerEnvelope envelope, DateTimeOffset? deliverAt, CancellationToken cancellationToken)
    {
        Activity? activity = null;
        var ownsEnvelope = true;

        if (NonDurableTransportTracing.HasListeners())
        {
            var headers = (Dictionary<string, string>)envelope.Headers;
            try
            {
                activity = NonDurableTransportTracing.StartSend(destination, messageId, headers, deliverAt.HasValue);
            }
#pragma warning disable CA1031 // telemetry must never break dispatch
            catch (Exception)
#pragma warning restore CA1031
            {
                activity = null;
            }
            NonDurableTransportTracing.PropagateContextToHeaders(activity, headers);
        }

        try
        {
            await broker.SimulateSendAsync(destination, cancellationToken).ConfigureAwait(false);

            if (deliverAt.HasValue)
            {
                broker.EnqueueDelayed(envelope, deliverAt.Value);
                ownsEnvelope = false;
            }
            else
            {
                var queue = broker.GetOrCreateQueue(destination);
                await queue.Enqueue(envelope, cancellationToken).ConfigureAwait(false);
                ownsEnvelope = false;
            }

            NonDurableTransportTracing.AddProducerDispatchEvent(activity, deliverAt);
            NonDurableTransportTracing.MarkSuccess(activity);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            if (ownsEnvelope)
            {
                envelope.Dispose();
            }

            throw;
        }
        catch (Exception ex)
        {
            if (ownsEnvelope)
            {
                envelope.Dispose();
            }

            NonDurableTransportTracing.MarkError(activity, ex, exceptionEscaped: true);
            throw;
        }
        finally
        {
            activity?.Dispose();
        }
    }

    bool ShouldInline(UnicastTransportOperation operation, TransportTransaction transaction, DateTimeOffset? deliverAt)
    {
        if (!inlineExecutionSettings.IsEnabled)
        {
            return false;
        }

        if (!localReceiveAddresses.Contains(operation.Destination))
        {
            return false;
        }

        var isInsideReceivePipeline = transaction.IsInsideReceivePipeline();

        var hasInlineScope = TryGetInlineScope(transaction, out _);

        if (!isInsideReceivePipeline)
        {
            return !hasInlineScope;
        }

        return !deliverAt.HasValue && hasInlineScope;
    }

    static bool TryGetInlineScope(TransportTransaction transaction, out InlineExecutionScope? scope) => transaction.TryGet(out scope);

    static bool ShouldPreserveInlineScopeForDelayedRecoverability(TransportTransaction transaction, DateTimeOffset? deliverAt, out InlineExecutionScope? scope, out InlineExecutionDispatchContext? dispatchContext)
    {
        dispatchContext = null;

        if (!deliverAt.HasValue || !transaction.TryGet<RecoverabilityAction>(out var action) || action is not DelayedRetry)
        {
            scope = null;
            return false;
        }

        if (!TryGetInlineScope(transaction, out scope) || scope == null)
        {
            return false;
        }

        _ = transaction.TryGet(out dispatchContext);

        return true;
    }

    static Task CombineTasks(List<Task> tasks, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        var writeIndex = 0;

        for (var i = 0; i < tasks.Count; i++)
        {
            var task = tasks[i];
            if (task != Task.CompletedTask)
            {
                tasks[writeIndex++] = task;
            }
        }

        tasks.RemoveRange(writeIndex, tasks.Count - writeIndex);

        return writeIndex switch
        {
            0 => Task.CompletedTask,
            1 => tasks[0],
            2 => Task.WhenAll(tasks[0], tasks[1]),
            _ => Task.WhenAll(tasks)
        };
    }

    static DateTimeOffset? GetDeliverAt(DispatchProperties properties, DateTimeOffset now)
    {
        if (properties.DoNotDeliverBefore is not null)
        {
            return properties.DoNotDeliverBefore.At.ToUniversalTime();
        }

        return now + properties.DelayDeliveryWith?.Delay;
    }

    static DateTimeOffset? GetDiscardAfter(DispatchProperties properties, DateTimeOffset now)
    {
        var ttbr = properties.DiscardIfNotReceivedBefore;
        if (ttbr != null && ttbr.MaxTime < TimeSpan.MaxValue)
        {
            return now + ttbr.MaxTime;
        }
        return null;
    }

    static bool TryEnlistToReceiveTransaction(TransportTransaction transaction, BrokerEnvelope envelope, DispatchConsistency dispatchConsistency)
    {
        if (dispatchConsistency == DispatchConsistency.Isolated)
        {
            return false;
        }
        return transaction.TryGet<PendingEnvelopeEnlistment>(out var enlistment) && enlistment.Add(envelope);
    }
}
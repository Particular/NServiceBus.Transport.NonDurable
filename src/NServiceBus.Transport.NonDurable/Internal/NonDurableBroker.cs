namespace NServiceBus;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.RateLimiting;

/// <summary>
/// Provides the shared in-memory broker used by the non-durable transport.
/// </summary>
/// <remarks>
/// Messages exist only in memory and are lost when the broker is disposed or the process terminates.
/// </remarks>
public sealed class NonDurableBroker : IAsyncDisposable
{
    /// <summary>
    /// Creates a broker that clones the supplied options at construction time.
    /// </summary>
    /// <param name="options">Optional configuration. Effective values are captured now; later mutation affects only brokers constructed afterwards.</param>
    public NonDurableBroker(NonDurableBrokerOptions? options = null)
    {
        this.options = (options ?? new NonDurableBrokerOptions()).Clone();
        ValidateOptions(this.options);
        timeProvider = this.options.TimeProvider ?? TimeProvider.System;
    }

    internal NonDurableChannel GetOrCreateQueue(string address) => queues.GetOrAdd(address, _ => new NonDurableChannel());

    internal bool TryGetQueue(string address, out NonDurableChannel? queue) => queues.TryGetValue(address, out queue);

    internal void Subscribe(string publisherAddress, string topic) =>
        subscriptions.AddOrUpdate(
            topic,
            static (_, address) => new Lazy<string[]>([address]),
            static (_, currentLazy, address) =>
            {
                if (currentLazy.Value.AsSpan().IndexOf(address) >= 0)
                {
                    return currentLazy;
                }

                return new Lazy<string[]>(() =>
                {
                    var current = currentLazy.Value;
                    var currentSpan = current.AsSpan();
                    if (currentSpan.IndexOf(address) >= 0)
                    {
                        return current;
                    }

                    var next = new string[current.Length + 1];
                    currentSpan.CopyTo(next);
                    next[^1] = address;
                    return next;
                });
            },
            publisherAddress);

    internal void Unsubscribe(string publisherAddress, string topic) =>
        subscriptions.AddOrUpdate(
            topic,
            static (_, _) => new Lazy<string[]>([]),
            static (_, currentLazy, address) =>
            {
                if (currentLazy.Value.AsSpan().IndexOf(address) < 0)
                {
                    return currentLazy;
                }

                return new Lazy<string[]>(() =>
                {
                    var current = currentLazy.Value;
                    var currentSpan = current.AsSpan();
                    var index = currentSpan.IndexOf(address);
                    if (index < 0)
                    {
                        return current;
                    }

                    if (current.Length == 1)
                    {
                        return [];
                    }

                    var next = new string[current.Length - 1];
                    var nextSpan = next.AsSpan();
                    currentSpan[..index].CopyTo(nextSpan[..index]);
                    currentSpan[(index + 1)..].CopyTo(nextSpan[index..]);
                    return next;
                });
            },
            publisherAddress);

    internal IReadOnlyList<string> GetSubscribers(string topic) => subscriptions.TryGetValue(topic, out var lazy) ? lazy.Value : [];

    internal long GetNextSequenceNumber() => Interlocked.Increment(ref sequenceNumber);

    internal void EnqueueDelayed(BrokerEnvelope envelope, DateTimeOffset deliverAt)
    {
        lock (delayedMessagesLock)
        {
            delayedMessages.Enqueue(envelope.WithDeliverAt(deliverAt), (deliverAt, envelope.SequenceNumber));
            SignalDelayedMessagesChanged();
        }
    }

    internal bool TryDequeueDelayed(DateTimeOffset now, [NotNullWhen(true)] out BrokerEnvelope? envelope)
    {
        lock (delayedMessagesLock)
        {
            return TryDequeueDelayedCore(now, out envelope);
        }
    }

    internal Task SimulateSendAsync(string destination, CancellationToken cancellationToken = default) => !HasSimulationFor(NonDurableSimulationOperation.Send, destination) ? Task.CompletedTask : ApplySimulationAsync(NonDurableSimulationOperation.Send, destination, cancellationToken);

    internal Task SimulateReceiveAsync(string destination, CancellationToken cancellationToken = default) => !HasSimulationFor(NonDurableSimulationOperation.Receive, destination) ? Task.CompletedTask : ApplySimulationAsync(NonDurableSimulationOperation.Receive, destination, cancellationToken);

    internal Task StartPump(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref pumpStarted, 1, 0) != 0)
        {
            return Task.CompletedTask;
        }

        delayedPumpCancelSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        delayedPumpTask = StartDelayedMessagePump(delayedPumpCancelSource.Token);
        return Task.CompletedTask;
    }

    internal void MarkQueueForExpirationEviction(string address, TimeSpan? expiration)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);
        GetOrCreateQueue(address); // idempotent: ensures the audit queue exists before we sweep it

        // Normalize non-positive expirations to null (disabled). StartEvictionPump treats
        // <= TimeSpan.Zero as "do not run", so storing a non-positive value would leave the queue
        // marked in expirationMarkedQueues with no pump ever started for it. Keeping the marked
        // state and the pump state in agreement avoids that inconsistency.
        expiration = expiration is { } value && value <= TimeSpan.Zero ? null : expiration;

        expirationMarkedQueues[address] = expiration;

        if (expiration.HasValue)
        {
            StartEvictionPump(address, expiration.Value);
        }
    }

    // Each marked queue gets its own pump sweeping at THAT queue's configured expiration, so a
    // long-TTBR queue isn't churned at a short-TTBR queue's cadence and a busy queue doesn't
    // serialize behind others — both matter at production throughput. Eviction uses its own
    // cancellation tied to the broker lifetime (not the receive pump), so it also covers send-only
    // hosts and survives endpoint stop on a shared broker.
    void StartEvictionPump(string address, TimeSpan expiration)
    {
        if (expiration <= TimeSpan.Zero)
        {
            return;
        }

        lock (evictionPumpStartLock)
        {
            if (expirationEvictionPumps.ContainsKey(address))
            {
                return;
            }

            expirationEvictionPumps[address] = RunEvictionPump(address, expiration, evictionCancelSource.Token);
        }
    }

    async Task RunEvictionPump(string address, TimeSpan expiration, CancellationToken cancellationToken)
    {
        // The timer is created (and registered with the TimeProvider) in the synchronous prefix so
        // that FakeTimeProvider-backed tests can Advance time immediately after the mark returns. No
        // sweeping happens on the calling thread: the first sweep runs on the first tick, keeping the
        // start lock and the mark call chain free of drain-filter work.
        using var timer = new PeriodicTimer(expiration, timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                SweepExpiredEnvelopes(address, GetUtcNow());
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Broker disposal cancels the eviction pump as part of normal cleanup.
        }
        catch (ObjectDisposedException)
        {
            // Broker disposal cancels and disposes the token source WITHOUT awaiting this pump
            // (eviction is best-effort), so a pump still winding down may observe the disposed
            // source here. That is expected; just exit.
        }
    }

    void SweepExpiredEnvelopes(string address, DateTimeOffset now)
    {
        if (!expirationMarkedQueues.TryGetValue(address, out var expiration) || !expiration.HasValue)
        {
            return;
        }

        if (!queues.TryGetValue(address, out var queue))
        {
            return;
        }

        // Drain-filter bounded by a snapshot of the queue length, so only the backlog present at the
        // start of this sweep is processed. Survivors are re-enqueued inline (no scratch list, so no
        // per-sweep allocation), and bounding by the snapshot is what prevents us from re-draining the
        // survivors we just put back — which would otherwise loop forever on the same channel.
        // Messages enqueued by the dispatcher during the sweep are left for the next tick. The audit
        // queue has no consumer, so there is no reader race; order is not preserved — acceptable for a
        // centralized audit queue ingested in parallel.
        //
        // Completion-safe: a completed channel (broker disposal) drains its remaining items via
        // TryDequeue and then returns false (no throw), and TryWrite returns false so a survivor that
        // can't be re-enqueued is disposed, reclaiming its pooled buffer rather than throwing.
        var count = queue.Count;
#pragma warning disable CA2000 // each dequeued envelope is disposed or transferred back to the channel
        for (var i = 0; i < count; i++)
        {
            if (!queue.TryDequeue(out var envelope) || envelope is null)
            {
                break;
            }

            if (envelope.DiscardAfter.HasValue && envelope.DiscardAfter.Value <= now)
            {
                envelope.Dispose();
            }
            else if (!queue.TryEnqueue(envelope))
            {
                // A completed channel (broker shutting down) rejects writes; reclaim the pooled buffer.
                envelope.Dispose();
            }
        }
#pragma warning restore CA2000
    }

    internal bool HasEvictionPump(string address)
    {
        lock (evictionPumpStartLock)
        {
            return expirationEvictionPumps.ContainsKey(address);
        }
    }

    internal bool IsMarkedForExpirationEviction(string address) => expirationMarkedQueues.ContainsKey(address);

    bool HasSimulationFor(NonDurableSimulationOperation operation, string queue)
    {
        var resolved = ResolveSimulation(operation, queue);
        return resolved.RateLimit is not null || resolved.RateLimiter is not null || resolved.RateLimiterFactory is not null;
    }

    async Task StartDelayedMessagePump(CancellationToken cancellationToken)
    {
        while (true)
        {
            BrokerEnvelope? envelopeToDispatch;
            Task scheduleChangedTask;
            TimeSpan? waitDuration;

            lock (delayedMessagesLock)
            {
                if (TryDequeueDelayedCore(GetUtcNow(), out envelopeToDispatch))
                {
                    scheduleChangedTask = Task.CompletedTask;
                    waitDuration = null;
                }
                else
                {
                    envelopeToDispatch = null;
                    scheduleChangedTask = delayedMessagesChanged.Task;
                    waitDuration = GetNextWaitDuration(GetUtcNow());
                }
            }

            if (envelopeToDispatch != null)
            {
                try
                {
                    await ApplySimulationAsync(NonDurableSimulationOperation.DelayedDelivery, envelopeToDispatch.Destination, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    envelopeToDispatch.Dispose();
                    break;
                }
                catch (NonDurableSimulationException ex)
                {
                    EnqueueDelayed(envelopeToDispatch, ex.TimeProvider.GetUtcNow() + ex.RetryAfter);
                    if (ex.RetryAfter <= TimeSpan.Zero)
                    {
                        // A rejected simulation with no positive RetryAfter re-schedules the
                        // message due immediately, so without yielding the pump would spin in a
                        // synchronous tight loop and starve other work. Force a yield (no wall-clock
                        // delay, mirroring NonDurableMessagePump's zero-delay receive retry) so the
                        // loop can be interrupted and progress when the limiter starts granting.
                        cancellationToken.ThrowIfCancellationRequested();
                        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
                    }
                    continue;
                }

                var queue = GetOrCreateQueue(envelopeToDispatch.Destination);
                await queue.Enqueue(envelopeToDispatch, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            try
            {
                await WaitForDelayedMessagesAsync(scheduleChangedTask, waitDuration, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    async Task WaitForDelayedMessagesAsync(Task scheduleChangedTask, TimeSpan? waitDuration, CancellationToken cancellationToken)
    {
        if (waitDuration is null)
        {
            await scheduleChangedTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (waitDuration <= TimeSpan.Zero)
        {
            return;
        }

        var delayTask = Task.Delay(waitDuration.Value, timeProvider, cancellationToken);
        var completedTask = await Task.WhenAny(scheduleChangedTask, delayTask).ConfigureAwait(false);
        await completedTask.ConfigureAwait(false);
    }

    DateTimeOffset GetUtcNow() => timeProvider.GetUtcNow();

    internal DateTimeOffset GetCurrentTime() => GetUtcNow();

    async Task ApplySimulationAsync(NonDurableSimulationOperation operation, string queue, CancellationToken cancellationToken)
    {
        var resolved = ResolveSimulation(operation, queue);
        if (resolved.RateLimit is null && resolved.RateLimiter is null && resolved.RateLimiterFactory is null)
        {
            return;
        }

        if (resolved.RateLimiter != null || resolved.RateLimiterFactory != null)
        {
            await ApplyCustomRateLimiterAsync(operation, queue, resolved, cancellationToken).ConfigureAwait(false);
            return;
        }

        while (true)
        {
            var now = resolved.TimeProvider.GetUtcNow();
            var acquired = TryAcquirePermit(operation, queue, resolved.RateLimit!, now, out var retryAfter);
            if (acquired)
            {
                return;
            }

            if (resolved.Mode == NonDurableSimulationMode.Reject)
            {
                throw new NonDurableSimulationException($"In-memory {operation} simulation rejected access to queue '{queue}'.", retryAfter, resolved.TimeProvider);
            }

            await Task.Delay(retryAfter, resolved.TimeProvider, cancellationToken).ConfigureAwait(false);
        }
    }

    async Task ApplyCustomRateLimiterAsync(NonDurableSimulationOperation operation, string queue, ResolvedSimulationSettings resolved, CancellationToken cancellationToken)
    {
        var factory = resolved.RateLimiterFactory;
        var limiter = resolved.RateLimiter ?? customLimiters.GetOrAdd(
            (operation, queue),
            static (_, state) => state.RateLimiterFactory(state.TimeProvider),
            (RateLimiterFactory: factory!, resolved.TimeProvider));

        if (resolved.Mode == NonDurableSimulationMode.Reject)
        {
            using var lease = limiter.AttemptAcquire();
            if (lease.IsAcquired)
            {
                return;
            }

            throw new NonDurableSimulationException($"In-memory {operation} simulation rejected access to queue '{queue}'.", GetRetryAfter(lease), resolved.TimeProvider);
        }

        using var acquiredLease = await limiter.AcquireAsync(1, cancellationToken).ConfigureAwait(false);
        if (!acquiredLease.IsAcquired)
        {
            throw new NonDurableSimulationException($"In-memory {operation} simulation rejected access to queue '{queue}'.", GetRetryAfter(acquiredLease), resolved.TimeProvider);
        }
    }

    ResolvedSimulationSettings ResolveSimulation(NonDurableSimulationOperation operation, string queue)
    {
        options.TryGetQueue(queue, out var queueOptions);

        var brokerLevel = operation switch
        {
            NonDurableSimulationOperation.Send => options.Send,
            NonDurableSimulationOperation.Receive => options.Receive,
            NonDurableSimulationOperation.DelayedDelivery => options.DelayedDelivery,
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
        };

        var queueLevel = queueOptions is null
            ? null
            : operation switch
            {
                NonDurableSimulationOperation.Send => queueOptions.Send,
                NonDurableSimulationOperation.Receive => queueOptions.Receive,
                NonDurableSimulationOperation.DelayedDelivery => queueOptions.DelayedDelivery,
                _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
            };

        var effectiveTimeProvider = queueLevel?.TimeProvider
            ?? queueOptions?.TimeProvider
            ?? brokerLevel.TimeProvider
            ?? options.TimeProvider
            ?? TimeProvider.System;

        var effectiveRateLimit = queueLevel?.RateLimit
            ?? queueOptions?.Default.RateLimit
            ?? brokerLevel.RateLimit
            ?? options.Default.RateLimit;

        var effectiveRateLimiter = queueLevel?.RateLimiter
            ?? queueOptions?.Default.RateLimiter
            ?? brokerLevel.RateLimiter
            ?? options.Default.RateLimiter;

        var effectiveRateLimiterFactory = queueLevel?.RateLimiterFactory
            ?? queueOptions?.Default.RateLimiterFactory
            ?? brokerLevel.RateLimiterFactory
            ?? options.Default.RateLimiterFactory;

        var effectiveMode = queueLevel?.Mode
            ?? queueOptions?.Default.Mode
            ?? brokerLevel.Mode
            ?? options.Default.Mode
            ?? (effectiveRateLimit is null && effectiveRateLimiter is null && effectiveRateLimiterFactory is null ? null : NonDurableSimulationMode.Delay);

        return new ResolvedSimulationSettings(effectiveTimeProvider, effectiveMode, effectiveRateLimit, effectiveRateLimiter, effectiveRateLimiterFactory);
    }

    static TimeSpan GetRetryAfter(RateLimitLease lease)
    {
        if (lease.TryGetMetadata(MetadataName.RetryAfter.Name, out var metadata) && metadata is TimeSpan retryAfter)
        {
            return retryAfter;
        }

        return TimeSpan.Zero;
    }

    static void ValidateOptions(NonDurableBrokerOptions options)
    {
        ValidateNode(options.Default, "Default");
        ValidateNode(options.Send, "Send");
        ValidateNode(options.Receive, "Receive");
        ValidateNode(options.DelayedDelivery, "DelayedDelivery");

        foreach (var queueOptions in options.GetQueues())
        {
            ValidateNode(queueOptions.Default, "Queue.Default");
            ValidateNode(queueOptions.Send, "Queue.Send");
            ValidateNode(queueOptions.Receive, "Queue.Receive");
            ValidateNode(queueOptions.DelayedDelivery, "Queue.DelayedDelivery");
        }
    }

    static void ValidateNode(NonDurableSimulationOptions options, string nodeName)
    {
        var configuredLimiterSources = 0;
        if (options.RateLimit != null)
        {
            configuredLimiterSources++;
        }

        if (options.RateLimiter != null)
        {
            configuredLimiterSources++;
        }

        if (options.RateLimiterFactory != null)
        {
            configuredLimiterSources++;
        }

        if (configuredLimiterSources > 1)
        {
            throw new ArgumentException($"Simulation node '{nodeName}' configures multiple limiter sources. Only one of RateLimit, RateLimiter, or RateLimiterFactory may be set.");
        }

        // PermitLimit of 0 is the supported pause mechanism, but a fixed window must be strictly
        // positive: a non-positive window could otherwise synchronously loop forever while pausing.
        if (options.RateLimit is { Window: var window } && window <= TimeSpan.Zero)
        {
            throw new ArgumentException($"Simulation node '{nodeName}' must have a strictly positive Window, but was '{window}'.");
        }
    }

    bool TryAcquirePermit(NonDurableSimulationOperation operation, string queue, NonDurableRateLimitOptions rateLimit, DateTimeOffset now, out TimeSpan retryAfter)
    {
        var state = simulationState.GetOrAdd((operation, queue), static (_, now) => new WindowState(now), now);

        lock (state)
        {
            if (rateLimit.PermitLimit <= 0)
            {
                retryAfter = rateLimit.Window;
                return false;
            }

            if (rateLimit.Window <= TimeSpan.Zero)
            {
                retryAfter = TimeSpan.Zero;
                return true;
            }

            if (now - state.WindowStart >= rateLimit.Window)
            {
                state.WindowStart = now;
                state.PermitsUsed = 0;
            }

            if (state.PermitsUsed < rateLimit.PermitLimit)
            {
                state.PermitsUsed++;
                retryAfter = TimeSpan.Zero;
                return true;
            }

            var nextPermitAt = state.WindowStart + rateLimit.Window;
            retryAfter = nextPermitAt - now;
            return false;
        }
    }

    TimeSpan? GetNextWaitDuration(DateTimeOffset now)
    {
        if (delayedMessages.Count == 0)
        {
            return null;
        }

        var nextMessage = delayedMessages.Peek();
        var deliverAt = nextMessage.DeliverAt ?? now;
        return deliverAt - now;
    }

    bool TryDequeueDelayedCore(DateTimeOffset now, [NotNullWhen(true)] out BrokerEnvelope? envelope)
    {
        if (delayedMessages.Count == 0)
        {
            envelope = null;
            return false;
        }

        var peeked = delayedMessages.Peek();
        if (peeked.DeliverAt <= now)
        {
            _ = delayedMessages.Dequeue();
            envelope = peeked;
            return true;
        }

        envelope = null;
        return false;
    }

    void SignalDelayedMessagesChanged()
    {
        if (!delayedMessagesChanged.Task.IsCompleted)
        {
            _ = delayedMessagesChanged.TrySetResult();
            delayedMessagesChanged = CreateDelayedMessagesChangedSignal();
        }
    }

    static TaskCompletionSource CreateDelayedMessagesChangedSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Disposes the broker, completing all queues and stopping the delayed message pump. Any buffered messages will be lost.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await delayedPumpCancelSource.CancelAsync().ConfigureAwait(false);
        SignalDelayedMessagesChanged();
        if (delayedPumpTask != null)
        {
            try
            {
                await delayedPumpTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (delayedPumpCancelSource.IsCancellationRequested)
            {
                // Intentionally ignored: broker disposal cancels the delayed pump as part of normal cleanup.
            }
        }

        // Eviction is best-effort and the sweep is completion-safe (TryDequeue/TryWrite never throw
        // on a completed channel), so we cancel the pumps but do NOT await them — disposal must not
        // wait for throwaway eviction work. The pumps exit promptly on cancellation; a pump still
        // winding down when the token source is disposed is handled by RunEvictionPump's
        // ObjectDisposedException catch. (The delayed pump above is different: it uses WriteAsync,
        // which throws on a completed channel, so it MUST be awaited before TryComplete.)
        await evictionCancelSource.CancelAsync().ConfigureAwait(false);

        DisposeDelayedMessages();

        // Completing queues lets receivers drain any buffered envelopes and then exit cleanly.
        foreach (var queue in queues.Values)
        {
            queue.TryComplete();
        }

        foreach (var limiter in customLimiters.Values)
        {
            await limiter.DisposeAsync().ConfigureAwait(false);
        }

        customLimiters.Clear();
        delayedPumpCancelSource.Dispose();
        evictionCancelSource.Dispose();
    }

    void DisposeDelayedMessages()
    {
        lock (delayedMessagesLock)
        {
            while (delayedMessages.Count > 0)
            {
                delayedMessages.Dequeue().Dispose();
            }
        }
    }

    readonly ConcurrentDictionary<string, NonDurableChannel> queues = new();
    readonly ConcurrentDictionary<string, TimeSpan?> expirationMarkedQueues = new();
    readonly Dictionary<string, Task> expirationEvictionPumps = [];
    readonly Lock evictionPumpStartLock = new();
    CancellationTokenSource evictionCancelSource = new();
    readonly ConcurrentDictionary<string, Lazy<string[]>> subscriptions = new();
    readonly PriorityQueue<BrokerEnvelope, (DateTimeOffset DeliverAt, long SequenceNumber)> delayedMessages = new();
    readonly Lock delayedMessagesLock = new();
    long sequenceNumber;
    int pumpStarted;
    CancellationTokenSource delayedPumpCancelSource = new();
    Task? delayedPumpTask;
    TaskCompletionSource delayedMessagesChanged = CreateDelayedMessagesChangedSignal();
    readonly TimeProvider timeProvider;
    readonly NonDurableBrokerOptions options;
    readonly ConcurrentDictionary<(NonDurableSimulationOperation Operation, string Queue), WindowState> simulationState = new();
    readonly ConcurrentDictionary<(NonDurableSimulationOperation Operation, string Queue), RateLimiter> customLimiters = new();

    sealed class WindowState(DateTimeOffset windowStart)
    {
        public DateTimeOffset WindowStart { get; set; } = windowStart;

        public int PermitsUsed { get; set; }
    }

    readonly record struct ResolvedSimulationSettings(TimeProvider TimeProvider, NonDurableSimulationMode? Mode, NonDurableRateLimitOptions? RateLimit, RateLimiter? RateLimiter, Func<TimeProvider, RateLimiter>? RateLimiterFactory);

    enum NonDurableSimulationOperation
    {
        Send,
        Receive,
        DelayedDelivery
    }
}
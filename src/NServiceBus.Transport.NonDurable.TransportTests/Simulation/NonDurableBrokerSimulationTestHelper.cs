namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using NServiceBus.Routing;
using NServiceBus.Transport;

static class NonDurableBrokerSimulationTestHelper
{
    public static async Task<IMessageDispatcher> CreateDispatcher(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var infrastructure = await CreateInfrastructure(broker, cancellationToken);
        return infrastructure.Dispatcher;
    }

    public static async Task<IMessageReceiver> CreateReceiver(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var infrastructure = await CreateInfrastructure(broker, cancellationToken);
        return infrastructure.Receivers["main"];
    }

    public static Task<TransportInfrastructure> CreateInfrastructure(NonDurableBroker broker, CancellationToken cancellationToken = default)
    {
        var transport = new NonDurableTransport(new NonDurableTransportOptions(broker));
        return transport.Initialize(
            new HostSettings("endpoint", string.Empty, new StartupDiagnosticEntries(), static (_, _, _) => { }, true),
            [new ReceiveSettings("main", new QueueAddress("input"), false, true, "error")],
            ["error"],
            cancellationToken);
    }

    public static Task Dispatch(IMessageDispatcher dispatcher, string messageId, string destination, CancellationToken cancellationToken = default)
    {
        var message = new OutgoingMessage(messageId, [], new byte[] { 1 });
        var operation = new TransportOperation(message, new UnicastAddressTag(destination));
        return dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction(), cancellationToken);
    }

    public static BrokerEnvelope CreateEnvelope(string messageId, string destination, long sequenceNumber)
    {
        return BrokerPayloadStore.Borrow(messageId, new byte[] { 1 }, new Dictionary<string, string>(), destination, false, sequenceNumber);
    }
}

sealed class CountingRateLimiter(int permitCount) : RateLimiter
{
    int remainingPermits = permitCount;

    public int AttemptAcquireCalls { get; private set; }

    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        AttemptAcquireCalls++;
        if (remainingPermits > 0)
        {
            remainingPermits--;
            return FixedRateLimitLease.Granted;
        }

        return FixedRateLimitLease.Rejected;
    }

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
    {
        AttemptAcquireCalls++;
        if (remainingPermits > 0)
        {
            remainingPermits--;
            return ValueTask.FromResult<RateLimitLease>(FixedRateLimitLease.Granted);
        }

        return ValueTask.FromResult<RateLimitLease>(FixedRateLimitLease.Rejected);
    }

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override void Dispose(bool disposing)
    {
    }
}

sealed class ScriptedRateLimiter(params ScriptedRateLimiterStep[] steps) : RateLimiter
{
    readonly Queue<ScriptedRateLimiterStep> scriptedSteps = new(steps);

    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        if (scriptedSteps.Count == 0)
        {
            throw new InvalidOperationException("No scripted limiter steps remain.");
        }

        return scriptedSteps.Dequeue().ToLease();
    }

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(AttemptAcquireCore(permitCount));

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override void Dispose(bool disposing)
    {
    }
}

readonly record struct ScriptedRateLimiterStep(bool IsAcquired, TimeSpan? RetryAfter)
{
    public static ScriptedRateLimiterStep Acquired() => new(true, null);

    public static ScriptedRateLimiterStep Rejected(TimeSpan retryAfter) => new(false, retryAfter);

    public RateLimitLease ToLease() => IsAcquired
        ? FixedRateLimitLease.Granted
        : FixedRateLimitLease.RejectedWithRetryAfter(RetryAfter!.Value);
}

// A RateLimitLease with a fixed acquisition result and optional RetryAfter metadata. Collapses the
// per-limiter granted/rejected lease duplicates into shared singletons (Granted/Rejected) plus a
// retry-after variant (RejectedWithRetryAfter) for rejections that carry RetryAfter metadata.
sealed class FixedRateLimitLease : RateLimitLease
{
    public static FixedRateLimitLease Granted { get; } = new(true);

    public static FixedRateLimitLease Rejected { get; } = new(false);

    readonly bool isAcquired;
    readonly TimeSpan? retryAfter;

    FixedRateLimitLease(bool isAcquired, TimeSpan? retryAfter = null)
    {
        this.isAcquired = isAcquired;
        this.retryAfter = retryAfter;
    }

    public static FixedRateLimitLease RejectedWithRetryAfter(TimeSpan retryAfter) => new(false, retryAfter);

    public override bool IsAcquired => isAcquired;

    public override IEnumerable<string> MetadataNames => retryAfter.HasValue ? [MetadataName.RetryAfter.Name] : [];

    public override bool TryGetMetadata(string metadataName, out object metadata)
    {
        if (retryAfter.HasValue && metadataName == MetadataName.RetryAfter.Name)
        {
            metadata = retryAfter.Value;
            return true;
        }

        metadata = null;
        return false;
    }
}

// Rejects every attempt (with no RetryAfter metadata, so the broker treats it as
// TimeSpan.Zero) until StartGranting() flips it to always grant. Used to assert that a
// rejected queued message stays unprocessed and is only processed once permits appear.
sealed class ManualGrantRateLimiter : RateLimiter
{
    int granting;
    int attempts;

    public int Attempts => Volatile.Read(ref attempts);

    public void StartGranting() => Volatile.Write(ref granting, 1);

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        Interlocked.Increment(ref attempts);
        return Volatile.Read(ref granting) == 1 ? FixedRateLimitLease.Granted : FixedRateLimitLease.Rejected;
    }

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(AttemptAcquireCore(permitCount));

    protected override void Dispose(bool disposing)
    {
    }
}

// A send/receive simulation limiter whose AcquireAsync blocks until Grant() is called, then returns a
// granted lease. Used to model a send delay deterministically: Acquired signals when the broker first
// asks for a permit, and Grant() releases the delayed operation. Unlike the built-in window limiter, the
// delay does not depend on a FakeTimeProvider timer fire, so it is fully observable and race-free.
sealed class BlockingGrantRateLimiter : RateLimiter
{
    readonly TaskCompletionSource<RateLimitLease> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskCompletionSource acquired = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Acquired => acquired.Task;

    public void Grant() => pending.TrySetResult(FixedRateLimitLease.Granted);

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override RateLimitLease AttemptAcquireCore(int permitCount) => FixedRateLimitLease.Rejected;

    protected override async ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
    {
        acquired.TrySetResult();
        using var registration = cancellationToken.Register(() => pending.TrySetCanceled(cancellationToken));
        return await pending.Task.ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
    }
}

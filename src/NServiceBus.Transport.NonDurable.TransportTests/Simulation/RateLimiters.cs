namespace NServiceBus.TransportTests.Simulation;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;

sealed class CountingRateLimiter(int permitCount) : RateLimiter
{
    int remainingPermits = permitCount;

    int attemptAcquireCalls;

    public int AttemptAcquireCalls => Volatile.Read(ref attemptAcquireCalls);

    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        _ = Interlocked.Increment(ref attemptAcquireCalls);
        if (Interlocked.Decrement(ref remainingPermits) >= 0)
        {
            return FixedRateLimitLease.Granted;
        }

        _ = Interlocked.Increment(ref remainingPermits);
        return FixedRateLimitLease.Rejected;

    }

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(AttemptAcquireCore(permitCount));

    public override TimeSpan? IdleDuration => null;

    public override RateLimiterStatistics GetStatistics() => null;

    protected override void Dispose(bool disposing)
    {
    }
}

sealed class ScriptedRateLimiter(params ScriptedRateLimiterStep[] steps) : RateLimiter
{
    readonly ConcurrentQueue<ScriptedRateLimiterStep> scriptedSteps = new(steps);

    protected override RateLimitLease AttemptAcquireCore(int permitCount)
    {
        if (scriptedSteps.IsEmpty || !scriptedSteps.TryDequeue(out var step))
        {
            throw new InvalidOperationException("No scripted limiter steps remain.");
        }

        return step.ToLease();
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

sealed class FixedRateLimitLease : RateLimitLease
{
    public static FixedRateLimitLease Granted { get; } = new(true);

    public static FixedRateLimitLease Rejected { get; } = new(false);

    readonly TimeSpan? retryAfter;

    FixedRateLimitLease(bool isAcquired, TimeSpan? retryAfter = null)
    {
        IsAcquired = isAcquired;
        this.retryAfter = retryAfter;
    }

    public static FixedRateLimitLease RejectedWithRetryAfter(TimeSpan retryAfter) => new(false, retryAfter);

    public override bool IsAcquired { get; }

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
        _ = Interlocked.Increment(ref attempts);
        return Volatile.Read(ref granting) == 1 ? FixedRateLimitLease.Granted : FixedRateLimitLease.Rejected;
    }

    protected override ValueTask<RateLimitLease> AcquireAsyncCore(int permitCount, CancellationToken cancellationToken = default)
        => ValueTask.FromResult(AttemptAcquireCore(permitCount));

    protected override void Dispose(bool disposing)
    {
    }
}

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
        _ = acquired.TrySetResult();
        await using var registration = cancellationToken.Register(() => pending.TrySetCanceled(cancellationToken));
        return await pending.Task.ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
    }
}
namespace NServiceBus;

using System.Threading;

/// <summary>
/// Controls how the non-durable endpoint will shut down. Because the transport is in memory and messages
/// left in the queue will likely be discarded (unless another endpoint connects to the same queue on the
/// <see cref="NonDurableBroker" /> later) the default <see cref="DrainQueueBeforeShutdown" /> behavior
/// deviates from other (durable) message transports in order to provide the greatest amount of safety by
/// default. The <see cref="ShutdownAfterHandlerExit" /> provides a shutdown behavior that is more similar
/// to durable message transports, but could result in message loss.
/// </summary>
public enum NonDurableTransportShutdownBehavior
{
    /// <summary>
    /// <para>
    /// The default option implements a graceful shutdown that attempts to drain the in-memory queue until the
    /// queue is empty. If the <see cref="CancellationToken" /> is triggered, cancellation signals will be
    /// propagated to all message handlers.
    /// </para>
    /// <para>
    /// This behavior deviates from the other transports, but ensures that any in-progress multi-message flows
    /// are given a chance to complete before the endpoint shuts down.
    /// </para>
    /// <para>
    /// However, this also means that if the queue will never empty, the endpoint will be unable to shut down until
    /// the cancellation token is signaled. Or, if the Stop method uses <see cref="CancellationToken.None" />, the
    /// endpoint will never shut down.  This could happen, for example, if a message handler always sends a new
    /// message to the queue in a loop.
    /// </para>
    /// </summary>
    DrainQueueBeforeShutdown = 0,

    /// <summary>
    /// <para>
    /// In this mode, which more closely resembles durable message transports, the endpoint will attempt to allow each
    /// currently in-flight message to complete, but will not start processing any new messages.
    /// </para>
    /// <para>
    /// If a <see cref="CancellationToken" /> is provided to the Stop method and it signals cancellation, the in-flight
    /// message handlers will be interrupted to force the endpoint to stop faster.
    /// </para>
    /// <para>
    /// Buffered messages remain in the queue on shutdown. They are only processed if the endpoint is started again
    /// (for example via <c>ChangeConcurrency</c> or a restart); otherwise they are lost when the <see cref="NonDurableBroker" />
    /// is disposed. This also means that an inline-execution root dispatch whose cascade has buffered-but-unprocessed
    /// messages will not reach a terminal outcome on shutdown, so its dispatch task may stay incomplete until the
    /// endpoint restarts. Use <see cref="DrainQueueBeforeShutdown" /> if those cascades must complete before shutdown returns.
    /// </para>
    /// </summary>
    ShutdownAfterHandlerExit = 1,
}
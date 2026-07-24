namespace NServiceBus;

using System.Threading;

/// <summary>
/// Controls how the non-durable endpoint will shut down. Because the transport is in memory and messages
/// left in the queue will likely be discarded (unless another endpoint connects to the same queue on the
/// <see cref="NonDurableBroker" /> later) the default <see cref="DrainQueueBeforeShutdown" /> behavior
/// deviates from other (durable) message transports to provide the greatest amount of safety by
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
    /// This behavior deviates from the other transports but ensures that any in-progress multi-message flows
    /// are given a chance to complete before the endpoint shuts down.
    /// </para>
    /// <para>
    /// However, this also means that if the queue never empties, the endpoint will be unable to shut down until
    /// the cancellation token is signaled. Or, if the Stop method uses <see cref="CancellationToken.None" />, the
    /// endpoint will never shut down. This could happen, for example, if a message handler always sends a new
    /// message to the queue in a loop.
    /// </para>
    /// </summary>
    DrainQueueBeforeShutdown = 0,

    /// <summary>
    /// <para>
    /// In this mode, which more closely resembles durable message transports, the endpoint allows message pipelines
    /// admitted before shutdown to complete but does not admit additional queued or inline message pipelines.
    /// A message already fetched from the queue when shutdown begins is considered in-flight.
    /// </para>
    /// <para>
    /// An inline operation attempted after its destination receiver begins stopping is rejected. The originating
    /// parent message then follows its configured recoverability policy. If recoverability requests a retry, the
    /// parent message is requeued and remains buffered until processing restarts.
    /// </para>
    /// <para>
    /// If a <see cref="CancellationToken" /> is provided to the Stop method, and it signals cancellation, the in-flight
    /// message handlers will be interrupted to force the endpoint to stop faster.
    /// </para>
    /// <para>
    /// Buffered messages remain in the queue on shutdown. They can be processed if the same receiver starts again,
    /// for example, through <c>ChangeConcurrency</c>; otherwise they are lost when the <see cref="NonDurableBroker" />
    /// is disposed. A new endpoint using the same broker can process a buffered message, but it cannot complete an
    /// inline-execution dispatch task owned by the previous endpoint instance. Use <see cref="DrainQueueBeforeShutdown" />
    /// if inline cascades must be given an opportunity to complete before shutdown returns.
    /// </para>
    /// </summary>
    ShutdownAfterHandlerExit = 1,
}
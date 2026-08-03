namespace NServiceBus;

/// <summary>
/// Specifies how a simulated constraint affects an operation when no permit is available.
/// </summary>
public enum NonDurableSimulationMode
{
    /// <summary>
    /// Wait until the operation can obtain a permit.
    /// </summary>
    Delay,

    /// <summary>
    /// Reject the operation and report when it may be retried.
    /// </summary>
    Reject
}
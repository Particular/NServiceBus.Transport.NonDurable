namespace NServiceBus;

using System;

public sealed class NonDurableRateLimitOptions
{
    public required int PermitLimit { get; init; }

    public required TimeSpan Window { get; init; }
}

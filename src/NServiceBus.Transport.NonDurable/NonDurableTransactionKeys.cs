namespace NServiceBus;

static class NonDurableTransactionKeys
{
    // Cross-repository contract with NServiceBus.Persistence.NonDurable.
    public const string Transaction = "NServiceBus.NonDurable.Transaction";
}
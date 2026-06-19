namespace NServiceBus;

using System.Collections.Generic;
using System.Threading;
using System.Transactions;

sealed class PendingEnvelopeEnlistment : IEnlistmentNotification
{
    public bool Add(BrokerEnvelope envelope)
    {
        if (Volatile.Read(ref completed) != 0)
        {
            return false;
        }
        pendingEnvelopes.Add(envelope);
        return true;
    }

    public IReadOnlyList<BrokerEnvelope> GetPendingAndClear()
    {
        var result = pendingEnvelopes;
        pendingEnvelopes = [];
        return result;
    }

    public void Prepare(PreparingEnlistment preparingEnlistment) => preparingEnlistment.Prepared();

    public void Commit(Enlistment enlistment)
    {
        MarkAsCompleted();
        enlistment.Done();
    }

    public void Rollback(Enlistment enlistment)
    {
        DisposeAndClear();
        enlistment.Done();
    }

    public void InDoubt(Enlistment enlistment)
    {
        DisposeAndClear();
        enlistment.Done();
    }

    void DisposeAndClear()
    {
        MarkAsCompleted();
        foreach (var envelope in pendingEnvelopes)
        {
            envelope.Dispose();
        }
        pendingEnvelopes.Clear();
    }

    void MarkAsCompleted() => Volatile.Write(ref completed, 1);

    internal IReadOnlyList<BrokerEnvelope> GetPendingEnvelopesForTesting() => [.. pendingEnvelopes];

    List<BrokerEnvelope> pendingEnvelopes = [];
    int completed;
}
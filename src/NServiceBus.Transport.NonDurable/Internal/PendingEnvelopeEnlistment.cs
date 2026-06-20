namespace NServiceBus;

using System.Collections.Generic;
using System.Threading;
using System.Transactions;

sealed class PendingEnvelopeEnlistment : IEnlistmentNotification
{
    public bool Add(BrokerEnvelope envelope)
    {
        lock (gate)
        {
            if (completed != 0)
            {
                return false;
            }
            pendingEnvelopes.Add(envelope);
            return true;
        }
    }

    public IReadOnlyList<BrokerEnvelope> GetPendingAndClear()
    {
        lock (gate)
        {
            var result = pendingEnvelopes;
            pendingEnvelopes = [];
            return result;
        }
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
        List<BrokerEnvelope> toDispose;
        lock (gate)
        {
            toDispose = pendingEnvelopes;
            pendingEnvelopes = [];
        }
        foreach (var envelope in toDispose)
        {
            envelope.Dispose();
        }
    }

    void MarkAsCompleted() => Volatile.Write(ref completed, 1);

    internal IReadOnlyList<BrokerEnvelope> GetPendingEnvelopesForTesting()
    {
        lock (gate)
        {
            return [.. pendingEnvelopes];
        }
    }

    readonly Lock gate = new();
    List<BrokerEnvelope> pendingEnvelopes = [];
    int completed;
}
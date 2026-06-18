namespace NServiceBus;

using System.Collections.Generic;

interface INonDurableReceiveTransaction
{
    void Enlist(BrokerEnvelope envelope);
    IReadOnlyList<BrokerEnvelope> GetPendingAndClear();
    void Commit();
    void Rollback();
}

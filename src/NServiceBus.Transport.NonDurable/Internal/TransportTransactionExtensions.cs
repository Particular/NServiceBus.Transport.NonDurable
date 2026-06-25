namespace NServiceBus;

using Transport;

static class TransportTransactionExtensions
{
    public static bool IsInsideReceivePipeline(this TransportTransaction transaction) =>
        transaction.TryGet<ReceivePipelineTransportTransactionMarker>(out _);
}
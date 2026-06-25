namespace NServiceBus;

sealed class ReceivePipelineTransportTransactionMarker
{
    public static readonly ReceivePipelineTransportTransactionMarker Instance = new();

    ReceivePipelineTransportTransactionMarker() { }
}
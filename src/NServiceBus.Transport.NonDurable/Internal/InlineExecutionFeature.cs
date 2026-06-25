namespace NServiceBus;

using Features;
using Transport;

sealed class InlineExecutionFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        var transportDefinition = context.Settings.Get<TransportDefinition>();
        var nonDurableTransport = (NonDurableTransport)transportDefinition;
        var settings = nonDurableTransport.InlineExecutionSettings;

        if (!settings.IsEnabled)
        {
            return;
        }

        context.Pipeline.Register(new InlineExecutionRecoverabilityBehavior(settings), "Overrides recoverability actions based on configuration.");
    }
}
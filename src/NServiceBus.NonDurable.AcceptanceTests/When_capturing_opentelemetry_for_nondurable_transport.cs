namespace NServiceBus.AcceptanceTests.Transport;

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AcceptanceTesting;
using EndpointTemplates;
using NUnit.Framework;

[NonParallelizable]
public class When_capturing_opentelemetry_for_nondurable_transport : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_surface_transport_and_core_spans_in_a_single_chain()
    {
        using var listener = new AcceptanceActivityListener("NServiceBus.Core", "NServiceBus.NonDurable");

        var context = await Scenario.Define<Context>()
            .WithEndpoint<Endpoint>(builder => builder.When(session => session.SendLocal(new StartMessage())))
            .Done(c => c.MessageHandled)
            .Run();

        Assert.That(context.MessageHandled, Is.True);

        var transportSendActivities = listener.CompletedActivities.Where(activity => activity.Source.Name == "NServiceBus.NonDurable" && activity.OperationName == "NServiceBus.NonDurable.Send").ToList();
        var transportProcessActivities = listener.CompletedActivities.Where(activity => activity.Source.Name == "NServiceBus.NonDurable" && activity.OperationName == "NServiceBus.NonDurable.Process").ToList();
        var coreReceiveActivities = listener.CompletedActivities.Where(activity => activity.Source.Name == "NServiceBus.Core" && activity.OperationName == "NServiceBus.Diagnostics.ReceiveMessage").ToList();
        var handlerActivities = listener.CompletedActivities.Where(activity => activity.Source.Name == "NServiceBus.Core" && activity.OperationName == "NServiceBus.Diagnostics.InvokeHandler").ToList();

        var transportProcess = transportProcessActivities.SingleOrDefault(process => transportSendActivities.Any(send => process.ParentId == send.Id));
        var coreReceive = coreReceiveActivities.SingleOrDefault(receive => receive.ParentId == transportProcess?.Id);
        var handler = handlerActivities.SingleOrDefault(candidate => candidate.ParentId == coreReceive?.Id);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(transportProcess, Is.Not.Null, "expected a NonDurable process span parented by a NonDurable send span");
            Assert.That(coreReceive, Is.Not.Null, "expected a core receive span parented by the NonDurable process span");
            Assert.That(handler, Is.Not.Null, "expected a handler span parented by the core receive span");
            Assert.That(transportProcess!.GetTagItem("messaging.system"), Is.EqualTo("nondurable"));
            Assert.That(transportProcess.GetTagItem("messaging.operation.name"), Is.EqualTo("process"));
            Assert.That(transportProcess.Events.Any(e => e.Name == "nondurable.handoff"), Is.True);
        }
    }

    public class Context : ScenarioContext
    {
        public bool MessageHandled { get; set; }
    }

    public class Endpoint : EndpointConfigurationBuilder
    {
        public Endpoint() => EndpointSetup<DefaultServer>();

        [Handler]
        public class Handler(Context scenarioContext) : IHandleMessages<StartMessage>
        {
            public Task Handle(StartMessage message, IMessageHandlerContext context)
            {
                scenarioContext.MessageHandled = true;
                return Task.CompletedTask;
            }
        }
    }

    public class StartMessage : IMessage;

    sealed class AcceptanceActivityListener : IDisposable
    {
        readonly ActivityListener activityListener;
        readonly string[] sourceNames;

        public AcceptanceActivityListener(params string[] sourceNames)
        {
            this.sourceNames = sourceNames;
            activityListener = new ActivityListener
            {
                ShouldListenTo = source => this.sourceNames.Contains(source.Name),
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllData
            };
            activityListener.ActivityStopped += activity => CompletedActivities.Enqueue(activity);

            ActivitySource.AddActivityListener(activityListener);
        }

        public ConcurrentQueue<Activity> CompletedActivities { get; } = new();

        public void Dispose() => activityListener.Dispose();
    }
}

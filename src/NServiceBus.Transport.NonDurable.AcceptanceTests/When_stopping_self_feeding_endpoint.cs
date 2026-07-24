#nullable enable

namespace NServiceBus.AcceptanceTests;

using System;
using System.Threading;
using System.Threading.Tasks;
using AcceptanceTesting;
using EndpointTemplates;
using Microsoft.Extensions.DependencyInjection;
using Conventions = AcceptanceTesting.Customization.Conventions;
using NUnit.Framework;

public class When_stopping_self_feeding_endpoint : NServiceBusAcceptanceTest
{
    [Test]
    public async Task ShutdownAfterHandlerExit_should_stop_without_cancelling()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<Endpoint>(builder => builder
                .ServiceResolve((provider, scenarioContext, _) =>
                {
                    scenarioContext.StopEndpoint = provider.GetRequiredKeyedService<Func<CancellationToken, Task>>("Stopper");
                    return Task.CompletedTask;
                }, afterStart: true)
                .When(async (session, scenarioContext) =>
                {
                    await session.SendLocal(new Tick());
                    await scenarioContext.HandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

                    var stopTask = scenarioContext.StopEndpoint!(CancellationToken.None);
                    try
                    {
                        Assert.That(stopTask.IsCompleted, Is.False, "Stop should wait for the in-flight handler.");
                        scenarioContext.ReleaseHandler.TrySetResult();
                        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));
                        scenarioContext.MarkAsCompleted();
                    }
                    catch (TimeoutException)
                    {
                        Volatile.Write(ref scenarioContext.ContinueFeeding, false);
                        throw;
                    }
                    finally
                    {
                        scenarioContext.ReleaseHandler.TrySetResult();
                    }
                }))
            .Run();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(context.HandlerInvocations, Is.EqualTo(1));
            Assert.That(CurrentBroker.GetOrCreateQueue(Conventions.EndpointNamingConvention(typeof(Endpoint))).Count, Is.EqualTo(1));
        }
    }

    public class Context : ScenarioContext
    {
        public Func<CancellationToken, Task>? StopEndpoint { get; set; }
        public TaskCompletionSource HandlerStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseHandler { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int HandlerInvocations;
        public bool ContinueFeeding = true;
    }

    public class Tick : ICommand;

    public class Endpoint : EndpointConfigurationBuilder
    {
        public Endpoint() => EndpointSetup<DefaultServer>(builder =>
        {
            builder.UseTransport(new NonDurableTransport(new NonDurableTransportOptions
            {
                ShutdownBehavior = NonDurableTransportShutdownBehavior.ShutdownAfterHandlerExit
            })
            {
                TransportTransactionMode = TransportTransactionMode.SendsAtomicWithReceive
            });
            builder.LimitMessageProcessingConcurrencyTo(1);
        });

        [Handler]
        public class TickHandler(Context scenarioContext) : IHandleMessages<Tick>
        {
            public async Task Handle(Tick message, IMessageHandlerContext context)
            {
                Interlocked.Increment(ref scenarioContext.HandlerInvocations);
                scenarioContext.HandlerStarted.TrySetResult();
                await scenarioContext.ReleaseHandler.Task.WaitAsync(context.CancellationToken);
                if (Volatile.Read(ref scenarioContext.ContinueFeeding))
                {
                    await context.SendLocal(new Tick());
                }
            }
        }
    }
}

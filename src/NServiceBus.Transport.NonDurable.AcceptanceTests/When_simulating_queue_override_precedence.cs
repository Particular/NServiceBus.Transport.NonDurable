using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using NServiceBus.AcceptanceTesting;
using NServiceBus.AcceptanceTests.EndpointTemplates;
using NUnit.Framework;

namespace NServiceBus.AcceptanceTests;

public class When_simulating_queue_override_precedence : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_honor_queue_override_precedence_through_endpoint_surface()
    {
        var simulatedTime = new FakeTimeProvider(new DateTimeOffset(2026, 03, 28, 12, 0, 0, TimeSpan.Zero));
        var options = new NonDurableBrokerOptions
        {
            TimeProvider = simulatedTime,
            Send =
            {
                RateLimit = new NonDurableRateLimitOptions
                {
                    PermitLimit = 1,
                    Window = TimeSpan.FromSeconds(30)
                }
            }
        };
        options.ForQueue(QueueOverrideEndpoint.EndpointName).Send.RateLimit = new NonDurableRateLimitOptions
        {
            PermitLimit = 2,
            Window = TimeSpan.FromSeconds(30)
        };

        var broker = new NonDurableBroker(options);

        await using var _ = broker;
        var result = await Scenario.Define<Context>()
            .WithServices(services =>
            {
                services.AddSingleton(broker);
                services.AddSingleton(simulatedTime);
            })
            .WithEndpoint<QueueOverrideEndpoint>(builder => builder.When(async session =>
            {
                await session.SendLocal(new QueueOverrideMessage());
                await session.SendLocal(new QueueOverrideMessage());
                await session.SendLocal(new QueueOverrideMessage());
            }))
            .Run();

        using (Assert.EnterMultipleScope())
        {
            Assert.That(result.QueueOverrideDeliveredCount, Is.EqualTo(3));
            Assert.That(result.SecondQueueOverrideDeliveredAt - result.FirstQueueOverrideDeliveredAt, Is.EqualTo(TimeSpan.Zero));
            Assert.That(result.ThirdQueueOverrideDeliveredAt - result.SecondQueueOverrideDeliveredAt, Is.EqualTo(TimeSpan.FromSeconds(30)));
        }
    }

    public class Context : ScenarioContext
    {
        public int QueueOverrideDeliveredCount;

        public DateTimeOffset FirstQueueOverrideDeliveredAt { get; set; }

        public DateTimeOffset SecondQueueOverrideDeliveredAt { get; set; }

        public DateTimeOffset ThirdQueueOverrideDeliveredAt { get; set; }

        public void MaybeCompleted() => MarkAsCompleted(QueueOverrideDeliveredCount >= 3);
    }

    public class QueueOverrideEndpoint : EndpointConfigurationBuilder
    {
        public const string EndpointName = "queue-override-endpoint";

        public QueueOverrideEndpoint()
        {
            EndpointSetup<DefaultServer>((configure, _) => configure.LimitMessageProcessingConcurrencyTo(1));
            CustomEndpointName(EndpointName);
        }

        [Handler]
        public class Handler(Context testContext, FakeTimeProvider simulatedTime) : IHandleMessages<QueueOverrideMessage>
        {
            public Task Handle(QueueOverrideMessage message, IMessageHandlerContext context)
            {
                var count = Interlocked.Increment(ref testContext.QueueOverrideDeliveredCount);
                if (count == 1)
                {
                    testContext.FirstQueueOverrideDeliveredAt = simulatedTime.GetUtcNow();
                }
                else if (count == 2)
                {
                    testContext.SecondQueueOverrideDeliveredAt = simulatedTime.GetUtcNow();
                    simulatedTime.Advance(TimeSpan.FromSeconds(30));
                }
                else if (count == 3)
                {
                    testContext.ThirdQueueOverrideDeliveredAt = simulatedTime.GetUtcNow();
                }

                testContext.MaybeCompleted();

                return Task.CompletedTask;
            }
        }
    }

    public class QueueOverrideMessage : IMessage;
}

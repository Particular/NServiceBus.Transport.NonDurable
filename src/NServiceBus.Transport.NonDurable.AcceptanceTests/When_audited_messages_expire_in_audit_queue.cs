using System;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.AcceptanceTesting;
using NServiceBus.AcceptanceTests.EndpointTemplates;
using NUnit.Framework;

namespace NServiceBus.AcceptanceTests;

public class When_audited_messages_expire_in_audit_queue : NServiceBusAcceptanceTest
{
    [Test]
    [CancelAfter(15_000)]
    public async Task Should_evict_audited_messages_from_the_consumerless_audit_queue(CancellationToken cancellationToken)
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<Endpoint>(builder => builder.When(session => session.SendLocal(new StartMessage())))
            .Run(cancellationToken);

        Assert.That(context.Handled, Is.True);

        // The audit queue has no consumer, so the audited message accumulates there until the
        // broker-owned eviction pump (driven by the audit TTBR) removes it. Track that the queue
        // became non-empty at least once so the assertion below cannot pass on a queue that never
        // received an audited message.
        var observedNonEmpty = false;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (CurrentBroker.TryGetQueue("audit", out var queue))
            {
                if (queue.Count > 0)
                {
                    observedNonEmpty = true;
                }
                else if (observedNonEmpty)
                {
                    // The queue was populated and has since been drained by eviction.
                    break;
                }
            }

            await Task.Delay(100, cancellationToken);
        }

        Assert.That(observedNonEmpty, Is.True, "The audit queue never received an audited message to evict.");
        Assert.That(CurrentBroker.TryGetQueue("audit", out var auditQueue), Is.True);
        Assert.That(auditQueue!.Count, Is.EqualTo(0));
    }

    public class Context : ScenarioContext
    {
        public bool Handled { get; set; }
    }

    public class Endpoint : EndpointConfigurationBuilder
    {
        public Endpoint() => EndpointSetup<DefaultServer>((config, _) =>
        {
            config.AuditProcessedMessagesTo("audit", TimeSpan.FromSeconds(2));
        });

        [Handler]
        public class StartMessageHandler(Context testContext) : IHandleMessages<StartMessage>
        {
            public Task Handle(StartMessage message, IMessageHandlerContext context)
            {
                testContext.Handled = true;
                testContext.MarkAsCompleted();
                return Task.CompletedTask;
            }
        }
    }

    public class StartMessage : IMessage;
}

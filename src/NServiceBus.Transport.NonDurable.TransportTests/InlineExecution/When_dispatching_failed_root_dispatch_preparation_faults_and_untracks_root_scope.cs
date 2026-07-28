#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;
using Simulation;

[TestFixture]
public class When_dispatching_failed_root_dispatch_preparation_faults_and_untracks_root_scope
{
    [Test]
    public async Task Should_fault_and_untrack_root_scope_when_root_dispatch_preparation_fails()
    {
        var options = new NonDurableBrokerOptions();
        options.ForQueue("input").Send.Mode = NonDurableSimulationMode.Reject;
        options.ForQueue("input").Send.RateLimiter = new ScriptedRateLimiter(
            ScriptedRateLimiterStep.Rejected(TimeSpan.FromMinutes(1)));

        await using var broker = new NonDurableBroker(options);
        var infrastructure = await CreateInfrastructure(broker, ["input"]);
        var dispatcher = infrastructure.Dispatcher;

        var task = dispatcher.Dispatch(new TransportOperations(CreateUnicast("input")), new TransportTransaction());

        Assert.That(async () => await task.WaitAsync(TimeSpan.FromSeconds(5)), Throws.TypeOf<NonDurableSimulationException>());
    }
}

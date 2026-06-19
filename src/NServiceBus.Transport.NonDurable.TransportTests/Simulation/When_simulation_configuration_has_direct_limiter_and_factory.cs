namespace NServiceBus.TransportTests.Simulation;

using System;
using NUnit.Framework;

[TestFixture]
public class When_simulation_configuration_has_direct_limiter_and_factory
{
    [Test]
    public void Should_fail_when_direct_limiter_and_factory_are_both_configured()
    {
        using var limiter = new CountingRateLimiter(permitCount: 1);

        var exception = Assert.Throws<ArgumentException>(() => new NonDurableBroker(new NonDurableBrokerOptions
        {
            Send =
            {
                RateLimiter = limiter,
                RateLimiterFactory = _ => new CountingRateLimiter(permitCount: 1)
            }
        }));

        Assert.That(exception!.Message, Does.Contain("RateLimiterFactory"));
    }
}

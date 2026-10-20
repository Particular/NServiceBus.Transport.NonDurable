namespace NServiceBus.Transport.NonDurable.Tests.Design;

using System;
using System.Threading;
using NUnit.Framework;

[TestFixture]
public class When_configuring_transaction_timeout
{
    [Test]
    public void Should_not_set_a_timeout_by_default()
    {
        var transport = new NonDurableTransport();

        Assert.That(transport.TransactionTimeout, Is.Null);
    }

    [Test]
    public void Should_snapshot_the_timeout_at_construction()
    {
        var options = new NonDurableTransportOptions { TransactionTimeout = TimeSpan.FromMinutes(5) };

        var transport = new NonDurableTransport(options);
        options.TransactionTimeout = TimeSpan.FromMinutes(10);

        Assert.That(transport.TransactionTimeout, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void Should_allow_resetting_to_the_default()
    {
        var options = new NonDurableTransportOptions { TransactionTimeout = TimeSpan.FromMinutes(5) };

        options.TransactionTimeout = null;

        Assert.That(options.TransactionTimeout, Is.Null);
    }

    // TimeSpan.Zero means "no timeout" to System.Transactions, so it's rejected along with negative values.
    [TestCaseSource(nameof(InvalidTimeouts))]
    public void Should_reject_timeouts_that_are_not_positive(TimeSpan timeout)
    {
        var options = new NonDurableTransportOptions();

        Assert.That(() => options.TransactionTimeout = timeout, Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    static readonly TimeSpan[] InvalidTimeouts = [TimeSpan.Zero, TimeSpan.FromSeconds(-1), Timeout.InfiniteTimeSpan];
}

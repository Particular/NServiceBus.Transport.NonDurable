#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Buffers;
using System.Collections.Generic;
using NUnit.Framework;

[TestFixture]
public class When_rolling_back_enlisted_sends
{
    [Test]
    public void Should_return_pending_envelope_buffers_to_pool()
    {
        var pool = new TrackingArrayPool();
        var (committable, enlistment) = CreateReceiveTransaction();

        enlistment.Add(CreateEnvelope(pool, [1]));
        enlistment.Add(CreateEnvelope(pool, [2]));

        // Rolling back the CommittableTransaction fires the enlistment's
        // Rollback callback, which disposes pending envelopes (returns pooled
        // buffers) and clears the list — the decision 2-B contract.
        committable.Rollback();

        Assert.That(pool.ReturnedBuffers, Is.EqualTo(2));
        Assert.That(GetPendingEnvelopes(enlistment), Is.Empty);
        committable.Dispose();
    }

    static BrokerEnvelope CreateEnvelope(TrackingArrayPool pool, byte[] body)
    {
        var buffer = pool.Rent(body.Length);
        body.CopyTo(buffer, 0);

        return new BrokerEnvelope(
            "message-id",
            new ReadOnlyMemory<byte>(buffer, 0, body.Length),
            new Dictionary<string, string>(),
            "destination",
            false,
            1)
        {
            Pool = pool,
            Buffer = buffer
        };
    }

    sealed class TrackingArrayPool : ArrayPool<byte>
    {
        public int ReturnedBuffers { get; private set; }

        public override byte[] Rent(int minimumLength) => new byte[minimumLength];

        public override void Return(byte[] array, bool clearArray = false)
        {
            ReturnedBuffers++;
            if (clearArray)
            {
                Array.Clear(array);
            }
        }
    }
}

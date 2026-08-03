#nullable enable

namespace NServiceBus.TransportTests.InlineExecution;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NServiceBus.Transport;
using NUnit.Framework;

// Regression guard for the pooled-buffer ownership defect: a successfully processed message
// left its received envelope undisposed in InlineExecutionRunner.Process, so the rented
// ArrayPool buffer was never returned. This proves the success path returns the received
// envelope buffer exactly once.
[TestFixture]
public class When_successfully_processing_returns_received_envelope_buffer
{
    [Test]
    public async Task Should_return_received_envelope_buffer_to_pool()
    {
        await using var broker = new NonDurableBroker();
        var pool = new TrackingArrayPool();
        var runner = new InlineExecutionRunner(
            "input",
            TransportTransactionMode.None,
            static (_, _, _) => { },
            broker,
            static () => CancellationToken.None);

        runner.Initialize(
            (_, _) => Task.CompletedTask,
            (_, _) => Task.FromResult(ErrorHandleResult.Handled));

        var receivedEnvelope = CreateEnvelope(pool);

        await runner.Process(receivedEnvelope);

        Assert.That(pool.ReturnedBuffers, Is.EqualTo(1));
    }

    static BrokerEnvelope CreateEnvelope(TrackingArrayPool pool)
    {
        var buffer = pool.Rent(1);
        buffer[0] = 1;
        return new BrokerEnvelope(
            "received",
            new ReadOnlyMemory<byte>(buffer, 0, 1),
            new Dictionary<string, string>(),
            "input",
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

        public override void Return(byte[] array, bool clearArray = false) => ReturnedBuffers++;
    }
}

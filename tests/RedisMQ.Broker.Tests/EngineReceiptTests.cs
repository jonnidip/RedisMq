using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using Xunit;

namespace RedisMQ.Broker.Tests;

public sealed class EngineReceiptTests
{
    [Fact]
    public void Enqueue_returns_monotonic_envelope_ids_and_a_completed_true_receipt()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));

        EnqueueReceipt first = engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");
        EnqueueReceipt second = engine.Enqueue("/", "q", new ContentProperties(), [2], "fex", "rk");

        Assert.Equal(1UL, first.EnvelopeId);
        Assert.Equal(2UL, second.EnvelopeId);
        Assert.True(first.Durable.IsCompletedSuccessfully);
        Assert.True(first.Durable.Result);
        Assert.Empty(first.DroppedEnvelopeIds);
    }

    [Fact]
    public void Enqueue_reports_max_length_drops_in_DroppedEnvelopeIds()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q", MaxLength: 2));

        engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties(), [2], "fex", "rk");
        EnqueueReceipt third = engine.Enqueue("/", "q", new ContentProperties(), [3], "fex", "rk");

        Assert.Equal(3UL, third.EnvelopeId);
        Assert.Equal([1UL], third.DroppedEnvelopeIds);
        Assert.Equal([2UL, 3UL], engine.PeekQueue("q").Select(m => m.EnvelopeId));
    }

    [Fact]
    public void ReplayEnqueue_advances_the_id_counter()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.ReplayEnqueue("/", "q", new InMemoryMessage(
            new ContentProperties(), [9], "fex", "rk", EnvelopeId: 100));

        EnqueueReceipt receipt = engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");

        Assert.Equal(101UL, receipt.EnvelopeId);
    }
}

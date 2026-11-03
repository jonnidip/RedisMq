using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using Xunit;

namespace RedisMQ.Broker.Tests;

public sealed class EngineDeltaTests
{
    [Fact]
    public void Acknowledge_returns_the_message_as_removed()
    {
        var engine = new InMemoryEngine(["/"]);
        var message = new InMemoryMessage(new ContentProperties(), [1], "fex", "rk", EnvelopeId: 7);

        EngineDelta delta = engine.Acknowledge("/", "q", message);

        Assert.Empty(delta.Enqueued);
        Assert.Empty(delta.DeletedQueues);
        QueuedMessageRef removed = Assert.Single(delta.Removed);
        Assert.Equal(new QueuedMessageRef("/", "q", 7), removed);
    }

    [Fact]
    public void Reject_returns_the_message_as_removed()
    {
        var engine = new InMemoryEngine(["/"]);
        var message = new InMemoryMessage(new ContentProperties(), [1], "fex", "rk", EnvelopeId: 8);

        EngineDelta delta = engine.Reject("/", "q", message);

        Assert.Empty(delta.Enqueued);
        Assert.Empty(delta.DeletedQueues);
        Assert.Equal(new QueuedMessageRef("/", "q", 8), Assert.Single(delta.Removed));
    }

    [Fact]
    public void Requeue_enqueues_the_message_redelivered_and_removes_the_original_id()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q"));
        engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "q", out InMemoryMessage? claimed, out _));
        Assert.NotNull(claimed);

        EngineDelta delta = engine.Requeue("/", "q", claimed!);

        QueuedMessage enqueued = Assert.Single(delta.Enqueued);
        Assert.Equal("/", enqueued.Vhost);
        Assert.Equal("q", enqueued.Queue);
        Assert.True(enqueued.Message.Redelivered);
        Assert.Equal(claimed!.EnvelopeId, enqueued.Message.EnvelopeId);
        Assert.Equal(
            new QueuedMessageRef("/", "q", claimed.EnvelopeId),
            Assert.Single(delta.Removed));
        Assert.Empty(delta.DeletedQueues);

        InMemoryMessage ready = Assert.Single(engine.PeekQueue("q"));
        Assert.True(ready.Redelivered);
    }

    [Fact]
    public void Max_length_drop_ids_are_available_to_the_mirror()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "q", MaxLength: 2));

        engine.Enqueue("/", "q", new ContentProperties(), [1], "fex", "rk");
        engine.Enqueue("/", "q", new ContentProperties(), [2], "fex", "rk");
        EnqueueReceipt third = engine.Enqueue("/", "q", new ContentProperties(), [3], "fex", "rk");

        Assert.Equal([1UL], third.DroppedEnvelopeIds);
    }
}

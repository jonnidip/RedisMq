using System;
using System.Collections.Generic;
using System.Linq;
using RedisMQ.Broker.Codec;
using RedisMQ.Broker.Memory;
using Xunit;

namespace RedisMQ.Broker.Tests;

// M4 Task 8: dead-letter routing for reject/nack-without-requeue, delivery-limit
// overflow and ready-message expiry, carrying RabbitMQ's x-death headers.
public sealed class DeadLetterTests
{
    [Fact]
    public void Reject_with_dlx_enqueues_a_copy_with_x_death()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareExchange(new ExchangeInfo("/", "dlx", "direct"));
        engine.DeclareQueue(new QueueInfo(
            "/", "source", DeadLetterExchange: "dlx", DeadLetterRoutingKey: "dead"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Bind("/", "dlx", "target", "dead");
        engine.Enqueue(
            "/", "source", new ContentProperties(ContentType: "text/plain"), [1, 2], "origin-ex", "orig");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        EngineDelta delta = engine.Reject("/", "source", claimed!);

        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        Assert.Empty(engine.PeekQueue("source"));
        Assert.Equal("dlx", copy.Exchange);
        Assert.Equal("dead", copy.RoutingKey);
        Assert.Equal(new byte[] { 1, 2 }, copy.Body);
        Assert.Equal("text/plain", copy.Props.ContentType);

        FieldTable death = Deaths(copy.Props)[0];
        Assert.Equal("source", Field(death, "queue"));
        Assert.Equal("rejected", Field(death, "reason"));
        Assert.Equal(1L, death["count"].Raw);
        Assert.Equal("origin-ex", Field(death, "exchange"));
        Assert.IsType<DateTimeOffset>(death["time"].Raw);

        Assert.Equal("source", copy.Props.Headers!["x-first-death-queue"].Raw);
        Assert.Equal("rejected", copy.Props.Headers!["x-first-death-reason"].Raw);
        Assert.Equal("origin-ex", copy.Props.Headers!["x-first-death-exchange"].Raw);

        Assert.Single(delta.Enqueued);
        Assert.Single(delta.Removed);
    }

    [Fact]
    public void Nack_without_requeue_dead_letters()
    {
        // basic.nack(requeue:false) and basic.reject(requeue:false) reach the engine
        // through the same Reject path; this pins that the path dead-letters.
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue("/", "source", new ContentProperties(), [7], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        engine.Reject("/", "source", claimed!);

        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        Assert.Equal("rejected", Field(Deaths(copy.Props)[0], "reason"));
    }

    [Fact]
    public void Requeue_over_limit_dead_letters()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo(
            "/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target", DeliveryLimit: 1));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue("/", "source", new ContentProperties(), [1], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));
        Assert.Equal(1, claimed!.DeliveryCount);

        EngineDelta delta = engine.Requeue("/", "source", claimed);

        Assert.Empty(engine.PeekQueue("source"));
        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        Assert.Equal(0, copy.DeliveryCount);
        Assert.Equal("rejected", Field(Deaths(copy.Props)[0], "reason"));
        Assert.Single(delta.Enqueued);
        Assert.Single(delta.Removed);
    }

    [Fact]
    public void Expired_ready_message_dead_letters()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue(
            "/", "source", new ContentProperties { Expiration = "1" }, [3], "ex", "rk");

        DateTimeOffset reapedAt = DateTimeOffset.UtcNow.AddSeconds(1);
        EngineDelta delta = engine.Reap(reapedAt);

        Assert.Empty(engine.PeekQueue("source"));
        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        FieldTable death = Deaths(copy.Props)[0];
        Assert.Equal("source", Field(death, "queue"));
        Assert.Equal("expired", Field(death, "reason"));
        Assert.Equal(reapedAt, death["time"].Raw);
        Assert.Single(delta.Enqueued);
        Assert.Single(delta.Removed);
    }

    [Fact]
    public void Missing_dlx_exchange_drops_the_copy()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "missing"));
        engine.Enqueue("/", "source", new ContentProperties(), [1], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        EngineDelta delta = engine.Reject("/", "source", claimed!);

        Assert.Empty(engine.PeekQueue("source"));
        Assert.Empty(delta.Enqueued);
        Assert.Single(delta.Removed);
    }

    [Fact]
    public void Empty_dlx_name_uses_the_default_exchange()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue("/", "source", new ContentProperties(), [9], "origin-ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        engine.Reject("/", "source", claimed!);

        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        Assert.Equal(string.Empty, copy.Exchange); // the default exchange, not null
        Assert.Equal("target", copy.RoutingKey);
        Assert.Equal("origin-ex", Field(Deaths(copy.Props)[0], "exchange"));
    }

    [Fact]
    public void X_death_increments_matching_entry_and_prepends_a_new_one()
    {
        // The loop guard (pinned below) prevents the same (queue, reason) from
        // dead-lettering twice, so the increment half is exercised directly on the
        // header builder, exactly as RabbitMQ's own table update behaves.
        DateTimeOffset first = DateTimeOffset.UtcNow.AddSeconds(-5);
        var properties = new ContentProperties(
            Headers: new FieldTable([
                new("x-death", FieldValue.List(FieldValue.From(Death("a", "rejected", 1, first, "ex-a")))),
            ]));

        ContentProperties incremented = InMemoryEngine.WithDeath(properties, "a", "ex-a", "rejected", DateTimeOffset.UtcNow);
        FieldTable match = Assert.Single(Deaths(incremented));
        Assert.Equal(2L, match["count"].Raw);
        Assert.True((DateTimeOffset)match["time"].Raw! > first);

        ContentProperties prepended = InMemoryEngine.WithDeath(properties, "b", "ex-b", "expired", DateTimeOffset.UtcNow);
        FieldTable[] entries = Deaths(prepended);
        Assert.Equal(2, entries.Length);
        Assert.Equal("b", Field(entries[0], "queue"));
        Assert.Equal("expired", Field(entries[0], "reason"));
        Assert.Equal(1L, entries[0]["count"].Raw);
        Assert.Equal("a", Field(entries[1], "queue"));
        Assert.Equal(1L, entries[1]["count"].Raw);
    }

    [Fact]
    public void X_first_death_fields_are_set_once()
    {
        DateTimeOffset first = DateTimeOffset.UtcNow.AddSeconds(-5);
        ContentProperties once = InMemoryEngine.WithDeath(
            new ContentProperties(), "a", "ex-a", "rejected", first);
        ContentProperties twice = InMemoryEngine.WithDeath(once, "b", "ex-b", "expired", DateTimeOffset.UtcNow);

        Assert.Equal("a", twice.Headers!["x-first-death-queue"].Raw);
        Assert.Equal("rejected", twice.Headers!["x-first-death-reason"].Raw);
        Assert.Equal("ex-a", twice.Headers!["x-first-death-exchange"].Raw);
    }

    [Fact]
    public void Dead_letter_copy_resets_delivery_count_and_clears_expiration()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        engine.Enqueue(
            "/", "source", new ContentProperties { Expiration = "60000" }, [1], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));
        Assert.Equal(1, claimed!.DeliveryCount);
        Assert.NotNull(claimed.Expiration);

        engine.Reject("/", "source", claimed);

        InMemoryMessage copy = Assert.Single(engine.PeekQueue("target"));
        Assert.Equal(0, copy.DeliveryCount);
        Assert.Null(copy.Expiration);
        Assert.Null(copy.Props.Expiration);
    }

    [Fact]
    public void Loop_guard_drops_a_second_death_for_the_same_queue_and_reason()
    {
        var engine = new InMemoryEngine(["/"]);
        engine.DeclareQueue(new QueueInfo("/", "source", DeadLetterExchange: "", DeadLetterRoutingKey: "target"));
        engine.DeclareQueue(new QueueInfo("/", "target"));
        var headers = new FieldTable([
            new("x-death", FieldValue.List(
                FieldValue.From(Death("source", "rejected", 1, DateTimeOffset.UtcNow.AddSeconds(-1), "ex")))),
        ]);
        engine.Enqueue("/", "source", new ContentProperties(Headers: headers), [1], "ex", "rk");
        Assert.Equal(GetClaimOutcome.Claimed, engine.TryClaimForGet("/", "source", out InMemoryMessage? claimed, out _));

        EngineDelta delta = engine.Reject("/", "source", claimed!);

        Assert.Empty(engine.PeekQueue("target"));
        Assert.Empty(delta.Enqueued);
        Assert.Single(delta.Removed);
    }

    // ---------- helpers ----------

    private static FieldTable Death(string queue, string reason, long count, DateTimeOffset time, string exchange) =>
        new([
            new("queue", FieldValue.From(queue)),
            new("reason", FieldValue.From(reason)),
            new("count", FieldValue.From(count)),
            new("time", FieldValue.From(time)),
            new("exchange", FieldValue.From(exchange)),
        ]);

    private static FieldTable[] Deaths(ContentProperties properties)
    {
        var array = (FieldValue[])properties.Headers!["x-death"].Raw!;
        return [.. array.Select(value => (FieldTable)value.Raw!)];
    }

    private static object? Field(FieldTable table, string key) => table[key].Raw;
}

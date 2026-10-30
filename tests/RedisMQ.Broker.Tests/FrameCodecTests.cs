using System.Buffers;
using RedisMQ.Broker.Codec;
using Xunit;

namespace RedisMQ.Broker.Tests;

// Frame layer (Task 2): AmqpFrame constants, FrameDecoder over partial/segmented
// buffers, ContentHeaderCodec property flags, and the FrameWriter helpers.
// Golden bytes follow AMQP 0-9-1 as spoken by RabbitMQ: frame = type(1) + channel(2)
// + size(4) + payload + 0xCE where size counts the payload only, and content bodies
// are split at frame-max minus the 8-byte empty-frame overhead
// (rabbitmq-java-client AMQCommand.EMPTY_FRAME_SIZE = 8).
// The negotiated frame-max is a u32 quantity (connection.tune carries a long-uint;
// RabbitMQ defaults to 131072, so it cannot be carried in a ushort).
public sealed class FrameCodecTests
{
    private static readonly uint TestFrameMax = 131072;

    // ---------- helpers ----------

    private static List<AmqpFrame> Decode(byte[] wire, uint frameMax = 131072)
    {
        var buffer = new ReadOnlySequence<byte>(wire);
        var frames = new List<AmqpFrame>();
        DecodeMode mode = FrameDecoder.ReadFrames(ref buffer, frames, frameMax);
        Assert.Equal(DecodeMode.NeedMore, mode);
        Assert.Equal(0, buffer.Length); // a fully decodable buffer leaves nothing behind
        return frames;
    }

    /// <summary>Builds a non-contiguous ReadOnlySequence over raw chunks, so the decoder
    /// is exercised across segmented buffers like real socket reads produce.</summary>
    private static ReadOnlySequence<byte> Segmented(params byte[][] parts)
    {
        Segment? head = null;
        Segment? tail = null;
        foreach (byte[] part in parts)
        {
            var next = new Segment(part, tail);
            if (head is null)
            {
                head = next;
            }

            tail = next;
        }

        return new ReadOnlySequence<byte>(head!, 0, tail!, tail!.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(byte[] bytes, Segment? previous)
        {
            Memory = bytes;
            RunningIndex = previous is null ? 0 : previous.RunningIndex + previous.Memory.Length;
            if (previous is not null)
            {
                previous.Next = this;
            }
        }
    }

    private static byte[] Encode(Action<ByteWriter> write)
    {
        var writer = new ByteWriter();
        write(writer);
        return writer.ToArray();
    }

    // Golden connection.tune method frame on channel 0: class 10, method 30, and
    // args channel-max 2047 (u16), frame-max 131072 (u32), heartbeat 60 (u16).
    private static readonly byte[] ConnectionTuneWire =
    [
        0x01, 0x00, 0x00,               // frame-method on channel 0
        0x00, 0x00, 0x00, 0x0C,         // payload size 12
        0x00, 0x0A,                     // class 10 (connection)
        0x00, 0x1E,                     // method 30 (tune)
        0x07, 0xFF,                     // channel-max 2047
        0x00, 0x02, 0x00, 0x00,         // frame-max 131072
        0x00, 0x3C,                     // heartbeat 60
        0xCE,                           // frame-end
    ];

    private static readonly byte[] HeartbeatWire = [0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xCE];

    // ---------- AmqpFrame constants ----------

    [Fact]
    public void Frame_constants_match_the_0_9_1_wire_values()
    {
        Assert.Equal(1, AmqpFrame.FrameMethod);
        Assert.Equal(2, AmqpFrame.FrameHeader);
        Assert.Equal(3, AmqpFrame.FrameBody);
        Assert.Equal(8, AmqpFrame.FrameHeartbeat);
        Assert.Equal(0xCE, AmqpFrame.FrameEnd);
        Assert.Equal(4096, AmqpFrame.FrameMinSize);
    }

    // ---------- FrameWriter.WriteMethod: golden bytes ----------

    [Fact]
    public void WriteMethod_connection_tune_produces_golden_bytes()
    {
        byte[] args = [0x07, 0xFF, 0x00, 0x02, 0x00, 0x00, 0x00, 0x3C];
        byte[] wire = Encode(w => FrameWriter.WriteMethod(ref w, channel: 0, classId: 10, methodId: 30, args));

        Assert.Equal(ConnectionTuneWire, wire);
    }

    [Fact]
    public void WriteMethod_with_empty_args_yields_four_byte_payload()
    {
        // channel.close-ok carries no arguments: the payload is class + method only.
        byte[] wire = Encode(w => FrameWriter.WriteMethod(ref w, channel: 0x0102, classId: 20, methodId: 51, args: []));

        Assert.Equal(
        [
            0x01, 0x01, 0x02,               // frame-method, channel 0x0102
            0x00, 0x00, 0x00, 0x04,         // payload size 4
            0x00, 0x14,                     // class 20 (channel)
            0x00, 0x33,                     // method 51 (close-ok)
            0xCE,
        ], wire);
    }

    [Fact]
    public void Decoded_method_frame_carries_type_channel_and_payload()
    {
        List<AmqpFrame> frames = Decode(ConnectionTuneWire);

        AmqpFrame frame = Assert.Single(frames);
        Assert.Equal(AmqpFrame.FrameMethod, frame.Type);
        Assert.Equal(0, frame.Channel);
        Assert.Equal(12, frame.Payload.Length);
        Assert.Equal([0x00, 0x0A, 0x00, 0x1E, 0x07, 0xFF, 0x00, 0x02, 0x00, 0x00, 0x00, 0x3C],
            frame.Payload.ToArray());
    }

    // ---------- heartbeat frame: 7 header bytes, empty payload ----------

    [Fact]
    public void Heartbeat_frame_is_seven_header_bytes_with_a_frame_end()
    {
        List<AmqpFrame> frames = Decode(HeartbeatWire);

        AmqpFrame frame = Assert.Single(frames);
        Assert.Equal(AmqpFrame.FrameHeartbeat, frame.Type);
        Assert.Equal(0, frame.Channel);
        Assert.Empty(frame.Payload.ToArray());
    }

    [Fact]
    public void Decoder_produces_consecutive_frames_from_one_buffer()
    {
        byte[] wire = [.. ConnectionTuneWire, .. HeartbeatWire];
        List<AmqpFrame> frames = Decode(wire);

        Assert.Equal(2, frames.Count);
        Assert.Equal(AmqpFrame.FrameMethod, frames[0].Type);
        Assert.Equal(AmqpFrame.FrameHeartbeat, frames[1].Type);
        Assert.Equal(12, frames[0].Payload.Length);
    }

    // ---------- channel identity is preserved per frame ----------

    [Fact]
    public void Out_of_order_channel_prefix_is_preserved_per_frame()
    {
        byte[] wire =
        [
            // channel 5 method frame, empty args (close-ok shaped)
            0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x00, 0x14, 0x00, 0x33, 0xCE,
            .. HeartbeatWire,                                            // channel 0
            // channel 3 body frame with a single payload byte
            0x03, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0xAA, 0xCE,
            // channel 7 heartbeat variant
            0x08, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0xCE,
        ];

        List<AmqpFrame> frames = Decode(wire);

        Assert.Equal(4, frames.Count);
        Assert.Equal(AmqpFrame.FrameMethod, frames[0].Type);
        Assert.Equal(AmqpFrame.FrameHeartbeat, frames[1].Type);
        Assert.Equal(AmqpFrame.FrameBody, frames[2].Type);
        Assert.Equal(AmqpFrame.FrameHeartbeat, frames[3].Type);
        Assert.Equal(5, frames[0].Channel);
        Assert.Equal(0, frames[1].Channel);
        Assert.Equal(3, frames[2].Channel);
        Assert.Equal(7, frames[3].Channel);
        Assert.Equal([0xAA], frames[2].Payload.ToArray());
    }

    // ---------- body splitting at frame-max ----------

    [Fact]
    public void Body_writing_splits_large_messages_at_frame_max_minus_overhead()
    {
        int bodyLength = 200_000;
        byte[] body = new byte[bodyLength];
        for (int i = 0; i < bodyLength; i++)
        {
            body[i] = (byte)(i % 251);
        }

        byte[] wire = Encode(w => FrameWriter.WriteBody(ref w, channel: 7, body, TestFrameMax));

        // Payload cap per body frame = frame-max - 8 (the empty-frame overhead), exactly
        // like the RabbitMQ clients: 200000 = 131064 + 68936, so the first frame totals
        // exactly 131072 bytes (the negotiated frame-max) and the second 68944.
        Assert.Equal(131064 + 68936 + 2 * 8, wire.Length);

        Assert.Equal(0x03, wire[0]);    // frame-body
        Assert.Equal(0x00, wire[1]);    // channel 7
        Assert.Equal(0x07, wire[2]);
        Assert.Equal(0x00, wire[3]);    // payload size 131064 = 0x0001FFF8
        Assert.Equal(0x01, wire[4]);
        Assert.Equal(0xFF, wire[5]);
        Assert.Equal(0xF8, wire[6]);
        Assert.True(wire.AsSpan(7, 131064).SequenceEqual(body.AsSpan(0, 131064)));
        Assert.Equal(0xCE, wire[7 + 131064]);

        int secondStart = 8 + 131064;
        Assert.Equal(0x03, wire[secondStart]);
        Assert.Equal(0x00, wire[secondStart + 1]);
        Assert.Equal(0x07, wire[secondStart + 2]);
        Assert.Equal(0x00, wire[secondStart + 3]);  // payload size 68936 = 0x00010D48
        Assert.Equal(0x01, wire[secondStart + 4]);
        Assert.Equal(0x0D, wire[secondStart + 5]);
        Assert.Equal(0x48, wire[secondStart + 6]);
        Assert.True(wire.AsSpan(secondStart + 7, 68936).SequenceEqual(body.AsSpan(131064, 68936)));
        Assert.Equal(0xCE, wire[^1]);
    }

    [Fact]
    public void Body_writing_within_frame_max_is_a_single_frame()
    {
        byte[] body = [0x11, 0x22, 0x33];
        byte[] wire = Encode(w => FrameWriter.WriteBody(ref w, channel: 1, body, TestFrameMax));

        Assert.Equal([0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x11, 0x22, 0x33, 0xCE], wire);
    }

    [Fact]
    public void Empty_body_writes_no_body_frames()
    {
        byte[] wire = Encode(w => FrameWriter.WriteBody(ref w, channel: 0, body: [], TestFrameMax));

        Assert.Empty(wire); // an empty body is carried by the content header alone
    }

    [Fact]
    public void Decoded_body_frames_reassemble_the_original_body()
    {
        byte[] body = new byte[300];
        for (int i = 0; i < body.Length; i++)
        {
            body[i] = (byte)(i * 7);
        }

        byte[] wire = Encode(w => FrameWriter.WriteBody(ref w, channel: 0, body, frameMax: 64));

        // payload cap = 64 - 8 = 56: 300 bytes split into 5 full frames plus a 20-byte tail
        List<AmqpFrame> frames = Decode(wire, frameMax: 64);

        Assert.Equal(6, frames.Count);
        for (int i = 0; i < frames.Count; i++)
        {
            Assert.Equal(AmqpFrame.FrameBody, frames[i].Type);
            Assert.Equal(0, frames[i].Channel);
            Assert.Equal(i < 5 ? 56 : 20, frames[i].Payload.Length);
        }

        var reassembled = new List<byte>();
        foreach (AmqpFrame frame in frames)
        {
            reassembled.AddRange(frame.Payload.ToArray());
        }

        Assert.Equal(body, reassembled);
    }

    // ---------- content header property flags ----------

    [Fact]
    public void Content_header_encode_uses_the_headers_bit_when_content_type_is_null()
    {
        var headers = new FieldTable([new KeyValuePair<string, FieldValue>("a", FieldValue.From(5))]);
        byte[] tableWire = Encode(w => w.WriteFieldTable(headers));

        byte[] content = Encode(w => ContentHeaderCodec.Write(w, classId: 60, bodySize: 8, new ContentProperties(Headers: headers)));

        Assert.Equal(
        [
            0x00, 0x3C,                                     // class 60 (basic)
            0x00, 0x00,                                     // weight 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // body size 8
            0x20, 0x00,                                     // property flags: headers (3rd bit, 15-2)
            .. tableWire,
        ], content);
    }

    [Fact]
    public void Content_header_flag_word_ends_the_property_list_with_a_clear_stop_bit()
    {
        // A single-property list terminates the flag list: the low bit of the only
        // flag word is the continuation/stop marker and must stay clear.
        byte[] content = Encode(w => ContentHeaderCodec.Write(w, classId: 60, bodySize: 0, new ContentProperties(ContentType: "text/plain")));

        Assert.Equal(0x80, content[12]); // flag word high byte: contentType (bit 15)
        Assert.Equal(0x00, content[13]); // low byte zero: no continuation flag word follows
        Assert.Equal(0x0A, content[14]); // shortstr length of "text/plain"
    }

    [Fact]
    public void Content_header_round_trips_all_basic_properties_in_wire_order()
    {
        var headers = new FieldTable();
        headers.Add("app", FieldValue.From("test"));

        var source = new ContentProperties(
            ContentType: "application/json",
            ContentEncoding: "utf-8",
            Headers: headers,
            DeliveryMode: 2,
            Priority: 3,
            CorrelationId: "corr-1",
            ReplyTo: "amq.gen-1",
            Expiration: "60000",
            MessageId: "msg-1",
            Timestamp: new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            Type: "test-type",
            UserId: "guest",
            AppId: "redis-mq",
            ClusterId: "rabbit@cluster");

        byte[] content = Encode(w => ContentHeaderCodec.Write(w, classId: 60, bodySize: 0x1234, source));

        // every one of the 14 standard flags is set: bits 15..2 => 0xFFFC
        Assert.Equal(0xFF, content[12]);
        Assert.Equal(0xFC, content[13]);

        var reader = new ByteReader(content);
        ContentHeader decoded = ContentHeaderCodec.Read(ref reader);

        Assert.Equal(60, decoded.ClassId);
        Assert.Equal(0, decoded.Weight);
        Assert.Equal(0x1234, decoded.BodySize);
        Assert.Equal("application/json", decoded.Properties.ContentType);
        Assert.Equal("utf-8", decoded.Properties.ContentEncoding);
        Assert.Equal("test", (string)decoded.Properties.Headers!["app"].Raw!);
        Assert.Equal((byte)2, decoded.Properties.DeliveryMode);
        Assert.Equal((byte)3, decoded.Properties.Priority);
        Assert.Equal("corr-1", decoded.Properties.CorrelationId);
        Assert.Equal("amq.gen-1", decoded.Properties.ReplyTo);
        Assert.Equal("60000", decoded.Properties.Expiration);
        Assert.Equal("msg-1", decoded.Properties.MessageId);
        Assert.Equal(source.Timestamp, decoded.Properties.Timestamp);
        Assert.Equal("test-type", decoded.Properties.Type);
        Assert.Equal("guest", decoded.Properties.UserId);
        Assert.Equal("redis-mq", decoded.Properties.AppId);
        Assert.Equal("rabbit@cluster", decoded.Properties.ClusterId);
        Assert.Equal(content.Length, reader.Offset);

        // re-encoding the decoded header is byte-identical (flags and field order hold)
        byte[] reencoded = Encode(w => ContentHeaderCodec.Write(w, decoded.ClassId, decoded.BodySize, decoded.Properties));
        Assert.Equal(content, reencoded);
    }

    [Fact]
    public void Content_header_read_omits_unset_properties()
    {
        // delivery-mode and priority only: bits 15-3 = 0x1000 and 15-4 = 0x0800
        byte[] content =
        [
            0x00, 0x3C,
            0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x18, 0x00,
            0x02, 0x07,
        ];

        var reader = new ByteReader(content);
        ContentHeader decoded = ContentHeaderCodec.Read(ref reader);

        Assert.Equal(60, decoded.ClassId);
        Assert.Null(decoded.Properties.ContentType);
        Assert.Equal((byte)2, decoded.Properties.DeliveryMode);
        Assert.Equal((byte)7, decoded.Properties.Priority);
    }

    [Fact]
    public void Content_header_read_rejects_a_continuation_flag_word()
    {
        // The low bit (0x0001) of a flag word continues the property list with a second
        // flag word of extension properties; the M0 broker refuses instead of misparsing.
        byte[] content =
        [
            0x00, 0x3C, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x80, 0x01,   // contentType present + continuation bit set
            0x00, 0x00,   // (the extension flag word would appear here)
        ];

        Assert.Throws<AmqpDecodeException>(() =>
        {
            var reader = new ByteReader(content);
            return ContentHeaderCodec.Read(ref reader);
        });
    }

    [Fact]
    public void Content_header_write_rejects_non_basic_classes()
    {
        Assert.Throws<AmqpEncodeException>(
            () => ContentHeaderCodec.Write(new ByteWriter(), classId: 20, bodySize: 0, new ContentProperties()));
    }

    [Fact]
    public void Content_header_read_rejects_non_basic_classes()
    {
        byte[] content = [0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        Assert.Throws<AmqpDecodeException>(() =>
        {
            var reader = new ByteReader(content);
            return ContentHeaderCodec.Read(ref reader);
        });
    }

    // ---------- FrameWriter.WriteContentHeader wraps the header in a frame ----------

    [Fact]
    public void WriteContentHeader_wraps_the_header_payload_in_a_frame()
    {
        var headers = new FieldTable([new KeyValuePair<string, FieldValue>("a", FieldValue.From(5))]);
        byte[] tableWire = Encode(w => w.WriteFieldTable(headers));
        byte[] content =
        [
            0x00, 0x3C,                                     // class 60
            0x00, 0x00,                                     // weight 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, // body size 8
            0x20, 0x00,                                     // flags: headers only
            .. tableWire,
        ];

        byte[] wire = Encode(w => FrameWriter.WriteContentHeader(ref w, channel: 0, classId: 60, bodySize: 8, new ContentProperties(Headers: headers)));

        Assert.Equal([0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, .. content, 0xCE], wire);
        Assert.Equal(content.Length, wire.Length - 8); // size field counts the payload only

        List<AmqpFrame> frames = Decode(wire);
        AmqpFrame frame = Assert.Single(frames);
        Assert.Equal(AmqpFrame.FrameHeader, frame.Type);
    }

    // ---------- FrameDecoder: partial and segmented feeds ----------

    [Fact]
    public void Feeding_a_frame_in_three_byte_chunks_decodes_it_at_the_end()
    {
        var buffered = new List<byte>();
        var frames = new List<AmqpFrame>();
        int chunkWithFirstFrame = -1;

        for (int chunkStart = 0; chunkStart < ConnectionTuneWire.Length; chunkStart += 3)
        {
            int take = Math.Min(3, ConnectionTuneWire.Length - chunkStart);
            for (int j = 0; j < take; j++)
            {
                buffered.Add(ConnectionTuneWire[chunkStart + j]);
            }

            var buffer = new ReadOnlySequence<byte>(buffered.ToArray());
            DecodeMode mode = FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

            buffered.Clear();
            buffered.AddRange(buffer.ToArray());

            if (frames.Count > 0 && chunkWithFirstFrame < 0)
            {
                chunkWithFirstFrame = chunkStart; // record which chunk completed the frame
            }
        }

        // 20 bytes in 3-byte chunks: no frame can complete before the last 2 bytes arrive
        Assert.Equal(ConnectionTuneWire.Length - 2, chunkWithFirstFrame);
        Assert.Single(frames);
        Assert.Equal(AmqpFrame.FrameMethod, frames[0].Type);
        Assert.Equal(0, frames[0].Channel);
        Assert.Equal(12, frames[0].Payload.Length);
    }

    [Fact]
    public void Partial_frame_needs_more_bytes_and_completes_on_the_final_octet()
    {
        var frames = new List<AmqpFrame>();

        byte[] head = HeartbeatWire[..^1]; // everything but the frame-end byte
        var buffer = new ReadOnlySequence<byte>(head);
        DecodeMode mode = FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

        Assert.Equal(DecodeMode.NeedMore, mode);
        Assert.Empty(frames);
        Assert.Equal(head.Length, buffer.Length); // nothing consumed until the frame completes

        byte[] last = [HeartbeatWire[^1]];
        byte[] completed = [.. buffer.ToArray(), .. last];
        buffer = new ReadOnlySequence<byte>(completed);
        mode = FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

        Assert.Equal(DecodeMode.NeedMore, mode);
        Assert.Single(frames);
        Assert.Equal(0, buffer.Length);
    }

    [Fact]
    public void Decoder_consumes_across_segmented_buffers()
    {
        byte[] methodAndHeartbeat = [.. ConnectionTuneWire, .. HeartbeatWire];

        // split so both frames straddle segment boundaries
        var buffer = Segmented(
            methodAndHeartbeat[..5],
            methodAndHeartbeat[5..15],
            methodAndHeartbeat[15..24],
            methodAndHeartbeat[24..]);
        var frames = new List<AmqpFrame>();
        DecodeMode mode = FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

        Assert.Equal(DecodeMode.NeedMore, mode);
        Assert.Equal(2, frames.Count);
        Assert.Equal(AmqpFrame.FrameMethod, frames[0].Type);
        Assert.Equal(AmqpFrame.FrameHeartbeat, frames[1].Type);
        Assert.Equal(0, buffer.Length);
    }

    [Fact]
    public void Segmented_partial_feed_keeps_unconsumed_bytes()
    {
        byte[] wire = [.. ConnectionTuneWire, .. HeartbeatWire]; // 28 bytes: 20 + 8

        // feed 23 bytes split over two segments: the whole method frame + 3 heartbeat header bytes
        var buffer = Segmented(wire[..12], wire[12..23]);
        var frames = new List<AmqpFrame>();
        FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

        Assert.Single(frames); // only the method frame is complete
        Assert.Equal(3, buffer.Length); // the 3 buffered heartbeat header bytes remain

        // feed the remaining 5 bytes as a third segment
        var remainder = buffer.ToArray();
        byte[] tail = wire[23..];
        byte[] merged = [.. remainder, .. tail];
        buffer = new ReadOnlySequence<byte>(merged);
        DecodeMode mode = FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);

        Assert.Equal(DecodeMode.NeedMore, mode);
        Assert.Equal(2, frames.Count);
        Assert.Equal(0, buffer.Length);
    }

    // ---------- hard errors ----------

    [Fact]
    public void Wrong_frame_end_byte_throws_the_hard_error()
    {
        byte[] wire = [.. HeartbeatWire[..^1], (byte)0x00];

        Assert.Throws<AmqpProtocolException>(() => Decode(wire));
    }

    [Fact]
    public void Frame_size_over_the_negotiated_max_throws_even_before_the_payload_arrives()
    {
        // only the 7-byte header is buffered: size 131073 > frame-max 131072
        byte[] header = [0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01];

        Assert.Throws<AmqpProtocolException>(() =>
        {
            var buffer = new ReadOnlySequence<byte>(header);
            var frames = new List<AmqpFrame>();
            FrameDecoder.ReadFrames(ref buffer, frames, TestFrameMax);
        });
    }
}

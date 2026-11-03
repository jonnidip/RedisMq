# RedisMQ Protocol Implementation Notes

AMQP 0-9-1 (RabbitMQ dialect) as implemented in RedisMQ v1-completeness (M4 part 1).
This document covers the wire dialect, method set, queue arguments, error surface,
confirm behavior, and known deviations from RabbitMQ.

## Wire dialect

| Property | Value |
|----------|-------|
| Protocol header | `"AMQP\0\0\9\1"` (8 bytes) |
| Frame-end octet | `0xCE` |
| Frame overhead | 8 bytes (type 1 + channel 2 + size 4 + end 1) |
| Minimum negotiated frame-max | 4096 (`frame_min_size`) |
| Default frame-max offer | 131072 |
| Default heartbeat offer | 60 s |
| SASL mechanism | PLAIN only |
| PLAIN response | `authzid\0authcid\0password` |
| Transport | plaintext TCP or TLS (`SslStream`, TLS 1.2+ by default) |

## Connection handshake

```
Client                          Server
  |                               |
  |── "AMQP\0\0\9\1" ───────────>│  (validate header; mismatch → reply server header)
  |                               |
  |<──── connection.start ───────│  (version 0.9, PLAIN, locales)
  |                               |
  |── connection.start-ok ───────>│  (PLAIN response)
  |                               │
  |<──── connection.tune ────────│  (channel-max, frame-max, heartbeat)
  |                               │
  |── connection.tune-ok ───────>│  (negotiated values)
  |                               │
  |── connection.open("/") ──────>│  (unknown vhost → invalid-path 402 hard close;
  |                               │   no permission entry → access-refused 403 hard close)
  |<──── connection.open-ok ─────│  → state = Serving
  |                               │
  |── channel.open ─────────────>│  (per channel number)
  |                               │
  |<──── channel.open-ok ────────│  → channel Running
```

### Authentication and vhost authorization

- PLAIN is the only mechanism; any other mechanism closes with 403
  `ACCESS_REFUSED - PLAIN is the only supported authentication mechanism`.
- The account's configured credential is either a plaintext password (development
  only) or a PBKDF2-SHA256 hash. Verification is fixed-time.
- `connection.open` is **default-deny**: the account must have a permission entry for
  the named vhost, even one whose patterns deny everything. An unlisted vhost gets a
  hard 403, not 402. A vhost that is absent from `BrokerServerOptions.Vhosts` still
  gets 402.
- Resource permissions (configure/write/read) are checked per operation and produce a
  soft 403 channel close with RabbitMQ's wording, for example:
  `ACCESS_REFUSED - access to queue 'q' in vhost '/' refused for user 'bob'`.

### TuneOk negotiation rules

- Frame-max = min(server offer, client value). A client TuneOk of 0 keeps the server
  offer.
- A client TuneOk frame-max **below 4096** → hard `not_allowed` (530) connection
  close. Never clamped up.
- Heartbeat adopted verbatim. 0 disables heartbeats entirely (no watchdog).
- Channel-max = min(offer, client), stored but unenforced.
- No second `connection.tune` is ever sent.

### TLS

TLS is negotiated below AMQP; the wire protocol is identical. Clients must attach an
`SslOption` to the explicit `AmqpTcpEndpoint`. With mutual TLS, the server requires a
client certificate chained to the configured CA bundle and closes the socket
otherwise.

## Channel method set

Only these class/method pairs are implemented. Everything else on a channel → hard
`not_implemented` (540) connection close.

| Class | Method | (class,method) | Direction | Content? | Behavior |
|-------|--------|----------------|-----------|----------|----------|
| 20 | channel.open | (20,10) | C→S | No | Starting → Running; open-ok |
| 20 | channel.open-ok | (20,11) | S→C | No | Reply |
| 20 | channel.flow | (20,20) | C→S | No | Sets flow state; flow-ok echoes `active`. `false` pauses deliveries, does not close |
| 20 | channel.flow-ok | (20,21) | S→C | No | Reply |
| 20 | channel.close | (20,40) | C→S | No | close-ok; retire channel number |
| 20 | channel.close-ok | (20,41) | S→C | No | Retire channel number |
| 40 | exchange.declare | (40,10) | C→S | No | type check → default-exchange refusal → permission → equivalence → create |
| 40 | exchange.bind | (40,30) | C→S | No | Not in the channel switch → 540 |
| 40 | exchange.unbind | (40,40) | C→S | No | 540 |
| 50 | queue.declare | (50,10) | C→S | No | arguments → permission → reserved prefix → existence → equivalence → create |
| 50 | queue.declare-ok | (50,11) | S→C | No | queue/message-count/consumer-count |
| 50 | queue.bind | (50,20) | C→S | No | permission → shortcuts → default-exchange refusal → existence → bind |
| 50 | queue.bind-ok | (50,21) | S→C | No | Reply (unless nowait) |
| 50 | queue.unbind | (50,50) | C→S | No | permission; always replies unbind-ok |
| 50 | queue.unbind-ok | (50,51) | S→C | No | Reply |
| 60 | basic.qos | (60,10) | C→S | No | count+size stored; global demoted; qos-ok via chain |
| 60 | basic.qos-ok | (60,11) | S→C | No | Reply |
| 60 | basic.consume | (60,20) | C→S | No | read permission → lookup → tag reuse → register → consume-ok |
| 60 | basic.consume-ok | (60,21) | S→C | No | Reply with consumer-tag |
| 60 | basic.cancel | (60,30) | C→S | No | unregister; cancel-ok echoes any tag |
| 60 | basic.cancel-ok | (60,31) | S→C | No | Reply |
| 60 | basic.publish | (60,40) | C→S | Yes | content assembly → permission → routing → enqueue / return / drop |
| 60 | basic.return | (60,50) | S→C | Yes | mandatory unroutable: 312 NO_ROUTE + content |
| 60 | basic.deliver | (60,60) | S→C | Yes | pump-delivered to consumer |
| 60 | basic.get | (60,70) | C→S | No | read permission → atomic claim → get-ok + content, or get-empty |
| 60 | basic.get-ok | (60,71) | S→C | Yes | delivery-tag/redelivered/exchange/routing-key/message-count |
| 60 | basic.get-empty | (60,72) | S→C | No | cluster-id "" |
| 60 | basic.ack | (60,80) | C→S / S→C | No | Client: settle ledger. Server: publisher confirm ack (confirm mode) |
| 60 | basic.reject | (60,90) | C→S | No | settle single; optional requeue (dead-letters when false) |
| 60 | basic.recover-async | (60,100) | C→S | No | 540 (outside the set) |
| 60 | basic.recover | (60,110) | C→S | No | requeue=true → recover-ok + requeue channel ledger; false → hard 540 |
| 60 | basic.recover-ok | (60,111) | S→C | No | Reply |
| 60 | basic.nack | (60,120) | C→S / S→C | No | Client: settle. Server: publisher confirm nack |
| 85 | confirm.select | (85,10) | C→S | No | Enables publisher confirms; confirm.select-ok |
| 85 | confirm.select-ok | (85,11) | S→C | No | Reply |

Connection-class methods implemented: start-ok (10,11), tune-ok (10,31), open (10,40),
close (10,50), close-ok (10,51), blocked (10,60 — no-op), unblocked (10,61 — no-op).

## Publisher confirms

`confirm.select` enables confirm mode for the channel. Every publish after that gets
exactly one outcome, on the channel's writer chain, in publish-sequence order:

- `confirm.ack(delivery-tag = sequence, multiple = false)` when every target of the
  publish is durable (or the publish was unroutable and therefore had nothing to
  persist).
- `confirm.nack(delivery-tag = sequence, multiple = false, requeue = false)` when any
  target refused, faulted, or exceeded `ConfirmTimeout`.
- A mandatory-unroutable publish receives `basic.return` **before** its confirm.

The confirm sequence is per channel and monotonic. Confirms are never batched with
`multiple=true`. A full `ConfirmQueueCapacity` blocks the publishing connection until a
slot drains; it never drops a publish or invents an answer. A confirm for a channel
number that has since been closed and reused is discarded.

## Queue arguments

`queue.declare` accepts these arguments (non-passive declares only):

| Argument | Wire value | Meaning |
|----------|-----------|---------|
| `x-expires` | non-negative integer (ms) | Delete the queue after that much idle time with no consumers. Messages are dropped, not dead-lettered. |
| `x-max-length` | non-negative integer | Cap on ready messages; overflow drops the oldest. |
| `x-delivery-limit` | non-negative integer | After N deliveries, a requeue dead-letters the message (reason `rejected`). |
| `x-dead-letter-exchange` | string | Dead-letter target. Explicit `""` means the default exchange; absent means no dead-lettering. |
| `x-dead-letter-routing-key` | string | Overrides the routing key on dead-letter copies. |

- Unknown arguments → `not_implemented` (540).
- Non-integral or negative integral values, or a non-string dead-letter name → soft
  `precondition-failed` (406) naming the argument.
- Integral values are clamped to `int.MaxValue` after the non-negative check.
- Redeclaring with different arguments → soft 406 with RabbitMQ's `inequivalent arg`
  explanation; `x-expires` is rendered as its millisecond count.

### Message expiry

The `expiration` content property is a **relative** millisecond string. RedisMQ
converts it to an absolute deadline at publish time:

- Preserved across requeue.
- Cleared on dead-letter copies.
- Invalid or negative values mean no expiry.
- Ready messages are swept by the reaper; unacked messages are not expired.

## Content header (basic class, class 60)

Payload layout: class-id (2) + weight (2, always 0) + body-size (8) + flag-word (2) +
property values.

Flag word: bit `n` (from MSB, bit 15 down to bit 2) marks property `n` present. Bit 0
(0x0001) is the continuation bit; if set, a second flag word follows — RedisMQ rejects
this (only the standard 14-property list is supported).

Properties in wire order (ordinal → flag bit):

| Ordinal | Property | Flag bit | Wire type |
|---------|----------|----------|-----------|
| 0 | content-type | 0x8000 | shortstr |
| 1 | content-encoding | 0x4000 | shortstr |
| 2 | headers | 0x2000 | field table |
| 3 | delivery-mode | 0x1000 | octet |
| 4 | priority | 0x0800 | octet |
| 5 | correlation-id | 0x0400 | shortstr |
| 6 | reply-to | 0x0200 | shortstr |
| 7 | expiration | 0x0100 | shortstr |
| 8 | message-id | 0x0080 | shortstr |
| 9 | timestamp | 0x0040 | timestamp (8 bytes) |
| 10 | type | 0x0020 | shortstr |
| 11 | user-id | 0x0010 | shortstr |
| 12 | app-id | 0x0008 | shortstr |
| 13 | cluster-id | 0x0004 | shortstr |

### RabbitMQ.Client 7.x quirk

RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header values as `byte[]`, not
`string`. RedisMQ correctly emits `'S'` + longstr on the wire. Conformance tests
round-trip the bytes back with `Encoding.UTF8.GetString((byte[])value)`.

## Content assembly (`basic.publish`)

```
basic.publish method  →  channel enters ExpectHeader
content-header frame  →  validate class 60, check body cap (134,217,728), enter ExpectBody
content-body frame(s) →  accumulate until declared size
                        →  empty fragment while owed → hard 501
                        →  fragment overshoots declared → hard 501
complete              →  FinishPublish()
```

`FinishPublish()` order:

1. `immediate=true` → hard 540 `not_implemented`.
2. Permission check: write on the exchange name, or on the routing key for the
   default exchange → soft 403 on refusal.
3. Exchange lookup: absent → soft 404 `not-found` channel close.
4. Default exchange (empty name): route to queue named by the routing key; absent
   queue = unroutable (not not-found).
5. Named exchange: routing per exchange type:
   - `direct` — exact routing-key match.
   - `fanout` — every binding.
   - `topic` — binding key words; `*` matches exactly one word, `#` zero or more.
   - `headers` — binding arguments vs message headers; `x-match=all` (default) or
     `x-match=any`.
6. Zero targets + `mandatory=true` → `basic.return` (312 `NO_ROUTE`) + full content.
7. Zero targets + `mandatory=false` → silent drop.
8. One+ targets → `Enqueue` per queue, collecting one `EnqueueReceipt` each.
9. On a confirm-mode channel, hand the publish to the confirm loop:
   `basic.return` first (if any), then one ack or nack.

## Dead-lettering and `x-death`

A dead-letter copy is produced when a message is rejected without requeue, exceeds
`x-delivery-limit` on requeue, or expires while ready. The copy:

- Resets `delivery-count` to 0 and clears the expiry deadline.
- Carries a header table with `x-death` (newest first). Each entry is a field table:

  | Key | Value |
  |-----|-------|
  | `queue` | Origin queue name |
  | `reason` | `rejected` or `expired` |
  | `count` | Number of times this (queue, reason) pair dead-lettered the message |
  | `time` | Timestamp of the most recent death |
  | `exchange` | Exchange the message was published to originally |

- Sets `x-first-death-queue`, `x-first-death-reason` and `x-first-death-exchange`
  once, on the first death.
- Is dropped silently when the DLX is missing or the copy is unroutable.
- Is dropped when an `x-death` entry for the same origin queue and reason already
  exists (unconditional loop guard).

## Error codes

| Code | Name | Hard/Soft | Typical trigger |
|------|------|-----------|-----------------|
| 311 | ContentTooLarge | Hard | body > 134,217,728 |
| 312 | NoRoute | — | basic.return reply-code |
| 320 | ConnectionForced | — | — |
| 402 | InvalidPath | Hard | unknown vhost |
| 403 | AccessRefused | Hard/Soft | bad PLAIN, unlisted vhost (hard); resource permission, reserved `amq.*` (soft) |
| 404 | NotFound | Soft | unknown exchange/queue |
| 405 | ResourceLocked | Soft | exclusive arbitration |
| 406 | PreconditionFailed | Soft | equivalence mismatch, invalid queue argument, unknown delivery tag |
| 501 | FrameError | Hard | oversize frame, bad body fragment |
| 502 | SyntaxError | Hard | malformed method payload |
| 504 | ChannelError | Hard | second channel.open, expected channel.open |
| 505 | UnexpectedFrame | Soft (M0 pin) | content/method out of phase |
| 530 | NotAllowed | Hard | TuneOk frame-max < 4096 |
| 540 | NotImplemented | Hard | out-of-set method, recover(false), unknown queue argument |
| 541 | InternalError | Hard | encode failure |

## Known deviations from RabbitMQ

| Item | RedisMQ behavior | RabbitMQ reality | Status |
|------|------------------|------------------|--------|
| channel.flow(active=false) | pauses the channel; flow-ok; connection stays open | per-channel pause | Matches (M3) |
| qos global=true | demoted to per-channel | demoted (global_qos flag off in 4.0) | Matches |
| no-local=true | accepted, ignored | accepted, ignored | Matches |
| basic.get | never prefetch-refused; counts toward gate | same (source TODO 1594-1595) | Matches |
| ack(0, multiple=true) | whole ledger | whole ledger (collect_acks) | Matches |
| recover(requeue=false) | hard 540 | hard 540 | Matches |
| Field-table 'S' | emitted as longstr; client decodes byte[] | client decodes byte[] (known quirk) | Client quirk |
| Durable/auto-delete | flags stored and replayed; Redis persistence governs survival | durable queues persist | Documented simplification |
| `amq.*` prefix | refused on creation only | refused on use | Documented deviation |
| Unexpected-frame (505) | soft channel close | hard connection close | M0 pin (report D1) |
| Content-too-large (311) | hard 311 at cap | soft 406 at cap + hard 501 above 512MiB | M0 pin (report D2) |
| basic.return | 312 NO_ROUTE + content | basic_return/5 shape | Matches |
| Requeue placement | queue tail with redelivered=true | RabbitMQ distinguishes placements | Documented simplification |
| x-death on same (queue, reason) | dropped (loop guard) | increments `count` | Documented limitation |
| Unacked replay after crash | ready with `Redelivered=false` | consumer-group dependent | Documented limitation |

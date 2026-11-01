# RedisMQ Protocol Implementation Notes

AMQP 0-9-1 (RabbitMQ dialect) as implemented in RedisMQ M0. This document covers the wire
dialect, the method set, error surface, and known deviations from RabbitMQ.

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

## Connection handshake

```
Client                          Server
  |                               |
  |── "AMQP\0\0\9\1" ───────────>│  (validate header; mismatch → reply server header)
  |                               │
  |<──── connection.start ───────│  (version 0.9, PLAIN, locales)
  |                               │
  |── connection.start-ok ───────>│  (PLAIN response)
  |                               │
  |<──── connection.tune ────────│  (channel-max, frame-max, heartbeat)
  |                               │
  |── connection.tune-ok ───────>│  (negotiated values)
  |                               │
  |── connection.open("/") ──────>│  (unknown vhost → invalid-path 402 hard close)
  |                               │
  |<──── connection.open-ok ─────│  → state = Serving
  |                               │
  |── channel.open ─────────────>│  (per channel number)
  |                               │
  |<──── channel.open-ok ────────│  → channel Running
```

### TuneOk negotiation rules

- Frame-max = min(server offer, client value). A client TuneOk of 0 keeps the server offer.
- A client TuneOk frame-max **below 4096** → hard `not_allowed` (530) connection close. Never clamped up
  (a clamp could push the negotiated max above the client's declared cap — a buffer-overflow hazard).
- Heartbeat adopted verbatim from the client's TuneOk. 0 disables heartbeats entirely (no watchdog).
- Channel-max = min(offer, client), stored but unenforced in M0.
- No second `connection.tune` is ever sent after `connection.tune-ok`.

## Channel method set (M0)

Only these class/method pairs are implemented. Everything else on a channel → hard `not_implemented` (540)
connection close.

| Class | Method | (class,method) | Direction | Content? | M0 behavior |
|-------|--------|----------------|-----------|----------|-------------|
| 20 | channel.open | (20,10) | C→S | No | Starting → Running; open-ok with empty channel-id |
| 20 | channel.open-ok | (20,11) | S→C | No | Reply to channel.open |
| 20 | channel.flow | (20,20) | C→S | No | `active=true` → flow-ok(active=true); `active=false` → hard 540 close |
| 20 | channel.flow-ok | (20,21) | S→C | No | Reply to channel.flow |
| 20 | channel.close | (20,40) | C→S | No | close-ok; retire channel number |
| 20 | channel.close-ok | (20,41) | S→C | No | Retire channel number |
| 40 | exchange.declare | (40,10) | C→S | No | type check → default-exchange refusal → equivalence → create |
| 40 | exchange.declare-ok | (40,11) | S→C | No | Reply |
| 40 | exchange.bind | (40,30) | C→S | No | Not in M0 channel switch → 540 not-implemented |
| 40 | exchange.bind-ok | (40,31) | S→C | No | — |
| 40 | exchange.unbind | (40,40) | C→S | No | 540 not-implemented |
| 40 | exchange.unbind-ok | (40,51) | S→C | No | (note: fixture id is 51, not 41) |
| 50 | queue.declare | (50,10) | C→S | No | reserved-prefix → existence → equivalence → create |
| 50 | queue.declare-ok | (50,11) | S→C | No | Reply with queue/message-count/consumer-count |
| 50 | queue.bind | (50,20) | C→S | No | shortcuts → default-exchange refusal → existence → bind |
| 50 | queue.bind-ok | (50,21) | S→C | No | Reply (unless nowait) |
| 50 | queue.unbind | (50,50) | C→S | No | Always replies unbind-ok |
| 50 | queue.unbind-ok | (50,51) | S→C | No | Reply |
| 60 | basic.qos | (60,10) | C→S | No | count+size stored; global demoted; qos-ok via chain |
| 60 | basic.qos-ok | (60,11) | S→C | No | Reply |
| 60 | basic.consume | (60,20) | C→S | No | queue lookup → tag-reuse → register → consume-ok (both tag flavors) |
| 60 | basic.consume-ok | (60,21) | S→C | No | Reply with consumer-tag |
| 60 | basic.cancel | (60,30) | C→S | No | unregister; cancel-ok echoes any tag |
| 60 | basic.cancel-ok | (60,31) | S→C | No | Reply with consumer-tag |
| 60 | basic.publish | (60,40) | C→S | Yes | content assembly → routing → enqueue / return / drop |
| 60 | basic.return | (60,50) | S→C | Yes | mandatory unroutable: 312 NO_ROUTE + content |
| 60 | basic.deliver | (60,60) | S→C | Yes | pump-delivered to consumer |
| 60 | basic.get | (60,70) | C→S | No | atomic claim → get-ok + content, or get-empty |
| 60 | basic.get-ok | (60,71) | S→C | Yes | delivery-tag/redelivered/exchange/routing-key/message-count |
| 60 | basic.get-empty | (60,72) | S→C | No | cluster-id "" |
| 60 | basic.ack | (60,80) | C→S | No | settle ledger (tag 0/multiple = whole ledger) |
| 60 | basic.reject | (60,90) | C→S | No | settle single; optional requeue |
| 60 | basic.recover-async | (60,100) | C→S | No | 540 not-implemented (outside M0 set) |
| 60 | basic.recover | (60,110) | C→S | No | requeue=true → recover-ok + requeue channel ledger; false → hard 540 |
| 60 | basic.recover-ok | (60,111) | S→C | No | Reply |
| 60 | basic.nack | (60,120) | C→S | No | settle single/multiple; optional requeue |

Connection-class methods implemented: start-ok (10,11), tune-ok (10,31), open (10,40), close (10,50),
close-ok (10,51), blocked (10,60 — no-op), unblocked (10,61 — no-op).

## Content header (basic class, class 60)

Payload layout: class-id (2) + weight (2, always 0) + body-size (8) + flag-word (2) + property values.

Flag word: bit `n` (from MSB, bit 15 down to bit 2) marks property `n` present. Bit 0 (0x0001) is the
continuation bit; if set, a second flag word follows — M0 rejects this (only the standard 14-property
list is supported).

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

RabbitMQ.Client 7.x decodes field-table longstr (`'S'`) header values as `byte[]`, not `string`.
RedisMQ correctly emits `'S'` + longstr on the wire. Conformance tests round-trip the bytes back with
`Encoding.UTF8.GetString((byte[])value)`.

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
2. Exchange lookup: absent → soft 404 `not-found` channel close.
3. Default exchange (empty name): route to queue named by routing key; absent queue = unroutable (not not-found).
4. Named exchange: `IRouteEngine.RouteTo` → binding-insertion order queue list.
5. Zero targets + `mandatory=true` → `basic.return` (312 `NO_ROUTE`) + full content back on channel.
6. Zero targets + `mandatory=false` → silent drop.
7. One+ targets → `Enqueue` per queue; `basic.publish` never replies on success.

## Error codes

| Code | Name | Hard/Soft | Typical trigger |
|------|------|-----------|-----------------|
| 311 | ContentTooLarge | Hard | body > 134,217,728 |
| 312 | NoRoute | — | basic.return reply-code |
| 320 | ConnectionForced | — | — |
| 402 | InvalidPath | Hard | unknown vhost |
| 403 | AccessRefused | Soft | bad PLAIN, reserved `amq.*` |
| 404 | NotFound | Soft | unknown exchange/queue |
| 405 | ResourceLocked | Soft | exclusive arbitration |
| 406 | PreconditionFailed | Soft | equivalence mismatch, unknown delivery tag |
| 501 | FrameError | Hard | oversize frame, bad body fragment |
| 502 | SyntaxError | Hard | malformed method payload |
| 504 | ChannelError | Hard | second channel.open, expected channel.open |
| 505 | UnexpectedFrame | Soft (M0 pin) | content/method out of phase |
| 530 | NotAllowed | Hard | TuneOk frame-max < 4096 |
| 540 | NotImplemented | Hard | out-of-M0 method, channel.flow(active=false), recover(false) |
| 541 | InternalError | Hard | encode failure |

## Known deviations from RabbitMQ

| Item | M0 behavior | RabbitMQ reality | Status |
|------|-------------|------------------|--------|
| channel.flow(active=false) | hard 540 connection close | hard not_implemented via map_exception | Matches |
| qos global=true | demoted to per-channel | demoted (global_qos flag off in 4.0) | Matches |
| no-local=true | accepted, ignored | accepted, ignored | Matches |
| basic.get | never prefetch-refused; counts toward gate | same (source TODO 1594-1595) | Matches |
| ack(0, multiple=true) | whole ledger | whole ledger (collect_acks) | Matches |
| recover(requeue=false) | hard 540 | hard 540 | Matches |
| Field-table 'S' | emitted as longstr; client decodes byte[] | client decodes byte[] (known quirk) | Client quirk |
| Durable/auto-delete | accepted & stored, behave non-durable | durable queues persist | Documented M0 simplification |
| `amq.*` prefix | refused on creation only | refused on use | Documented deviation |
| Unexpected-frame (505) | soft channel close | hard connection close | M0 pin (report D1) |
| Content-too-large (311) | hard 311 at cap | soft 406 at cap + hard 501 above 512MiB | M0 pin (report D2) |
| basic.return | 312 NO_ROUTE + content | basic_return/5 shape | Matches |
| Requeue placement | queue tail with redelivered=true | RabbitMQ distinguishes placements | Documented M0 simplification |

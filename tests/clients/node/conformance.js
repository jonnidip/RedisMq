#!/usr/bin/env node
// amqplib conformance suite for RedisMQ.
//
// Reads the broker coordinates from the environment, runs the nine named
// scenarios, and prints "PASS <id>" on success or "FAIL <id>: <reason>" on
// failure. Progress goes to stderr so stdout stays a clean pass/fail stream.

'use strict';

const amqp = require('amqplib');

const HOST = process.env.REDISMQ_HOST || '127.0.0.1';
const PORT = Number(process.env.REDISMQ_PORT || '5672');
const USER = process.env.REDISMQ_USER || 'guest';
const PASSWORD = process.env.REDISMQ_PASSWORD || 'guest';
const VHOST = process.env.REDISMQ_VHOST || '/';
const WAIT_MS = Number(process.env.REDISMQ_WAIT_SECONDS || '10') * 1000;

class ScenarioFailure extends Error {}

function ensure(condition, message) {
  if (!condition) {
    throw new ScenarioFailure(message);
  }
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function connect() {
  return amqp.connect({
    protocol: 'amqp',
    hostname: HOST,
    port: PORT,
    username: USER,
    password: PASSWORD,
    vhost: VHOST,
    heartbeat: 0,
  });
}

function requireOk(connection) {
  connection.on('error', () => {});
  return connection;
}

async function waitFor(predicate, timeoutMs = WAIT_MS) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await predicate();
    if (result) {
      return result;
    }
    if (Date.now() >= deadline) {
      return null;
    }
    await delay(50);
  }
}

async function getMessage(channel, queue) {
  const message = await channel.get(queue, { noAck: false });
  return message === false ? null : message;
}

async function closeQuietly(channel, connection) {
  try {
    if (channel) {
      await channel.close();
    }
  } catch (ignored) {
    // Already closed by a failed scenario.
  }
  try {
    if (connection) {
      await connection.close();
    }
  } catch (ignored) {
    // Already closed by a failed scenario.
  }
}

async function scenarioConnect() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  try {
    ensure(channel && !channel.closed, 'channel did not open');
  } finally {
    await closeQuietly(channel, connection);
  }
}

async function scenarioRoundtrip() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const exchange = 'conformance.roundtrip.exchange';
  const queue = 'conformance.roundtrip.queue';
  try {
    await channel.assertExchange(exchange, 'direct', { durable: false, autoDelete: false });
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    await channel.bindQueue(queue, exchange, 'rk');
    channel.publish(exchange, 'rk', Buffer.from('roundtrip'));

    const message = await waitFor(() => getMessage(channel, queue));
    ensure(message !== null, 'no message returned within the deadline');
    ensure(message.content.toString() === 'roundtrip', `body mismatch: ${message.content}`);
    channel.ack(message);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    try { await channel.deleteExchange(exchange); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioProperties() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const queue = 'conformance.properties.queue';
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    const sentAt = Math.floor(Date.now() / 1000);
    channel.sendToQueue(queue, Buffer.from('properties'), {
      contentType: 'text/plain',
      correlationId: 'corr-1',
      messageId: 'msg-1',
      timestamp: sentAt,
      headers: { s: 'text', b: true, n: 42 },
    });

    const message = await waitFor(() => getMessage(channel, queue));
    ensure(message !== null, 'no message returned within the deadline');
    ensure(message.content.toString() === 'properties', `body mismatch: ${message.content}`);
    ensure(message.properties.contentType === 'text/plain', `contentType: ${message.properties.contentType}`);
    ensure(message.properties.correlationId === 'corr-1', `correlationId: ${message.properties.correlationId}`);
    ensure(message.properties.messageId === 'msg-1', `messageId: ${message.properties.messageId}`);
    const receivedAt = message.properties.timestamp instanceof Date
      ? Math.floor(message.properties.timestamp.getTime() / 1000)
      : Number(message.properties.timestamp);
    ensure(receivedAt === sentAt, `timestamp: ${message.properties.timestamp}`);
    const headers = message.properties.headers || {};
    ensure(headers.s === 'text', `header s: ${headers.s}`);
    ensure(headers.b === true, `header b: ${headers.b}`);
    ensure(headers.n === 42, `header n: ${headers.n}`);
    channel.ack(message);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioQos() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const queue = 'conformance.qos.queue';
  const received = [];
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    await channel.prefetch(2);
    for (let index = 0; index < 4; index += 1) {
      channel.sendToQueue(queue, Buffer.from(`qos-${index}`));
    }

    const consumer = await channel.consume(queue, (message) => {
      if (message) {
        received.push(message);
      }
    }, { noAck: false });

    await waitFor(() => received.length >= 2);
    ensure(received.length === 2, `expected 2 unacked deliveries, got ${received.length}`);

    await delay(500);
    ensure(received.length === 2, `prefetch exceeded: got ${received.length} deliveries before any ack`);

    channel.ack(received[0]);
    await waitFor(() => received.length >= 3);
    ensure(received.length >= 3, `no delivery after ack: got ${received.length}`);

    for (const message of received.slice(1)) {
      channel.ack(message);
    }
    await channel.cancel(consumer.consumerTag);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioNackRequeue() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const queue = 'conformance.nack.queue';
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    channel.sendToQueue(queue, Buffer.from('nack-me'));

    const first = await waitFor(() => getMessage(channel, queue));
    ensure(first !== null, 'initial delivery missing');
    ensure(first.content.toString() === 'nack-me', `body mismatch: ${first.content}`);
    channel.nack(first, false, true);

    const redelivered = await waitFor(async () => {
      const message = await getMessage(channel, queue);
      return message && message.fields.redelivered ? message : null;
    });
    ensure(redelivered !== null, 'message was not redelivered with the redelivered flag');
    ensure(redelivered.content.toString() === 'nack-me', `redelivered body mismatch: ${redelivered.content}`);
    channel.ack(redelivered);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioAck() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const queue = 'conformance.ack.queue';
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    channel.sendToQueue(queue, Buffer.from('ack-1'));
    channel.sendToQueue(queue, Buffer.from('ack-2'));

    for (let index = 0; index < 2; index += 1) {
      const message = await waitFor(() => getMessage(channel, queue));
      ensure(message !== null, 'acked message missing');
      channel.ack(message);
    }

    await delay(200);
    ensure((await getMessage(channel, queue)) === null, 'queue not empty after acks');
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioBasicGetEmpty() {
  const connection = requireOk(await connect());
  const channel = await connection.createChannel();
  const queue = 'conformance.getempty.queue';
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });

    const empty = await channel.get(queue, { noAck: false });
    ensure(empty === false, `empty get returned ${empty}`);

    channel.sendToQueue(queue, Buffer.from('after-empty'));
    const message = await waitFor(() => getMessage(channel, queue));
    ensure(message !== null, 'channel did not stay open after get-empty');
    ensure(message.content.toString() === 'after-empty', `body mismatch: ${message.content}`);
    channel.ack(message);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioConfirms() {
  const connection = requireOk(await connect());
  const channel = await connection.createConfirmChannel();
  const queue = 'conformance.confirms.queue';
  try {
    await channel.assertQueue(queue, { durable: false, autoDelete: false });
    channel.sendToQueue(queue, Buffer.from('confirmed'));
    await channel.waitForConfirms();

    const message = await waitFor(() => getMessage(channel, queue));
    ensure(message !== null, 'confirmed publish did not reach the queue');
    ensure(message.content.toString() === 'confirmed', `body mismatch: ${message.content}`);
    channel.ack(message);
  } finally {
    try { await channel.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channel, connection);
  }
}

async function scenarioRoundRobin() {
  const connection = requireOk(await connect());
  const channelA = await connection.createChannel();
  const channelB = await connection.createChannel();
  const queue = 'conformance.roundrobin.queue';
  const bodies = [];
  try {
    await channelA.assertQueue(queue, { durable: false, autoDelete: false });
    await channelA.prefetch(1);
    await channelB.prefetch(1);

    const consumerA = await channelA.consume(queue, (message) => {
      if (message) {
        bodies.push(message.content.toString());
        channelA.ack(message);
      }
    }, { noAck: false });
    const consumerB = await channelB.consume(queue, (message) => {
      if (message) {
        bodies.push(message.content.toString());
        channelB.ack(message);
      }
    }, { noAck: false });

    for (let index = 0; index < 10; index += 1) {
      channelA.sendToQueue(queue, Buffer.from(`rr-${index}`));
    }

    await waitFor(() => bodies.length >= 10);
    ensure(bodies.length === 10, `expected 10 deliveries, got ${bodies.length}`);
    const received = [...bodies].sort();
    const expected = Array.from({ length: 10 }, (_, index) => `rr-${index}`).sort();
    ensure(JSON.stringify(received) === JSON.stringify(expected), `delivery set mismatch: ${received}`);

    await channelA.cancel(consumerA.consumerTag);
    await channelB.cancel(consumerB.consumerTag);
  } finally {
    try { await channelA.deleteQueue(queue); } catch (ignored) { /* channel already closed */ }
    await closeQuietly(channelA, connection);
  }
}

const scenarios = [
  ['connect', scenarioConnect],
  ['roundtrip', scenarioRoundtrip],
  ['properties', scenarioProperties],
  ['qos', scenarioQos],
  ['nack-requeue', scenarioNackRequeue],
  ['ack', scenarioAck],
  ['basic-get-empty', scenarioBasicGetEmpty],
  ['confirms', scenarioConfirms],
  ['round-robin', scenarioRoundRobin],
];

async function main() {
  for (const [id, scenario] of scenarios) {
    process.stderr.write(`running ${id}\n`);
    try {
      await scenario();
    } catch (error) {
      console.log(`FAIL ${id}: ${error.message}`);
      process.stderr.write(`${error.stack || error}\n`);
      return 1;
    }
    console.log(`PASS ${id}`);
  }
  return 0;
}

main().then((code) => process.exit(code), (error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exit(1);
});

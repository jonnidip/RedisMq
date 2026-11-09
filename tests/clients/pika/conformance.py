#!/usr/bin/env python3
"""Pika conformance suite for RedisMQ.

Reads the broker coordinates from the environment, runs the nine named
scenarios, and prints "PASS <id>" on success or "FAIL <id>: <reason>" on
failure. Progress and tracebacks go to stderr.
"""

import os
import sys
import time
import traceback

import pika

HOST = os.environ.get("REDISMQ_HOST", "127.0.0.1")
PORT = int(os.environ.get("REDISMQ_PORT", "5672"))
USER = os.environ.get("REDISMQ_USER", "guest")
PASSWORD = os.environ.get("REDISMQ_PASSWORD", "guest")
VHOST = os.environ.get("REDISMQ_VHOST", "/")
WAIT_SECONDS = float(os.environ.get("REDISMQ_WAIT_SECONDS", "10"))

EXCHANGE = "conformance.roundtrip.exchange"
QUEUE = "conformance.roundtrip.queue"


class ScenarioFailure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise ScenarioFailure(message)


def connect():
    parameters = pika.ConnectionParameters(
        host=HOST,
        port=PORT,
        virtual_host=VHOST,
        credentials=pika.PlainCredentials(USER, PASSWORD),
        socket_timeout=WAIT_SECONDS,
    )
    return pika.BlockingConnection(parameters)


def safe(action):
    try:
        action()
    except Exception:
        pass


def wait_for(predicate, timeout=WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    return predicate()


def pump_until(connection, predicate, timeout=WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        connection.process_data_events(time_limit=min(0.1, remaining))
    return predicate()


def get_message(channel, queue):
    method, properties, body = channel.basic_get(queue, auto_ack=False)
    if method is None:
        return None
    return method, properties, body


def scenario_connect():
    connection = connect()
    try:
        channel = connection.channel()
        require(channel.is_open, "channel did not open")
        channel.close()
    finally:
        safe(connection.close)


def scenario_roundtrip():
    connection = connect()
    channel = connection.channel()
    try:
        channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=False, auto_delete=False)
        channel.queue_declare(queue=QUEUE, durable=False, auto_delete=False)
        channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="rk")
        channel.basic_publish(EXCHANGE, "rk", b"roundtrip")

        message = wait_for(lambda: get_message(channel, QUEUE))
        require(message is not None, "no message returned within the deadline")
        method, _properties, body = message
        require(body == b"roundtrip", f"body mismatch: {body!r}")
        channel.basic_ack(method.delivery_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=QUEUE))
        safe(lambda: channel.exchange_delete(exchange=EXCHANGE))
        safe(connection.close)


def scenario_properties():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.properties.queue"
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        sent_at = int(time.time())
        properties = pika.BasicProperties(
            content_type="text/plain",
            correlation_id="corr-1",
            message_id="msg-1",
            timestamp=sent_at,
            headers={"s": "text", "b": True, "n": 42},
        )
        channel.basic_publish("", queue, b"properties", properties=properties)

        message = wait_for(lambda: get_message(channel, queue))
        require(message is not None, "no message returned within the deadline")
        method, received, body = message
        require(body == b"properties", f"body mismatch: {body!r}")
        require(received.content_type == "text/plain", f"content_type: {received.content_type!r}")
        require(received.correlation_id == "corr-1", f"correlation_id: {received.correlation_id!r}")
        require(received.message_id == "msg-1", f"message_id: {received.message_id!r}")
        require(int(received.timestamp) == sent_at, f"timestamp: {received.timestamp!r}")
        headers = received.headers or {}
        require(headers.get("s") == "text", f"header s: {headers.get('s')!r}")
        require(headers.get("b") is True, f"header b: {headers.get('b')!r}")
        require(headers.get("n") == 42, f"header n: {headers.get('n')!r}")
        channel.basic_ack(method.delivery_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=queue))
        safe(connection.close)


def scenario_qos():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.qos.queue"
    received = []
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel.basic_qos(prefetch_count=2)
        for index in range(4):
            channel.basic_publish("", queue, f"qos-{index}".encode())

        def on_message(ch, method, properties, body):
            received.append((method.delivery_tag, body))

        consumer_tag = channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)

        pump_until(connection, lambda: len(received) >= 2)
        require(len(received) == 2, f"expected 2 unacked deliveries, got {len(received)}")

        before = len(received)
        pump_until(connection, lambda: len(received) > before, timeout=0.5)
        require(len(received) == before, f"prefetch exceeded: got {len(received)} deliveries before any ack")

        channel.basic_ack(received[0][0])
        pump_until(connection, lambda: len(received) >= 3)
        require(len(received) >= 3, f"no delivery after ack: got {len(received)}")

        for delivery_tag, _body in received[1:]:
            channel.basic_ack(delivery_tag)
        channel.basic_cancel(consumer_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=queue))
        safe(connection.close)


def scenario_nack_requeue():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.nack.queue"
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel.basic_publish("", queue, b"nack-me")

        first = wait_for(lambda: get_message(channel, queue))
        require(first is not None, "initial delivery missing")
        method, _properties, body = first
        require(body == b"nack-me", f"body mismatch: {body!r}")
        channel.basic_nack(method.delivery_tag, requeue=True)

        redelivered = wait_for(
            lambda: (lambda m: m if m is not None and m[0].redelivered else None)(get_message(channel, queue))
        )
        require(redelivered is not None, "message was not redelivered with the redelivered flag")
        method, _properties, body = redelivered
        require(body == b"nack-me", f"redelivered body mismatch: {body!r}")
        channel.basic_ack(method.delivery_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=queue))
        safe(connection.close)


def scenario_ack():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.ack.queue"
    probe = None
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel.basic_publish("", queue, b"ack-1")
        channel.basic_publish("", queue, b"ack-2")

        for _ in range(2):
            message = wait_for(lambda: get_message(channel, queue))
            require(message is not None, "acked message missing")
            channel.basic_ack(message[0].delivery_tag)

        # Close the settling channel: unacked messages requeue on close, so a
        # broker that ignored the acks cannot hide them in the channel ledger.
        channel.close()
        probe = connection.channel()
        require(wait_for(lambda: get_message(probe, queue) is not None, timeout=0.5) is False,
                "queue not empty after acks")
    finally:
        if probe is not None:
            safe(lambda: probe.queue_delete(queue=queue))
        safe(connection.close)


def scenario_basic_get_empty():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.getempty.queue"
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)

        method, properties, body = channel.basic_get(queue, auto_ack=False)
        require(method is None and properties is None and body is None,
                f"empty get returned {method!r}, {properties!r}, {body!r}")

        channel.basic_publish("", queue, b"after-empty")
        message = wait_for(lambda: get_message(channel, queue))
        require(message is not None, "channel did not stay open after get-empty")
        require(message[2] == b"after-empty", f"body mismatch: {message[2]!r}")
        channel.basic_ack(message[0].delivery_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=queue))
        safe(connection.close)


def scenario_confirms():
    connection = connect()
    channel = connection.channel()
    queue = "conformance.confirms.queue"
    try:
        channel.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel.confirm_delivery()
        # confirm_delivery makes basic_publish block until the broker's basic.ack;
        # a nack raises NackError. Pika 1.4 returns None, older releases returned True.
        confirmed = channel.basic_publish("", queue, b"confirmed", mandatory=False)
        require(confirmed is None or confirmed is True,
                f"publish was not confirmed: {confirmed!r}")

        message = wait_for(lambda: get_message(channel, queue))
        require(message is not None, "confirmed publish did not reach the queue")
        require(message[2] == b"confirmed", f"body mismatch: {message[2]!r}")
        channel.basic_ack(message[0].delivery_tag)
    finally:
        safe(lambda: channel.queue_delete(queue=queue))
        safe(connection.close)


def scenario_round_robin():
    connection = connect()
    channel_a = connection.channel()
    channel_b = connection.channel()
    queue = "conformance.roundrobin.queue"
    bodies = []
    try:
        channel_a.queue_declare(queue=queue, durable=False, auto_delete=False)
        channel_a.basic_qos(prefetch_count=1)
        channel_b.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            bodies.append((ch, method.delivery_tag, body))
            ch.basic_ack(method.delivery_tag)

        tag_a = channel_a.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
        tag_b = channel_b.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)

        for index in range(10):
            channel_a.basic_publish("", queue, f"rr-{index}".encode())

        pump_until(connection, lambda: len(bodies) >= 10)
        require(len(bodies) == 10, f"expected 10 deliveries, got {len(bodies)}")

        received = sorted(body for _ch, _tag, body in bodies)
        expected = sorted(f"rr-{index}".encode() for index in range(10))
        require(received == expected, f"delivery set mismatch: {received!r}")

        from_a = sum(1 for ch, _tag, _body in bodies if ch is channel_a)
        from_b = sum(1 for ch, _tag, _body in bodies if ch is channel_b)
        require(from_a > 0 and from_b > 0,
                f"round-robin did not reach both consumers: A={from_a} B={from_b}")

        channel_a.basic_cancel(tag_a)
        channel_b.basic_cancel(tag_b)
    finally:
        safe(lambda: channel_a.queue_delete(queue=queue))
        safe(connection.close)


SCENARIOS = [
    ("connect", scenario_connect),
    ("roundtrip", scenario_roundtrip),
    ("properties", scenario_properties),
    ("qos", scenario_qos),
    ("nack-requeue", scenario_nack_requeue),
    ("ack", scenario_ack),
    ("basic-get-empty", scenario_basic_get_empty),
    ("confirms", scenario_confirms),
    ("round-robin", scenario_round_robin),
]


def main():
    for scenario_id, scenario in SCENARIOS:
        print(f"running {scenario_id}", file=sys.stderr, flush=True)
        try:
            scenario()
        except Exception as error:  # noqa: BLE001 - report the scenario id with any failure
            print(f"FAIL {scenario_id}: {error}", flush=True)
            traceback.print_exc(file=sys.stderr)
            return 1
        print(f"PASS {scenario_id}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

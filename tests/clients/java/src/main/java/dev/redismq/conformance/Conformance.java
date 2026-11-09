package dev.redismq.conformance;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/// RabbitMQ Java client conformance suite for RedisMQ.
///
/// Reads the broker coordinates from the environment, runs the nine named
/// scenarios, and prints "PASS <id>" on success or "FAIL <id>: <reason>" on
/// failure. Progress goes to stderr so stdout stays a clean pass/fail stream.
public final class Conformance {
    private static final String HOST = env("REDISMQ_HOST", "127.0.0.1");
    private static final int PORT = Integer.parseInt(env("REDISMQ_PORT", "5672"));
    private static final String USER = env("REDISMQ_USER", "guest");
    private static final String PASSWORD = env("REDISMQ_PASSWORD", "guest");
    private static final String VHOST = env("REDISMQ_VHOST", "/");
    private static final long WAIT_MS = Long.parseLong(env("REDISMQ_WAIT_SECONDS", "10")) * 1000L;

    private Conformance() {
    }

    private interface Scenario {
        void run() throws Exception;
    }

    private static final class ScenarioFailure extends RuntimeException {
        ScenarioFailure(String message) {
            super(message);
        }
    }

    public static void main(String[] args) {
        Map<String, Scenario> scenarios = new LinkedHashMap<>();
        scenarios.put("connect", Conformance::connect);
        scenarios.put("roundtrip", Conformance::roundtrip);
        scenarios.put("properties", Conformance::properties);
        scenarios.put("qos", Conformance::qos);
        scenarios.put("nack-requeue", Conformance::nackRequeue);
        scenarios.put("ack", Conformance::ack);
        scenarios.put("basic-get-empty", Conformance::basicGetEmpty);
        scenarios.put("confirms", Conformance::confirms);
        scenarios.put("round-robin", Conformance::roundRobin);

        for (Map.Entry<String, Scenario> entry : scenarios.entrySet()) {
            System.err.println("running " + entry.getKey());
            try {
                entry.getValue().run();
            } catch (Throwable error) {
                System.out.println("FAIL " + entry.getKey() + ": " + describe(error));
                error.printStackTrace(System.err);
                System.exit(1);
            }
            System.out.println("PASS " + entry.getKey());
        }
    }

    private static void connect() throws Exception {
        Connection connection = newConnection();
        try {
            Channel channel = connection.createChannel();
            require(channel.isOpen(), "channel did not open");
            channel.close();
        } finally {
            safe(connection::close);
        }
    }

    private static void roundtrip() throws Exception {
        String exchange = "conformance.roundtrip.exchange";
        String queue = "conformance.roundtrip.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.exchangeDeclare(exchange, "direct", false, false, null);
            channel.queueDeclare(queue, false, false, false, null);
            channel.queueBind(queue, exchange, "rk");
            channel.basicPublish(exchange, "rk", null, "roundtrip".getBytes(StandardCharsets.UTF_8));

            GetResponse response = waitFor(() -> channel.basicGet(queue, false));
            require(response != null, "no message returned within the deadline");
            require("roundtrip".equals(body(response)), "body mismatch: " + body(response));
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(() -> channel.exchangeDelete(exchange));
            safe(connection::close);
        }
    }

    private static void properties() throws Exception {
        String queue = "conformance.properties.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDeclare(queue, false, false, false, null);
            long sentAtSeconds = System.currentTimeMillis() / 1000L;
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put("s", "text");
            headers.put("b", true);
            headers.put("n", 42L);
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("text/plain")
                    .correlationId("corr-1")
                    .messageId("msg-1")
                    .timestamp(new Date(sentAtSeconds * 1000L))
                    .headers(headers)
                    .build();
            channel.basicPublish("", queue, properties, "properties".getBytes(StandardCharsets.UTF_8));

            GetResponse response = waitFor(() -> channel.basicGet(queue, false));
            require(response != null, "no message returned within the deadline");
            require("properties".equals(body(response)), "body mismatch: " + body(response));
            AMQP.BasicProperties received = response.getProps();
            require("text/plain".equals(received.getContentType()), "contentType: " + received.getContentType());
            require("corr-1".equals(received.getCorrelationId()), "correlationId: " + received.getCorrelationId());
            require("msg-1".equals(received.getMessageId()), "messageId: " + received.getMessageId());
            require(received.getTimestamp() != null, "timestamp missing");
            require(received.getTimestamp().getTime() / 1000L == sentAtSeconds,
                    "timestamp: " + received.getTimestamp());
            Map<String, Object> receivedHeaders = received.getHeaders();
            require(receivedHeaders != null, "headers missing");
            require("text".equals(String.valueOf(receivedHeaders.get("s"))),
                    "header s: " + receivedHeaders.get("s"));
            require(Boolean.TRUE.equals(receivedHeaders.get("b")), "header b: " + receivedHeaders.get("b"));
            Object number = receivedHeaders.get("n");
            require(number instanceof Number && ((Number) number).longValue() == 42L,
                    "header n: " + receivedHeaders.get("n"));
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void qos() throws Exception {
        String queue = "conformance.qos.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        List<Long> received = Collections.synchronizedList(new ArrayList<>());
        try {
            channel.queueDeclare(queue, false, false, false, null);
            channel.basicQos(2);
            for (int index = 0; index < 4; index++) {
                channel.basicPublish("", queue, null, ("qos-" + index).getBytes(StandardCharsets.UTF_8));
            }

            CountDownLatch twoArrived = new CountDownLatch(2);
            String consumerTag = channel.basicConsume(queue, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) {
                    received.add(envelope.getDeliveryTag());
                    twoArrived.countDown();
                }
            });

            require(twoArrived.await(WAIT_MS, TimeUnit.MILLISECONDS),
                    "expected 2 unacked deliveries, got " + received.size());
            require(received.size() == 2, "expected 2 unacked deliveries, got " + received.size());

            Thread.sleep(500);
            require(received.size() == 2, "prefetch exceeded: got " + received.size() + " deliveries before any ack");

            channel.basicAck(received.get(0), false);
            require(waitUntil(() -> received.size() >= 3), "no delivery after ack: got " + received.size());

            List<Long> remaining = new ArrayList<>(received.subList(1, received.size()));
            for (Long tag : remaining) {
                channel.basicAck(tag, false);
            }
            channel.basicCancel(consumerTag);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void nackRequeue() throws Exception {
        String queue = "conformance.nack.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDeclare(queue, false, false, false, null);
            channel.basicPublish("", queue, null, "nack-me".getBytes(StandardCharsets.UTF_8));

            GetResponse first = waitFor(() -> channel.basicGet(queue, false));
            require(first != null, "initial delivery missing");
            require("nack-me".equals(body(first)), "body mismatch: " + body(first));
            channel.basicNack(first.getEnvelope().getDeliveryTag(), false, true);

            GetResponse redelivered = waitFor(() -> {
                GetResponse response = channel.basicGet(queue, false);
                return response != null && response.getEnvelope().isRedeliver() ? response : null;
            });
            require(redelivered != null, "message was not redelivered with the redelivered flag");
            require("nack-me".equals(body(redelivered)), "redelivered body mismatch: " + body(redelivered));
            channel.basicAck(redelivered.getEnvelope().getDeliveryTag(), false);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void ack() throws Exception {
        String queue = "conformance.ack.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDeclare(queue, false, false, false, null);
            channel.basicPublish("", queue, null, "ack-1".getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", queue, null, "ack-2".getBytes(StandardCharsets.UTF_8));

            for (int index = 0; index < 2; index++) {
                GetResponse message = waitFor(() -> channel.basicGet(queue, false));
                require(message != null, "acked message missing");
                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
            }

            GetResponse remaining = waitFor(() -> channel.basicGet(queue, false));
            require(remaining == null, "queue not empty after acks: " + body(remaining));
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void basicGetEmpty() throws Exception {
        String queue = "conformance.getempty.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDeclare(queue, false, false, false, null);

            GetResponse empty = channel.basicGet(queue, false);
            require(empty == null, "empty get returned a message: " + body(empty));

            channel.basicPublish("", queue, null, "after-empty".getBytes(StandardCharsets.UTF_8));
            GetResponse message = waitFor(() -> channel.basicGet(queue, false));
            require(message != null, "channel did not stay open after get-empty");
            require("after-empty".equals(body(message)), "body mismatch: " + body(message));
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void confirms() throws Exception {
        String queue = "conformance.confirms.queue";
        Connection connection = newConnection();
        Channel channel = connection.createChannel();
        try {
            channel.queueDeclare(queue, false, false, false, null);
            channel.confirmSelect();
            channel.basicPublish("", queue, null, "confirmed".getBytes(StandardCharsets.UTF_8));
            channel.waitForConfirmsOrDie(WAIT_MS);

            GetResponse message = waitFor(() -> channel.basicGet(queue, false));
            require(message != null, "confirmed publish did not reach the queue");
            require("confirmed".equals(body(message)), "body mismatch: " + body(message));
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        } finally {
            safe(() -> channel.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static void roundRobin() throws Exception {
        String queue = "conformance.roundrobin.queue";
        Connection connection = newConnection();
        Channel channelA = connection.createChannel();
        Channel channelB = connection.createChannel();
        List<String> bodies = Collections.synchronizedList(new ArrayList<>());
        try {
            channelA.queueDeclare(queue, false, false, false, null);
            channelA.basicQos(1);
            channelB.basicQos(1);

            CountDownLatch tenArrived = new CountDownLatch(10);
            String tagA = consumeAndAck(channelA, queue, bodies, tenArrived);
            String tagB = consumeAndAck(channelB, queue, bodies, tenArrived);

            for (int index = 0; index < 10; index++) {
                channelA.basicPublish("", queue, null, ("rr-" + index).getBytes(StandardCharsets.UTF_8));
            }

            require(tenArrived.await(WAIT_MS, TimeUnit.MILLISECONDS),
                    "expected 10 deliveries, got " + bodies.size());
            List<String> received = new ArrayList<>(bodies);
            Collections.sort(received);
            List<String> expected = new ArrayList<>();
            for (int index = 0; index < 10; index++) {
                expected.add("rr-" + index);
            }
            Collections.sort(expected);
            require(received.equals(expected), "delivery set mismatch: " + received);

            channelA.basicCancel(tagA);
            channelB.basicCancel(tagB);
        } finally {
            safe(() -> channelA.queueDelete(queue));
            safe(connection::close);
        }
    }

    private static String consumeAndAck(Channel channel, String queue, List<String> bodies, CountDownLatch latch)
            throws Exception {
        return channel.basicConsume(queue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws java.io.IOException {
                bodies.add(new String(body, StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
                latch.countDown();
            }
        });
    }

    private static Connection newConnection() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        factory.setPort(PORT);
        factory.setUsername(USER);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VHOST);
        factory.setConnectionTimeout((int) WAIT_MS);
        factory.setAutomaticRecoveryEnabled(false);
        return factory.newConnection();
    }

    private static String body(GetResponse response) {
        return response == null ? "<none>" : new String(response.getBody(), StandardCharsets.UTF_8);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new ScenarioFailure(message);
        }
    }

    private static <T> T waitFor(ThrowingSupplier<T> supplier) throws Exception {
        long deadline = System.currentTimeMillis() + WAIT_MS;
        for (;;) {
            T result = supplier.get();
            if (result != null) {
                return result;
            }
            if (System.currentTimeMillis() >= deadline) {
                return null;
            }
            Thread.sleep(50);
        }
    }

    private static boolean waitUntil(BooleanSupplier predicate) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (predicate.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return predicate.getAsBoolean();
    }

    private static void safe(ThrowingRunnable action) {
        try {
            action.run();
        } catch (Exception ignored) {
            // Channel or connection already closed by the failed scenario.
        }
    }

    private static String describe(Throwable error) {
        return Objects.toString(error.getMessage(), error.getClass().getSimpleName());
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}

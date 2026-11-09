package main

// amqp091-go conformance suite for RedisMQ.
//
// Reads the broker coordinates from the environment, runs the nine named
// scenarios, and prints "PASS <id>" on success or "FAIL <id>: <reason>" on
// failure. Progress goes to stderr so stdout stays a clean pass/fail stream.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	host   = env("REDISMQ_HOST", "127.0.0.1")
	port   = env("REDISMQ_PORT", "5672")
	user   = env("REDISMQ_USER", "guest")
	pass   = env("REDISMQ_PASSWORD", "guest")
	vhost  = env("REDISMQ_VHOST", "/")
	wait   = waitDuration()
)

type scenario func() error

func main() {
	scenarios := []struct {
		id string
		fn scenario
	}{
		{"connect", connect},
		{"roundtrip", roundtrip},
		{"properties", properties},
		{"qos", qos},
		{"nack-requeue", nackRequeue},
		{"ack", ack},
		{"basic-get-empty", basicGetEmpty},
		{"confirms", confirms},
		{"round-robin", roundRobin},
	}

	for _, item := range scenarios {
		fmt.Fprintf(os.Stderr, "running %s\n", item.id)
		if err := item.fn(); err != nil {
			fmt.Printf("FAIL %s: %s\n", item.id, err)
			return
		}
		fmt.Printf("PASS %s\n", item.id)
	}
}

func connect() error {
	conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if channel.IsClosed() {
		return errors.New("channel did not open")
	}
	return nil
}

func roundtrip() error {
	const exchange = "conformance.roundtrip.exchange"
	const queue = "conformance.roundtrip.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, exchange)

	if err := channel.ExchangeDeclare(exchange, "direct", false, false, false, false, nil); err != nil {
		return err
	}
	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.QueueBind(queue, "rk", exchange, false, nil); err != nil {
		return err
	}
	if err := channel.PublishWithContext(context.Background(), exchange, "rk", false, false,
		amqp.Publishing{Body: []byte("roundtrip")}); err != nil {
		return err
	}

	delivery, err := waitForGet(channel, queue)
	if err != nil {
		return err
	}
	if string(delivery.Body) != "roundtrip" {
		return fmt.Errorf("body mismatch: %q", delivery.Body)
	}
	return channel.Ack(delivery.DeliveryTag, false)
}

func properties() error {
	const queue = "conformance.properties.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	sentAt := time.Now()
	if err := channel.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		ContentType:   "text/plain",
		CorrelationId: "corr-1",
		MessageId:     "msg-1",
		Timestamp:     sentAt,
		Headers:       amqp.Table{"s": "text", "b": true, "n": int64(42)},
		Body:          []byte("properties"),
	}); err != nil {
		return err
	}

	delivery, err := waitForGet(channel, queue)
	if err != nil {
		return err
	}
	if string(delivery.Body) != "properties" {
		return fmt.Errorf("body mismatch: %q", delivery.Body)
	}
	if delivery.ContentType != "text/plain" {
		return fmt.Errorf("contentType: %q", delivery.ContentType)
	}
	if delivery.CorrelationId != "corr-1" {
		return fmt.Errorf("correlationId: %q", delivery.CorrelationId)
	}
	if delivery.MessageId != "msg-1" {
		return fmt.Errorf("messageId: %q", delivery.MessageId)
	}
	if delivery.Timestamp.Unix() != sentAt.Unix() {
		return fmt.Errorf("timestamp: %s", delivery.Timestamp)
	}
	if value, ok := delivery.Headers["s"].(string); !ok || value != "text" {
		return fmt.Errorf("header s: %v", delivery.Headers["s"])
	}
	if value, ok := delivery.Headers["b"].(bool); !ok || !value {
		return fmt.Errorf("header b: %v", delivery.Headers["b"])
	}
	if value, ok := asInt64(delivery.Headers["n"]); !ok || value != 42 {
		return fmt.Errorf("header n: %v", delivery.Headers["n"])
	}
	return channel.Ack(delivery.DeliveryTag, false)
}

func qos() error {
	const queue = "conformance.qos.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.Qos(2, 0, false); err != nil {
		return err
	}
	for index := 0; index < 4; index++ {
		if err := channel.PublishWithContext(context.Background(), "", queue, false, false,
			amqp.Publishing{Body: []byte(fmt.Sprintf("qos-%d", index))}); err != nil {
			return err
		}
	}

	deliveries, err := channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	received := make([]amqp.Delivery, 0, 4)
	deadline := time.After(wait)
	for len(received) < 2 {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			received = append(received, delivery)
		case <-deadline:
			return fmt.Errorf("expected 2 unacked deliveries, got %d", len(received))
		}
	}
	if len(received) != 2 {
		return fmt.Errorf("expected 2 unacked deliveries, got %d", len(received))
	}

	select {
	case <-deliveries:
		return fmt.Errorf("prefetch exceeded: got more than 2 deliveries before any ack")
	case <-time.After(500 * time.Millisecond):
	}

	if err := channel.Ack(received[0].DeliveryTag, false); err != nil {
		return err
	}

	third := time.After(wait)
	for len(received) < 3 {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			received = append(received, delivery)
		case <-third:
			return fmt.Errorf("no delivery after ack: got %d", len(received))
		}
	}
	for _, delivery := range received[1:] {
		if err := channel.Ack(delivery.DeliveryTag, false); err != nil {
			return err
		}
	}
	return nil
}

func nackRequeue() error {
	const queue = "conformance.nack.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.PublishWithContext(context.Background(), "", queue, false, false,
		amqp.Publishing{Body: []byte("nack-me")}); err != nil {
		return err
	}

	first, err := waitForGet(channel, queue)
	if err != nil {
		return err
	}
	if string(first.Body) != "nack-me" {
		return fmt.Errorf("body mismatch: %q", first.Body)
	}
	if err := channel.Nack(first.DeliveryTag, false, true); err != nil {
		return err
	}

	deadline := time.Now().Add(wait)
	var redelivered amqp.Delivery
	for {
		delivery, ok, err := channel.Get(queue, false)
		if err != nil {
			return err
		}
		if ok && delivery.Redelivered {
			redelivered = delivery
			break
		}
		if ok {
			_ = channel.Nack(delivery.DeliveryTag, false, true)
		}
		if time.Now().After(deadline) {
			return errors.New("message was not redelivered with the redelivered flag")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if string(redelivered.Body) != "nack-me" {
		return fmt.Errorf("redelivered body mismatch: %q", redelivered.Body)
	}
	return channel.Ack(redelivered.DeliveryTag, false)
}

func ack() error {
	const queue = "conformance.ack.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	for index := 0; index < 2; index++ {
		if err := channel.PublishWithContext(context.Background(), "", queue, false, false,
			amqp.Publishing{Body: []byte(fmt.Sprintf("ack-%d", index+1))}); err != nil {
			return err
		}
	}

	for index := 0; index < 2; index++ {
		delivery, err := waitForGet(channel, queue)
		if err != nil {
			return err
		}
		if err := channel.Ack(delivery.DeliveryTag, false); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(wait)
	for {
		_, ok, err := channel.Get(queue, false)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("queue not empty after acks")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func basicGetEmpty() error {
	const queue = "conformance.getempty.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}

	if _, ok, err := channel.Get(queue, false); err != nil {
		return err
	} else if ok {
		return errors.New("empty get returned a message")
	}

	if err := channel.PublishWithContext(context.Background(), "", queue, false, false,
		amqp.Publishing{Body: []byte("after-empty")}); err != nil {
		return err
	}
	delivery, err := waitForGet(channel, queue)
	if err != nil {
		return fmt.Errorf("channel did not stay open after get-empty: %w", err)
	}
	if string(delivery.Body) != "after-empty" {
		return fmt.Errorf("body mismatch: %q", delivery.Body)
	}
	return channel.Ack(delivery.DeliveryTag, false)
}

func confirms() error {
	const queue = "conformance.confirms.queue"

	conn, channel, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channel, queue, "")

	if _, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.Confirm(false); err != nil {
		return err
	}
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := channel.PublishWithContext(context.Background(), "", queue, false, false,
		amqp.Publishing{Body: []byte("confirmed")}); err != nil {
		return err
	}

	select {
	case confirmation, ok := <-confirmations:
		if !ok {
			return errors.New("confirmation channel closed")
		}
		if !confirmation.Ack {
			return errors.New("publish was nacked")
		}
	case <-time.After(wait):
		return errors.New("publish was not confirmed within the deadline")
	}

	delivery, err := waitForGet(channel, queue)
	if err != nil {
		return fmt.Errorf("confirmed publish did not reach the queue: %w", err)
	}
	if string(delivery.Body) != "confirmed" {
		return fmt.Errorf("body mismatch: %q", delivery.Body)
	}
	return channel.Ack(delivery.DeliveryTag, false)
}

func roundRobin() error {
	const queue = "conformance.roundrobin.queue"

	conn, channelA, err := open()
	if err != nil {
		return err
	}
	defer cleanup(conn, channelA, queue, "")
	defer channelA.Close()

	channelB, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channelB.Close()

	if _, err := channelA.QueueDeclare(queue, false, false, false, false, nil); err != nil {
		return err
	}
	if err := channelA.Qos(1, 0, false); err != nil {
		return err
	}
	if err := channelB.Qos(1, 0, false); err != nil {
		return err
	}

	deliveriesA, err := channelA.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	deliveriesB, err := channelB.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	bodies := make([]string, 0, 10)
	ackAll := func(deliveries <-chan amqp.Delivery, channel *amqp.Channel) {
		for delivery := range deliveries {
			bodies = append(bodies, string(delivery.Body))
			_ = channel.Ack(delivery.DeliveryTag, false)
		}
	}
	go ackAll(deliveriesA, channelA)
	go ackAll(deliveriesB, channelB)

	for index := 0; index < 10; index++ {
		if err := channelA.PublishWithContext(context.Background(), "", queue, false, false,
			amqp.Publishing{Body: []byte(fmt.Sprintf("rr-%d", index))}); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(wait)
	for len(bodies) < 10 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(bodies) != 10 {
		return fmt.Errorf("expected 10 deliveries, got %d", len(bodies))
	}

	received := append([]string(nil), bodies...)
	sort.Strings(received)
	expected := make([]string, 0, 10)
	for index := 0; index < 10; index++ {
		expected = append(expected, fmt.Sprintf("rr-%d", index))
	}
	sort.Strings(expected)
	if strings.Join(received, ",") != strings.Join(expected, ",") {
		return fmt.Errorf("delivery set mismatch: %v", received)
	}

	_ = channelA.Cancel("", true)
	_ = channelB.Cancel("", true)
	return nil
}

func dial() (*amqp.Connection, error) {
	conn, err := amqp.Dial(brokerURL())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func open() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := dial()
	if err != nil {
		return nil, nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, channel, nil
}

func cleanup(conn *amqp.Connection, channel *amqp.Channel, queue string, exchange string) {
	if channel != nil {
		if queue != "" {
			_, _ = channel.QueueDelete(queue, false, false, false)
		}
		if exchange != "" {
			_ = channel.ExchangeDelete(exchange, false, false)
		}
		_ = channel.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

func waitForGet(channel *amqp.Channel, queue string) (amqp.Delivery, error) {
	deadline := time.Now().Add(wait)
	for {
		delivery, ok, err := channel.Get(queue, false)
		if err != nil {
			return amqp.Delivery{}, err
		}
		if ok {
			return delivery, nil
		}
		if time.Now().After(deadline) {
			return amqp.Delivery{}, errors.New("no message returned within the deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func brokerURL() string {
	path := "/"
	if vhost != "/" {
		path = "/" + url.PathEscape(vhost)
	}
	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(user, pass),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   path,
	}
	return u.String()
}

func asInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int8:
		return int64(number), true
	case uint8:
		return int64(number), true
	case int16:
		return int64(number), true
	case uint16:
		return int64(number), true
	case int32:
		return int64(number), true
	case uint32:
		return int64(number), true
	case int64:
		return number, true
	case uint64:
		return int64(number), true
	case float32:
		return int64(number), true
	case float64:
		return int64(number), true
	default:
		return 0, false
	}
}

func waitDuration() time.Duration {
	seconds, err := strconv.Atoi(env("REDISMQ_WAIT_SECONDS", "10"))
	if err != nil {
		seconds = 10
	}
	return time.Duration(seconds) * time.Second
}

func env(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

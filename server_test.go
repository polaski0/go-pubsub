package server

import (
	"context"
	"log"
	"net"
	"testing"
	"time"
)

func setup() (*Broker, net.Listener) {
	addr := ":8080"
	ctx := context.Background()
	broker := NewBroker(ctx, addr)
	listener, err := broker.Start()
	if err != nil {
		log.Fatalf("Unable to start the broker server: %v\n", err)
	}
	return broker, listener
}

func TestPublisherRegister(t *testing.T) {
	b, listener := setup()
	defer listener.Close()

	errc := make(chan error)
	go b.Listen(listener, errc)

	addr := "localhost:8080"
	ctx := context.Background()
	topic := "topic"
	producer := NewProducer(ctx, addr, topic)
	conn, err := producer.Start()
	if err != nil {
		t.Error("Unable to start producer server", err)
	}
	defer conn.Close()

	testCase := struct {
		value    string
		exists   bool
		expected string
	}{
		value:    conn.LocalAddr().String(),
		exists:   true,
		expected: topic,
	}

	p, ok := b.Publishers[testCase.value]
	if !ok {
		t.Errorf("Expected %v, found %v\n", testCase.exists, ok)
	}

	if testCase.expected != p {
		t.Errorf("Expected %v, found %v\n", testCase.expected, p)
	}

	for {
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("Error %v\n", err)
			}
		case <-time.After(1 * time.Second):
			return
		}
	}
}

func TestFailedRegister(t *testing.T) {
	b, listener := setup()
	defer listener.Close()

	errc := make(chan error)
	go b.Listen(listener, errc)

	addr := "localhost:8080"
	ctx := context.Background()
	topic := ""
	producer := NewProducer(ctx, addr, topic)
	conn, err := producer.Start()
	if err == nil && conn != nil {
		t.Error("Should not start the server since unregistered")
	}
}

func TestParseRequest(t *testing.T) {
	testCases := []struct {
		value    []byte
		expected struct {
			action  string
			content []string
		}
	}{
		{
			value: []byte("register^]subscriber"),
			expected: struct {
				action  string
				content []string
			}{
				action:  "register",
				content: []string{"subscriber"},
			},
		},
		{
			value: []byte("register^]publisher^]topic"),
			expected: struct {
				action  string
				content []string
			}{
				action:  "register",
				content: []string{"publisher", "topic"},
			},
		},
	}

	for _, c := range testCases {
		action, content := ParseRequest(c.value)
		if c.expected.action != action {
			t.Errorf("Expected %v, found %v\n", c.expected.action, action)
		}

		if len(content) != len(c.expected.content) {
			t.Errorf("Expected %v, found %v\n", len(c.expected.content), len(content))
		}

		for i, v := range content {
			if c.expected.content[i] != v {
				t.Errorf("Expected %v, found %v\n", c.expected.content[i], v)
			}
		}
	}
}

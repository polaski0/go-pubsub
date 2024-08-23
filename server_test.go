package server

import (
	"context"
	"fmt"
	"log"
	"testing"
)

func setup() *Broker {
	addr := ":8080"
	ctx := context.Background()
	broker := NewBroker(ctx, addr)
	listener, err := broker.Start()
	fmt.Println("Started broker server on:", listener.Addr().String())
	if err != nil {
		log.Fatal("Unable to start the broker server", err)
	}
	go func() {
		err := broker.Listen(listener)
		if err != nil {
			listener.Close()
			log.Fatal("Error occured while running the server", err)
		}
	}()
	return broker
}

func TestSubscription(t *testing.T) {
	b := setup()

	addr := "localhost:8080"
	ctx := context.Background()
	topic := "topic"
	producer := NewProducer(ctx, addr, topic)
	conn, err := producer.Start()
	fmt.Printf("Started producer on `%v` with topic `%v`\n", conn.LocalAddr().String(), producer.Topic)
	if err != nil {
		t.Error("Unable to start producer server", err)
	}

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

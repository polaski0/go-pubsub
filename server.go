package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
)

type Topic struct {
	Value []byte
}

type Broker struct {
	ctx         context.Context
	mux         *sync.Mutex
	Addr        string
	Topics      []string
	Publishers  map[string]string     // address of publisher = topic (one topic per publisher only)
	Subscribers map[string][]net.Conn // topic = addresses of consumers subscribed to the topic
}

func NewBroker(ctx context.Context, addr string) *Broker {
	return &Broker{
		ctx:         ctx,
		mux:         &sync.Mutex{},
		Addr:        addr,
		Topics:      []string{},
		Subscribers: map[string][]net.Conn{},
	}
}

func (b *Broker) Start() (net.Listener, error) {
	listener, err := net.Listen("tcp", b.Addr)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func (b *Broker) Listen(listener net.Listener) error {
	errc := make(chan error)

	for {
		select {
		case err := <-errc:
			return err
		default:
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			go b.handleConnection(conn, errc)
		}
	}
}

func (b *Broker) handleConnection(conn net.Conn, errc chan error) {
	reqc := make(chan any) // Fix receiving channel

	for {
		body, err := b.read(conn)
		if err != nil {
			errc <- err
		}

		action, content := ParseRequest(body)

		// Do necessary operations based on the action received
		switch action {
		case "register":
		case "pub":
			reqc <- content
		case "sub":
		case "unsub":
		}

	}
}

func (b *Broker) read(conn net.Conn) ([]byte, error) {
	buff := make([]byte, 1024*8)
	n, err := conn.Read(buff)
	if err != nil {
		return nil, err
	}
	fmt.Println("Received message: ", string(buff[:n]))
	return buff[:n], nil
}

func (b *Broker) write(conn net.Conn, value []byte) error {
	return nil
}

// Parse contents of the body to determine
// the type of the request.
//
// The format of the body should be:
//
//	<action>^]<content>...^]<content>
//
// where every values is delimited with `^]` to
// indicate that they are separate from the previous one.
//
// e.g.
//
//   - register^]subscriber -- the request is a subscriber
//   - register^]publisher^]topic -- the request is a publisher of `topic`
//   - pub^]topic -- the request is from a publisher publishing on its own `topic`
//   - sub^]topic -- the request is from a subscriber subscribing to `topic`
//   - unsub^]topic -- the request is from a subscriber unsubscribing to `topic`
func ParseRequest(b []byte) (string, []string) {
	sep := []byte("^]")
	split := bytes.SplitAfter(b, sep)
	if len(split) == 0 {
		return "", nil
	}

	contents := []string{}
	for _, c := range split[1:] {
		contents = append(contents, string(c))
	}
	return string(split[0]), contents
}

// Add subscriber to the `Subscribers` map
func (b *Broker) Subscribe(conn net.Conn) error {
	return nil
}

// Remove subscriber from the `Subscribers` map
func (b *Broker) Unsubscribe(topic string, conn net.Conn) error {
	return nil
}

type Producer struct {
	ctx   context.Context
	Addr  string
	Topic string
}

func NewProducer(ctx context.Context, addr string, topic string) *Producer {
	return &Producer{
		ctx:   ctx,
		Addr:  addr,
		Topic: topic,
	}
}

// Producers must send a data that
// tags them as a producer
func (p *Producer) Start() (net.Conn, error) {
	conn, err := net.Dial("tcp", p.Addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type Consumer struct {
	ctx           context.Context
	Addr          string
	Subscriptions map[string]bool // topics = is subscribed
}

func NewConsumer(ctx context.Context, addr string) *Consumer {
	return &Consumer{
		ctx:           ctx,
		Addr:          addr,
		Subscriptions: map[string]bool{},
	}
}

// Consumers must send a data that
// tags them as a consumer
func (c *Consumer) Start() (net.Conn, error) {
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Add topic to `Subscriptions` map with a value of true
func (c *Consumer) Subscribe(topic string) error {
	return nil
}

// Convert value from true to false if the topic exists
func (c *Consumer) Unsubscribe(topic string) error {
	return nil
}

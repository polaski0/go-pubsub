package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
)

const (
	DELIMITER = "^]"
)

type Service interface {
	Register(net.Conn) error
}

type Topic struct {
	Value []byte
}

type Broker struct {
	ctx         context.Context
	mux         *sync.Mutex
	Addr        string
	Topics      []string
	Publishers  map[string]string     // address of producer = topic (one topic per producer only)
	Subscribers map[string][]net.Conn // topic = addresses of consumers subscribed to the topic
}

func NewBroker(ctx context.Context, addr string) *Broker {
	return &Broker{
		ctx:         ctx,
		mux:         &sync.Mutex{},
		Addr:        addr,
		Topics:      []string{},
		Publishers:  map[string]string{},
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

func (b *Broker) Listen(listener net.Listener, errc chan error) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			errc <- err
			return
		}
		go b.handleConnection(conn, errc)
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
			serviceType := content[0]
			// Check if producer or consumer
			switch serviceType {
			case "producer":
				if len(content) < 2 || content[1] == "" {
					_, _ = conn.Write([]byte("NOT"))
					errc <- errors.New("Topic required")
					return
				}

				b.Publishers[conn.RemoteAddr().String()] = content[1]
			case "consumer":
			default:
				errc <- errors.New("Invalid service type")
				return
			}

			_, err := conn.Write([]byte("OK"))
			if err != nil {
				errc <- err
			}
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
//   - register^]consumer -- the request is a consumer
//   - register^]producer^]topic -- the request is a producer of `topic`
//   - pub^]topic -- the request is from a producer publishing on its own `topic`
//   - sub^]topic -- the request is from a consumer subscribing to `topic`
//   - unsub^]topic -- the request is from a consumer unsubscribing to `topic`
func ParseRequest(b []byte) (string, []string) {
	sep := []byte(DELIMITER)
	split := bytes.Split(b, sep)
	if len(split) == 0 {
		return "", nil
	}

	contents := []string{}
	for _, c := range split[1:] {
		contents = append(contents, string(c))
	}
	return string(split[0]), contents
}

// Add consumer to the `Subscribers` map
func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mux.Lock()
	defer b.mux.Unlock()
	consumers, ok := b.Subscribers[topic]
	if !ok {
		b.Subscribers[topic] = []net.Conn{conn}
		return
	}

	if !slices.Contains(consumers, conn) {
		consumers = append(consumers, conn)
		b.Subscribers[topic] = consumers
	}
}

// Remove consumer from the `Subscribers` map
func (b *Broker) Unsubscribe(topic string, conn net.Conn) {
	b.mux.Lock()
	defer b.mux.Unlock()
	consumers, ok := b.Subscribers[topic]
	if !ok {
		return
	}

	for i, s := range consumers {
		if s == conn {
			consumers = append(consumers[:i], consumers...)
			b.Subscribers[topic] = consumers
			break
		}
	}
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

	// Register service on start-up
	err = p.Register(conn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *Producer) Register(conn net.Conn) error {
	_, err := conn.Write([]byte("register" + DELIMITER + "producer" + DELIMITER + p.Topic))
	if err != nil {
		return err
	}

	buff := make([]byte, 128)
	n, err := conn.Read(buff)
	if err != nil {
		return err
	}
	if string(buff[:n]) != "OK" {
		return errors.New("Unable to register service")
	}
	return nil
}

func (p *Producer) Publish(conn net.Conn, body string) error {
	_, err := conn.Write([]byte(body))
	if err != nil {
		return err
	}
	return nil
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

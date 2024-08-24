package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"

	server "github.com/polaski0/go-pubsub"
)

const (
	addr = "localhost:8080"
)

func main() {
	topic := flag.String("topic", "", "Topic where the producer will subscribe")
	flag.Parse()

	if *topic == "" || topic == nil {
		fmt.Println("Topic is required")
		os.Exit(1)
	}

	ctx := context.Background()
	consumer := server.NewProducer(ctx, addr, *topic)
	conn, err := consumer.Start()
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Producer connected to broker")

	b := make(chan []byte)
	go startRepl(b)

	for v := range b {
		_, err := conn.Write(v)
		if err != nil {
			fmt.Println("Write Error:", err)
			return
		}
		fmt.Println("Published message:", string(v))
	}
}

func startRepl(b chan []byte) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		ok := scanner.Scan()
		if !ok {
			return
		}
		t := scanner.Text()
		b <- []byte(t)
	}
}

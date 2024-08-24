package main

import (
	"context"
	"fmt"
	"os"
	"time"

	server "github.com/polaski0/go-pubsub"
)

const (
	port = ":8080"
)

func main() {
	ctx := context.Background()
	broker := server.NewBroker(ctx, port)
	listener, err := broker.Start()
	if err != nil {
		fmt.Println("Error starting the broker:", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Broker is running on port", port)

	errc := make(chan error)
	go broker.Listen(listener, errc)

	for err := range errc {
		if err != nil {
			fmt.Printf("[%v] Error occured: %v", time.Now(), err)
		}
	}
}

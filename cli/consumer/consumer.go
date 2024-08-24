package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	server "github.com/polaski0/go-pubsub"
)

const (
	addr = "localhost:8080"
)

func main() {
	ctx := context.Background()
	consumer := server.NewConsumer(ctx, addr)
	conn, err := consumer.Start()
	if err != nil {
		fmt.Println("Error connecting to broker:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Subscriber connected to broker")

	b := make(chan []byte)
	go startRepl(b)

	go func() {
		for v := range b {
			_, err := conn.Write([]byte(v))
			if err != nil {
				fmt.Println("Error writing message:", err)
				return
			}
			fmt.Println("Message sent")
		}
	}()

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading message:", err)
			return
		}
		fmt.Println("Received message:", string(buffer[:n]))
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

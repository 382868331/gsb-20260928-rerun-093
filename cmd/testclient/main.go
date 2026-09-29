package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	delivery "message-delivery"
)

func main() {
	dir := flag.String("data", "./runtime-data", "local broker data directory")
	flag.Parse()

	b, err := delivery.Open(*dir, delivery.WithLeaseDuration(80*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	fatal := func(err error) {
		_ = b.Close()
		log.Fatal(err)
	}
	if err := b.CreateTopic("orders"); err != nil && err != delivery.ErrAlreadyExists {
		fatal(err)
	}
	if err := b.Subscribe("orders", "workers", delivery.SubscribeOptions{Position: delivery.StartAtEarliest}); err != nil {
		fatal(err)
	}
	msg, err := b.Publish("orders", "test order")
	if err != nil {
		fatal(err)
	}
	first, err := b.Receive(context.Background(), "orders", "workers", "consumer-a")
	if err != nil {
		fatal(err)
	}
	fmt.Printf("delivered seq=%d body=%q holder=%s attempt=%d\n", first.Seq, first.Body, first.ConsumerID, first.Attempt)
	time.Sleep(120 * time.Millisecond)
	second, err := b.Receive(context.Background(), "orders", "workers", "consumer-b")
	if err != nil {
		fatal(err)
	}
	fmt.Printf("transferred seq=%d holder=%s attempt=%d\n", second.Seq, second.ConsumerID, second.Attempt)
	if err := b.Ack("orders", "workers", first.ConsumerID, first.Token, first.Seq); err == nil {
		_ = b.Close()
		fmt.Fprintln(os.Stderr, "stale ack unexpectedly succeeded")
		os.Exit(1)
	} else {
		fmt.Printf("rejected stale holder=%s token=%s: %v\n", first.ConsumerID, first.Token, err)
	}
	if err := b.Ack("orders", "workers", second.ConsumerID, second.Token, second.Seq); err != nil {
		fatal(err)
	}
	if err := b.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("confirmed message=%s final-holder=consumer-b\n", msg.ID)
}

// Producer-only compatibility oracle for pqueue-kafka.
//
// Verifies that pqueue-kafka speaks the Kafka producer wire protocol correctly
// using franz-go, an independent Go Kafka implementation.
//
// Tests: ApiVersions → Metadata → Produce (no consumer APIs).
//
// Usage: go run . <bootstrap-servers> <topic>
// Exit 0 on success; non-zero on any deviation.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: main <bootstrap-servers> <topic>")
		os.Exit(1)
	}
	bootstrap := os.Args[1]
	topic := os.Args[2]

	if err := run(bootstrap, topic); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func run(bootstrap, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := check("produce", func() error {
		return produce(ctx, bootstrap, topic, 5)
	}); err != nil {
		return err
	}

	return nil
}

func check(name string, fn func() error) error {
	if err := fn(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	fmt.Printf("  ok  %s\n", name)
	return nil
}

func produce(ctx context.Context, bootstrap, topic string, n int) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DefaultProduceTopic(topic),
		// Disable idempotency (pqueue-kafka P2 only supports non-idempotent producer).
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		return fmt.Errorf("new producer: %w", err)
	}
	defer cl.Close()

	for i := 0; i < n; i++ {
		res := cl.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("val-%d", i)),
		})
		if err := res.FirstErr(); err != nil {
			return fmt.Errorf("record %d: %w", i, err)
		}
	}
	return nil
}

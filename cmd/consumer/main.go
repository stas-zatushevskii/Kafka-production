package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"kafka-production-2/internal/app"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

func main() {
	expectedCount := flag.Int("expected-count", 3, "How many messages must be read")
	timeoutSeconds := flag.Int("timeout-seconds", 20, "Overall timeout for message consumption")
	flag.Parse()

	settings := app.LoadSettings()
	registry, err := app.NewSchemaRegistryClient(settings)
	fatalIfErr(err)

	dialer, err := settings.KafkaDialer()
	fatalIfErr(err)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        settings.BootstrapServers,
		GroupID:        "orders-validation-" + uuid.NewString(),
		Topic:          settings.Topic,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        2 * time.Second,
		CommitInterval: time.Second,
		Dialer:         dialer,
	})
	defer reader.Close()

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSeconds)*time.Second)
	defer cancel()

	received := 0
	for received < *expectedCount {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			fatalIfErr(fmt.Errorf("expected %d messages, got %d: %w", *expectedCount, received, err))
		}

		_, event, err := app.DecodeConfluentAvro(registry, message.Value)
		fatalIfErr(err)

		received++
		fatalIfErr(encoder.Encode(map[string]any{
			"status":    "received",
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"key":       string(message.Key),
			"value":     event,
		}))
	}
}

func fatalIfErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

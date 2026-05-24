package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"kafka-production-2/internal/app"

	"github.com/segmentio/kafka-go"
)

func main() {
	inputPath := flag.String("input", "data/orders.json", "Path to JSON array with test messages")
	flag.Parse()

	settings := app.LoadSettings()
	registry, err := app.NewSchemaRegistryClient(settings)
	fatalIfErr(err)

	schemaID, schema, err := registry.GetOrRegisterSchema(settings.SchemaPath)
	fatalIfErr(err)

	data, err := os.ReadFile(*inputPath)
	fatalIfErr(err)

	var orders []app.OrderEvent
	fatalIfErr(json.Unmarshal(data, &orders))

	transport, err := settings.KafkaTransport()
	fatalIfErr(err)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(settings.BootstrapServers...),
		Topic:        settings.Topic,
		RequiredAcks: kafka.RequireAll,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 50 * time.Millisecond,
		Transport:    transport,
	}
	defer writer.Close()

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for index, order := range orders {
		valueBytes, err := app.EncodeConfluentAvro(schema, schemaID, order)
		fatalIfErr(err)

		messages := []kafka.Message{{
			Key:   []byte(order.OrderID),
			Value: valueBytes,
		}}
		fatalIfErr(writer.WriteMessages(ctx, messages...))

		fatalIfErr(encoder.Encode(map[string]any{
			"status":        "sent",
			"message_index": index + 1,
			"topic":         settings.Topic,
			"order_id":      order.OrderID,
		}))
	}
}

func fatalIfErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

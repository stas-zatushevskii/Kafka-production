package app

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
)

func EncodeConfluentAvro(schema avro.Schema, schemaID int, event OrderEvent) ([]byte, error) {
	avroPayload, err := avro.Marshal(schema, event)
	if err != nil {
		return nil, fmt.Errorf("marshal avro payload: %w", err)
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 5+len(avroPayload)))
	buffer.WriteByte(0)
	if err := binary.Write(buffer, binary.BigEndian, uint32(schemaID)); err != nil {
		return nil, fmt.Errorf("encode schema id: %w", err)
	}
	if _, err := buffer.Write(avroPayload); err != nil {
		return nil, fmt.Errorf("write avro payload: %w", err)
	}
	return buffer.Bytes(), nil
}

func DecodeConfluentAvro(client *SchemaRegistryClient, payload []byte) (int, OrderEvent, error) {
	if len(payload) < 5 {
		return 0, OrderEvent{}, fmt.Errorf("payload is too short for Confluent wire format")
	}
	if payload[0] != 0 {
		return 0, OrderEvent{}, fmt.Errorf("unexpected magic byte: %d", payload[0])
	}

	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	schema, err := client.GetSchemaByID(schemaID)
	if err != nil {
		return 0, OrderEvent{}, err
	}

	var event OrderEvent
	if err := avro.Unmarshal(schema, payload[5:], &event); err != nil {
		return 0, OrderEvent{}, fmt.Errorf("unmarshal avro payload: %w", err)
	}

	return schemaID, event, nil
}

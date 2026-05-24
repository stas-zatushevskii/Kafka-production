package app

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Settings struct {
	BootstrapServers       []string
	Topic                  string
	SchemaRegistryURL      string
	SchemaSubject          string
	SchemaPath             string
	SecurityProtocol       string
	SASLMechanism          string
	Username               string
	Password               string
	SSLCaFile              string
	SchemaRegistryUsername string
	SchemaRegistryPassword string
	SchemaRegistryCAFile   string
}

func LoadSettings() Settings {
	topic := getenv("KAFKA_TOPIC", "orders")
	username := getenv("KAFKA_USERNAME", "")
	password := getenv("KAFKA_PASSWORD", "")
	cafile := getenv("KAFKA_SSL_CAFILE", "")

	return Settings{
		BootstrapServers: splitCSV(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")),
		Topic:            topic,
		SchemaRegistryURL: getenv(
			"SCHEMA_REGISTRY_URL",
			"http://localhost:8081",
		),
		SchemaSubject:          getenv("SCHEMA_SUBJECT", topic+"-value"),
		SchemaPath:             getenv("SCHEMA_PATH", "schemas/order-value.avsc"),
		SecurityProtocol:       strings.ToUpper(getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")),
		SASLMechanism:          strings.ToUpper(getenv("KAFKA_SASL_MECHANISM", "")),
		Username:               username,
		Password:               password,
		SSLCaFile:              cafile,
		SchemaRegistryUsername: getenv("SCHEMA_REGISTRY_USERNAME", username),
		SchemaRegistryPassword: getenv("SCHEMA_REGISTRY_PASSWORD", password),
		SchemaRegistryCAFile:   getenv("SCHEMA_REGISTRY_CA_FILE", cafile),
	}
}

func (s Settings) KafkaDialer() (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{
		Timeout:   15 * time.Second,
		DualStack: true,
	}

	transportTLS, err := s.tlsConfig()
	if err != nil {
		return nil, err
	}
	if transportTLS != nil {
		dialer.TLS = transportTLS
	}

	mechanism, err := s.saslMechanism()
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		dialer.SASLMechanism = mechanism
	}

	return dialer, nil
}

func (s Settings) KafkaTransport() (*kafka.Transport, error) {
	tlsConfig, err := s.tlsConfig()
	if err != nil {
		return nil, err
	}
	mechanism, err := s.saslMechanism()
	if err != nil {
		return nil, err
	}
	return &kafka.Transport{
		TLS:  tlsConfig,
		SASL: mechanism,
	}, nil
}

func (s Settings) tlsConfig() (*tls.Config, error) {
	switch s.SecurityProtocol {
	case "PLAINTEXT", "SASL_PLAINTEXT":
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if s.SSLCaFile == "" {
		return tlsConfig, nil
	}

	certPath, err := expandPath(s.SSLCaFile)
	if err != nil {
		return nil, err
	}
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("append CA file %q: no certificates loaded", certPath)
	}
	tlsConfig.RootCAs = pool
	return tlsConfig, nil
}

func (s Settings) saslMechanism() (sasl.Mechanism, error) {
	switch s.SecurityProtocol {
	case "PLAINTEXT", "SSL":
		return nil, nil
	}

	if s.Username == "" || s.Password == "" {
		return nil, fmt.Errorf("KAFKA_USERNAME and KAFKA_PASSWORD are required for %s", s.SecurityProtocol)
	}

	switch s.SASLMechanism {
	case "", "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, s.Username, s.Password)
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, s.Username, s.Password)
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", s.SASLMechanism)
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func expandPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	expanded := os.ExpandEnv(path)
	if strings.HasPrefix(expanded, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		expanded = filepath.Join(home, strings.TrimPrefix(expanded, "~"))
	}
	return expanded, nil
}

package app

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

type SchemaRegistryClient struct {
	baseURL string
	subject string
	client  *http.Client
	auth    *basicAuth

	mu      sync.RWMutex
	schemas map[int]avro.Schema
}

type basicAuth struct {
	username string
	password string
}

func NewSchemaRegistryClient(settings Settings) (*SchemaRegistryClient, error) {
	httpClient := &http.Client{
		Timeout: 20 * time.Second,
	}

	if strings.HasPrefix(strings.ToLower(settings.SchemaRegistryURL), "https://") {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if settings.SchemaRegistryCAFile != "" {
			caPath, err := expandPath(settings.SchemaRegistryCAFile)
			if err != nil {
				return nil, err
			}
			pemBytes, err := os.ReadFile(caPath)
			if err != nil {
				return nil, fmt.Errorf("read schema registry CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pemBytes) {
				return nil, fmt.Errorf("append schema registry CA file %q: no certificates loaded", caPath)
			}
			tlsConfig.RootCAs = pool
		}
		httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	var auth *basicAuth
	if settings.SchemaRegistryUsername != "" || settings.SchemaRegistryPassword != "" {
		auth = &basicAuth{
			username: settings.SchemaRegistryUsername,
			password: settings.SchemaRegistryPassword,
		}
	}

	return &SchemaRegistryClient{
		baseURL: strings.TrimRight(settings.SchemaRegistryURL, "/"),
		subject: settings.SchemaSubject,
		client:  httpClient,
		auth:    auth,
		schemas: make(map[int]avro.Schema),
	}, nil
}

func (c *SchemaRegistryClient) GetOrRegisterSchema(schemaPath string) (int, avro.Schema, error) {
	if schemaID, schema, err := c.GetLatestSchema(); err == nil {
		return schemaID, schema, nil
	}

	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return 0, nil, fmt.Errorf("read schema file: %w", err)
	}

	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		return 0, nil, fmt.Errorf("parse avro schema: %w", err)
	}

	payload := map[string]string{
		"schemaType": "AVRO",
		"schema":     string(schemaBytes),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal register payload: %w", err)
	}

	responseBody, err := c.request(
		http.MethodPost,
		"/subjects/"+c.subject+"/versions",
		body,
		"application/vnd.schemaregistry.v1+json",
		http.StatusOK,
	)
	if err != nil {
		return 0, nil, err
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return 0, nil, fmt.Errorf("decode register response: %w", err)
	}

	c.storeSchema(result.ID, schema)
	return result.ID, schema, nil
}

func (c *SchemaRegistryClient) GetLatestSchema() (int, avro.Schema, error) {
	responseBody, err := c.request(
		http.MethodGet,
		"/subjects/"+c.subject+"/versions/latest",
		nil,
		"",
		http.StatusOK,
		http.StatusNotFound,
	)
	if err != nil {
		return 0, nil, err
	}
	if responseBody == nil {
		return 0, nil, fmt.Errorf("schema subject %q not found", c.subject)
	}

	var result struct {
		ID     int    `json:"id"`
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return 0, nil, fmt.Errorf("decode latest schema response: %w", err)
	}

	schema, err := avro.Parse(result.Schema)
	if err != nil {
		return 0, nil, fmt.Errorf("parse latest schema: %w", err)
	}
	c.storeSchema(result.ID, schema)
	return result.ID, schema, nil
}

func (c *SchemaRegistryClient) GetSchemaByID(schemaID int) (avro.Schema, error) {
	c.mu.RLock()
	cached, ok := c.schemas[schemaID]
	c.mu.RUnlock()
	if ok {
		return cached, nil
	}

	responseBody, err := c.request(
		http.MethodGet,
		fmt.Sprintf("/schemas/ids/%d", schemaID),
		nil,
		"",
		http.StatusOK,
	)
	if err != nil {
		return nil, err
	}

	var result struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, fmt.Errorf("decode schema by id response: %w", err)
	}

	schema, err := avro.Parse(result.Schema)
	if err != nil {
		return nil, fmt.Errorf("parse schema by id: %w", err)
	}
	c.storeSchema(schemaID, schema)
	return schema, nil
}

func (c *SchemaRegistryClient) storeSchema(schemaID int, schema avro.Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[schemaID] = schema
}

func (c *SchemaRegistryClient) request(
	method string,
	path string,
	body []byte,
	contentType string,
	expectedStatuses ...int,
) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if c.auth != nil {
		request.SetBasicAuth(c.auth.username, c.auth.password)
	}

	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	for _, status := range expectedStatuses {
		if response.StatusCode == status {
			if response.StatusCode == http.StatusNotFound {
				return nil, nil
			}
			return responseBody, nil
		}
	}

	return nil, fmt.Errorf("%s %s failed: %d %s", method, path, response.StatusCode, string(responseBody))
}

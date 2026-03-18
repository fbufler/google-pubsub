//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestRESTGateway_CreateAndListTopics(t *testing.T) {
	host := emulatorHost()
	// The gateway runs on the same port but requires HTTP formatting
	baseURL := fmt.Sprintf("http://%s", host)
	projID := projectID()
	topicID := uniqueName("rest-topic")

	topicName := fmt.Sprintf("projects/%s/topics/%s", projID, topicID)
	createURL := fmt.Sprintf("%s/v1/%s", baseURL, topicName)

	// 1. Create a Topic using HTTP PUT
	req, err := http.NewRequest(http.MethodPut, createURL, bytes.NewBuffer([]byte("{}")))
	if err != nil {
		t.Fatalf("NewRequest PUT: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK for topic creation, got %d: %s", resp.StatusCode, body)
	}

	// 2. List Topics using HTTP GET
	listURL := fmt.Sprintf("%s/v1/projects/%s/topics", baseURL, projID)
	respGet, err := client.Get(listURL)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	defer respGet.Body.Close()

	if respGet.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(respGet.Body)
		t.Fatalf("expected 200 OK for listing topics, got %d: %s", respGet.StatusCode, body)
	}

	// 3. Parse the JSON response
	var listResp struct {
		Topics []struct {
			Name string `json:"name"`
		} `json:"topics"`
	}
	if err := json.NewDecoder(respGet.Body).Decode(&listResp); err != nil {
		t.Fatalf("failed to decode GET response: %v", err)
	}

	// 4. Verify our created topic is in the REST response
	found := false
	for _, t := range listResp.Topics {
		if t.Name == topicName {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("created topic %q not found in REST list response. Got: %v", topicName, listResp.Topics)
	}
}

func TestRESTGateway_PublishMessage(t *testing.T) {
	host := emulatorHost()
	baseURL := fmt.Sprintf("http://%s", host)

	// Setup: We use the existing gRPC-based helper to create the topic securely
	grpcClient := newClient(t)
	topic := mustCreateTopic(t, grpcClient, uniqueName("rest-pub-topic"))

	// 1. Publish via REST POST
	publishURL := fmt.Sprintf("%s/v1/%s:publish", baseURL, topic.String())

	// Format matches the Google Pub/Sub JSON structure
	payload := []byte(`{
		"messages": [
			{"data": "aGVsbG8="}
		]
	}`) // "hello" in base64

	resp, err := http.Post(publishURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		t.Fatalf("POST request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK for publish, got %d: %s", resp.StatusCode, body)
	}

	var pubResp struct {
		MessageIds []string `json:"messageIds"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pubResp); err != nil {
		t.Fatalf("failed to decode publish response: %v", err)
	}

	if len(pubResp.MessageIds) == 0 {
		t.Error("expected at least one message ID, got none")
	}
}

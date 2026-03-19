//go:build integration

package integration_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestHealthEndpoints(t *testing.T) {
	host := emulatorHost()
	baseURL := fmt.Sprintf("http://%s", host)

	endpoints := []string{"/healthz", "/readyz"}

	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			url := baseURL + ep
			resp, err := http.Get(url)
			if err != nil {
				t.Fatalf("GET %s failed: %v", ep, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("expected status 200 OK, got %d", resp.StatusCode)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}

			if string(body) != "OK" {
				t.Errorf("expected body 'OK', got %q", string(body))
			}
		})
	}
}

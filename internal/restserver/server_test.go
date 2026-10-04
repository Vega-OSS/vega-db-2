package restserver_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Vega-OSS/vega-db/internal/metrics"
	"github.com/Vega-OSS/vega-db/internal/restserver"
	"github.com/Vega-OSS/vega-db/internal/service"
)

func setupTestServer(t *testing.T) (*restserver.Server, *httptest.Server) {
	t.Helper()
	m := metrics.New()
	store := service.NewMemStore()
	svc, err := service.New(store, m, service.DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	rs := restserver.New(svc, restserver.Options{
		Addr:         ":0",
		MaxValueSize: 1024 * 1024,
	})
	ts := httptest.NewServer(rs.HTTPServer().Handler)
	return rs, ts
}

func TestRESTBasicEndpoints(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	client := ts.Client()

	// 1. PUT
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/alice", strings.NewReader("engineer"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 2. GET
	resp, err = client.Get(ts.URL + "/v1/kv/alice")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	if string(body) != "engineer" {
		t.Fatalf("expected body 'engineer', got '%s'", string(body))
	}

	// 3. GET missing key
	resp, err = client.Get(ts.URL + "/v1/kv/bob")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	// 4. DELETE
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/v1/kv/alice", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	// 5. GET after delete
	resp, err = client.Get(ts.URL + "/v1/kv/alice")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestRESTBinaryKey(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	client := ts.Client()
	binaryKey := []byte{0x00, 0x01, 0xff}
	b64Key := base64.RawURLEncoding.EncodeToString(binaryKey)

	// PUT binary key
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/"+b64Key, strings.NewReader("binary-value"))
	req.Header.Set("X-Key-Encoding", "base64url")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// GET binary key
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/v1/kv/"+b64Key, nil)
	req.Header.Set("X-Key-Encoding", "base64url")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "binary-value" {
		t.Fatalf("expected binary-value, got %s", string(body))
	}
}

func TestRESTScan(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	client := ts.Client()

	// Insert items
	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/"+k, strings.NewReader("v_"+k))
		resp, _ := client.Do(req)
		_ = resp.Body.Close()
	}

	// Scan
	resp, err := client.Get(ts.URL + "/v1/scan?start=k1&end=k3")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	decoder := json.NewDecoder(resp.Body)
	var count int
	for decoder.More() {
		var item struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := decoder.Decode(&item); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 entries (k1, k2), got %d", count)
	}
}

func TestRESTHealthAndStats(t *testing.T) {
	rs, ts := setupTestServer(t)
	defer ts.Close()

	client := ts.Client()

	// healthz
	resp, err := client.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz failed: %v", err)
	}
	_ = resp.Body.Close()

	// readyz serving
	resp, err = client.Get(ts.URL + "/readyz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz serving failed: %v", err)
	}
	_ = resp.Body.Close()

	// readyz not serving
	rs.SetReady(false)
	resp, err = client.Get(ts.URL + "/readyz")
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz not serving expected 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// stats
	resp, err = client.Get(ts.URL + "/v1/stats")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("stats failed: %v", err)
	}
	var stats map[string]int64
	_ = json.NewDecoder(resp.Body).Decode(&stats)
	_ = resp.Body.Close()
	if stats == nil {
		t.Fatal("empty stats response")
	}
}

func TestRESTPayloadLimits(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	client := ts.Client()

	// Missing content-length (chunked transfer)
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/chunked", strings.NewReader("hello"))
	req.ContentLength = -1
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusLengthRequired {
		t.Fatalf("expected 411 Length Required, got %d", resp.StatusCode)
	}

	// Payload too large
	huge := bytes.Repeat([]byte("a"), 2*1024*1024)
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/huge", bytes.NewReader(huge))
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 Payload Too Large, got %d", resp.StatusCode)
	}
}

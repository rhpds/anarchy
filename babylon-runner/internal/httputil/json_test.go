package httputil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoJSON_PostAndDecode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing Content-Type header")
		}
		if r.Header.Get("X-Custom") != "test" {
			t.Error("missing custom header")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"result": "ok"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	var result map[string]interface{}
	status, err := DoJSON(context.Background(), client, http.MethodPost, server.URL,
		map[string]string{"X-Custom": "test"},
		map[string]string{"key": "val"}, &result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if result["result"] != "ok" {
		t.Errorf("result = %v", result)
	}
}

func TestDoJSON_NilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "healthy"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	var result map[string]interface{}
	status, err := DoJSON(context.Background(), client, http.MethodGet, server.URL, nil, nil, &result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
}

func TestDoJSON_NilResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &http.Client{}
	status, err := DoJSON(context.Background(), client, http.MethodDelete, server.URL, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 204 {
		t.Errorf("status = %d, want 204", status)
	}
}

func TestDoJSON_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "boom"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	status, err := DoJSON(context.Background(), client, http.MethodGet, server.URL, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 500 {
		t.Errorf("status = %d, want 500", status)
	}
}

package httputil

import (
	"crypto/tls"
	"testing"
)

func TestNewTransport_NilTLS(t *testing.T) {
	tr := NewTransport(nil)
	if tr.TLSClientConfig != nil {
		t.Error("expected nil TLS config for default transport")
	}
	if tr.MaxIdleConns != 100 {
		t.Errorf("MaxIdleConns = %d, want 100", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 10 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 10", tr.MaxIdleConnsPerHost)
	}
}

func TestNewTransport_CustomTLS(t *testing.T) {
	cfg := &tls.Config{InsecureSkipVerify: true}
	tr := NewTransport(cfg)
	if tr.TLSClientConfig != cfg {
		t.Error("TLS config not set")
	}
}

func TestNewTLSConfig_VerifyTrue(t *testing.T) {
	cfg, err := NewTLSConfig(true, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil config when verify=true and no CA")
	}
}

func TestNewTLSConfig_VerifyFalse(t *testing.T) {
	cfg, err := NewTLSConfig(false, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}
}

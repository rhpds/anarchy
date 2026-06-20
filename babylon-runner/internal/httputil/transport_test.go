package httputil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"
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

func TestNewTLSConfig_WithCAPath(t *testing.T) {
	// Generate a self-signed CA certificate.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tmpFile, err := os.CreateTemp("", "ca-*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.Write(certPEM); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	tmpFile.Close()

	cfg, err := NewTLSConfig(true, tmpFile.Name())
	if err != nil {
		t.Fatalf("NewTLSConfig failed: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config when CA provided")
	}
	if cfg.RootCAs == nil {
		t.Error("expected RootCAs to be set")
	}
}

func TestNewTLSConfig_InvalidCAPath(t *testing.T) {
	_, err := NewTLSConfig(true, "/nonexistent/ca.pem")
	if err == nil {
		t.Error("expected error for nonexistent CA file")
	}
}

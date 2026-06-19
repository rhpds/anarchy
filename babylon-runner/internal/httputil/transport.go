package httputil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"
)

// NewTransport creates a shared http.Transport with connection pooling.
// tlsConfig is optional — nil uses Go defaults (system CA, verify enabled).
func NewTransport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}
}

// NewTLSConfig builds a tls.Config from the given parameters.
// Returns nil when verify=true and no custom CA (use Go system defaults).
// Returns a config with InsecureSkipVerify=true when verify=false.
// Loads a custom CA bundle from caPath when provided.
func NewTLSConfig(verify bool, caPath string) (*tls.Config, error) {
	if !verify {
		return &tls.Config{InsecureSkipVerify: true}, nil
	}
	if caPath == "" {
		return nil, nil
	}
	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA cert from %s", caPath)
	}
	return &tls.Config{RootCAs: pool}, nil
}

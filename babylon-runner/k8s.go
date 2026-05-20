package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// k8sSecretData reads data fields from a K8s Secret selected by label
// in the given namespace. Works both in-cluster (ServiceAccount token)
// and locally (kubeconfig via oc/kubectl).
// Returns the raw data map (values are base64-encoded).
func k8sSecretData(namespace, labelSelector string) (map[string]string, error) {
	apiServer, token, err := k8sAuth()
	if err != nil {
		return nil, err
	}

	apiURL := fmt.Sprintf("%s/api/v1/namespaces/%s/secrets?labelSelector=%s",
		apiServer, namespace, url.QueryEscape(labelSelector))

	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("k8s API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("k8s API returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Items []struct {
			Data map[string]string `json:"data"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode k8s response: %w", err)
	}
	if len(result.Items) == 0 {
		return nil, fmt.Errorf("no secret found with label %q in namespace %s", labelSelector, namespace)
	}

	return result.Items[0].Data, nil
}

// k8sAuth returns (apiServer, token) for K8s API access.
// Prefers in-cluster auth, falls back to local oc/kubectl.
func k8sAuth() (string, string, error) {
	// In-cluster: use service account token.
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host != "" && port != "" {
		tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
		if err != nil {
			return "", "", fmt.Errorf("read serviceaccount token: %w", err)
		}
		return fmt.Sprintf("https://%s:%s", host, port), string(tokenBytes), nil
	}

	// Local dev: get API server and token from oc/kubectl.
	server, err := exec.Command("oc", "whoami", "--show-server").Output()
	if err != nil {
		return "", "", fmt.Errorf("not in-cluster and oc whoami --show-server failed: %w", err)
	}
	token, err := exec.Command("oc", "whoami", "-t").Output()
	if err != nil {
		return "", "", fmt.Errorf("oc whoami -t failed: %w", err)
	}

	return strings.TrimSpace(string(server)), strings.TrimSpace(string(token)), nil
}

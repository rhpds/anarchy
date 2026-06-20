package secrets

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCacheGetByLabel(t *testing.T) {
	clientset := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tower-creds-host1",
			Namespace: "anarchy",
			Labels: map[string]string{
				"babylon.gpte.redhat.com/ansible-control-plane": "host1.example.com",
			},
		},
		Data: map[string][]byte{
			"user":     []byte("admin"),
			"password": []byte("secret"),
		},
	})

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	secret, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host1.example.com")
	if !ok {
		t.Fatal("expected to find secret")
	}
	if string(secret.Data["user"]) != "admin" {
		t.Errorf("user = %q, want %q", string(secret.Data["user"]), "admin")
	}
	if string(secret.Data["password"]) != "secret" {
		t.Errorf("password = %q, want %q", string(secret.Data["password"]), "secret")
	}
}

func TestCacheGetByLabelNotFound(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	_, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "nonexistent")
	if ok {
		t.Error("expected not to find secret")
	}
}

func TestCacheMultipleSecrets(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tower-creds-host1",
				Namespace: "anarchy",
				Labels: map[string]string{
					"babylon.gpte.redhat.com/ansible-control-plane": "host1.example.com",
				},
			},
			Data: map[string][]byte{"user": []byte("admin1")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tower-creds-host2",
				Namespace: "anarchy",
				Labels: map[string]string{
					"babylon.gpte.redhat.com/ansible-control-plane": "host2.example.com",
				},
			},
			Data: map[string][]byte{"user": []byte("admin2")},
		},
	)

	c := NewCache(clientset, "anarchy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	s1, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host1.example.com")
	if !ok {
		t.Fatal("expected to find host1 secret")
	}
	if string(s1.Data["user"]) != "admin1" {
		t.Errorf("host1 user = %q, want %q", string(s1.Data["user"]), "admin1")
	}

	s2, ok := c.GetByLabel("babylon.gpte.redhat.com/ansible-control-plane", "host2.example.com")
	if !ok {
		t.Fatal("expected to find host2 secret")
	}
	if string(s2.Data["user"]) != "admin2" {
		t.Errorf("host2 user = %q, want %q", string(s2.Data["user"]), "admin2")
	}
}

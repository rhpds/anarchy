package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type Cache struct {
	informer cache.SharedIndexInformer
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewCache(clientset kubernetes.Interface, namespace string) *Cache {
	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset, 0,
		informers.WithNamespace(namespace),
	)
	informer := factory.Core().V1().Secrets().Informer()
	return &Cache{
		informer: informer,
		stopCh:   make(chan struct{}),
	}
}

func (c *Cache) Start(ctx context.Context) error {
	c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if s, ok := obj.(*corev1.Secret); ok {
				slog.Info("secret cache: added", "name", s.Name, "namespace", s.Namespace)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if s, ok := newObj.(*corev1.Secret); ok {
				slog.Info("secret cache: updated", "name", s.Name, "namespace", s.Namespace)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if s, ok := obj.(*corev1.Secret); ok {
				slog.Info("secret cache: deleted", "name", s.Name, "namespace", s.Namespace)
			}
		},
	})

	go func() {
		c.informer.Run(c.stopCh)
		select {
		case <-c.stopCh:
		default:
			slog.Error("secret cache informer exited unexpectedly")
		}
	}()

	deadline, hasDeadline := ctx.Deadline()
	timeout := 30 * time.Second
	if hasDeadline {
		timeout = time.Until(deadline)
	}

	if !cache.WaitForCacheSync(
		func() <-chan struct{} {
			ch := make(chan struct{})
			go func() {
				select {
				case <-time.After(timeout):
					close(ch)
				case <-ctx.Done():
					close(ch)
				}
			}()
			return ch
		}(),
		c.informer.HasSynced,
	) {
		return fmt.Errorf("secret informer cache sync timed out")
	}

	slog.Info("secret cache: synced", "secrets", len(c.informer.GetStore().List()))
	return nil
}

func (c *Cache) GetByLabel(labelKey, labelValue string) (*corev1.Secret, bool) {
	for _, obj := range c.informer.GetStore().List() {
		secret, ok := obj.(*corev1.Secret)
		if !ok {
			continue
		}
		if secret.Labels[labelKey] == labelValue {
			return secret, true
		}
	}
	return nil, false
}

func (c *Cache) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

// Package remote gives the control plane clients of the Kubernetes
// clusters data centers run in, from ClientConfigs (k8ssandra-operator's
// ClientConfig): a kubeconfig in a secret.
package remote

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
)

// Clients returns clients of Kubernetes clusters by ClientConfig name.
type Clients struct {
	// Local is the control plane's own cluster.
	Local  client.Client
	Scheme *runtime.Scheme
	// Namespace holds the ClientConfigs and their secrets.
	Namespace string

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	version string // the secret's resourceVersion
	c       client.Client
}

// Get returns the client of the cluster a ClientConfig names ("" is the
// local one).
func (r *Clients) Get(ctx context.Context, name string) (client.Client, error) {
	if name == "" {
		return r.Local, nil
	}
	cc := &v1.ClientConfig{}
	if err := r.Local.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: name}, cc); err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	s := &corev1.Secret{}
	if err := r.Local.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: cc.Spec.KubeConfigSecret.SecretName}, s); err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.cache[name]; ok && c.version == s.ResourceVersion {
		return c.c, nil
	}
	cfg, err := clientcmd.Load(s.Data["kubeconfig"])
	if err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cc.Spec.ContextName}
	rest, err := clientcmd.NewDefaultClientConfig(*cfg, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	c, err := client.New(rest, client.Options{Scheme: r.Scheme})
	if err != nil {
		return nil, err
	}
	if r.cache == nil {
		r.cache = map[string]cached{}
	}
	r.cache[name] = cached{version: s.ResourceVersion, c: c}
	return c, nil
}

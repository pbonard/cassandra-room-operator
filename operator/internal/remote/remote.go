// Package remote gives the control plane clients of the Kubernetes
// clusters data centers run in, from ClientConfigs (k8ssandra-operator's
// ClientConfig): a kubeconfig in a secret.
package remote

import (
	"context"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
)

// Clients returns clients of Kubernetes clusters by ClientConfig name.
type Clients struct {
	// Local is the control plane's own cluster.
	Local  client.Client
	Scheme *runtime.Scheme
	// Namespace holds the ClientConfigs and their secrets.
	Namespace string

	mu      sync.Mutex
	cache   map[string]cached
	watches map[string]*watch
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
	cc, s, err := r.load(ctx, name)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.cache[name]; ok && c.version == s.ResourceVersion {
		return c.c, nil
	}
	rest, err := restConfig(name, cc, s)
	if err != nil {
		return nil, err
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

func (r *Clients) load(ctx context.Context, name string) (*v1.ClientConfig, *corev1.Secret, error) {
	cc := &v1.ClientConfig{}
	if err := r.Local.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: name}, cc); err != nil {
		return nil, nil, fmt.Errorf("client config %q: %w", name, err)
	}
	s := &corev1.Secret{}
	if err := r.Local.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: cc.Spec.KubeConfigSecret.SecretName}, s); err != nil {
		return nil, nil, fmt.Errorf("client config %q: %w", name, err)
	}
	return cc, s, nil
}

func restConfig(name string, cc *v1.ClientConfig, s *corev1.Secret) (*rest.Config, error) {
	cfg, err := clientcmd.Load(s.Data["kubeconfig"])
	if err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cc.Spec.ContextName}
	rc, err := clientcmd.NewDefaultClientConfig(*cfg, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("client config %q: %w", name, err)
	}
	return rc, nil
}

// watch is a remote cluster's cache, running while its kubeconfig is the
// same, and the registrations of the controllers watching it.
type watch struct {
	version  string
	stop     context.CancelFunc
	cache    cache.Cache
	register map[string]func(cache.Cache) error
}

// Watch makes sure a remote cluster's cache runs and that register (under
// id: once per controller) has set up its watches on it, again whenever
// the cache restarts after the kubeconfig changed. The caches stop with
// Stop.
func (r *Clients) Watch(ctx context.Context, name, id string, register func(cache.Cache) error) error {
	if name == "" {
		return nil // the local cluster is watched by the manager
	}
	cc, s, err := r.load(ctx, name)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.watches[name]
	if ok && w.version == s.ResourceVersion {
		if _, done := w.register[id]; done {
			return nil
		}
		if err := register(w.cache); err != nil {
			return err
		}
		w.register[id] = register
		return nil
	}
	rc, err := restConfig(name, cc, s)
	if err != nil {
		return err
	}
	cl, err := cluster.New(rc, func(o *cluster.Options) { o.Scheme = r.Scheme })
	if err != nil {
		return err
	}
	regs := map[string]func(cache.Cache) error{id: register}
	if ok {
		for k, f := range w.register {
			regs[k] = f
		}
	}
	for _, f := range regs {
		if err := f(cl.GetCache()); err != nil {
			return err
		}
	}
	if ok {
		w.stop()
	}
	runCtx, stop := context.WithCancel(context.Background())
	go func() { _ = cl.Start(runCtx) }()
	if r.watches == nil {
		r.watches = map[string]*watch{}
	}
	r.watches[name] = &watch{version: s.ResourceVersion, stop: stop, cache: cl.GetCache(), register: regs}
	return nil
}

// Stop stops the remote caches.
func (r *Clients) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, w := range r.watches {
		w.stop()
		delete(r.watches, name)
	}
}

// AnnotationOwner names, on an object the control plane creates in a data
// center's cluster, the object ("namespace/name") it belongs to: owner
// references cannot cross clusters.
const AnnotationOwner = "cassandraroom.pbonard.github.io/owner"

// Owner is the request of the object an annotated object belongs to.
func Owner(obj client.Object) []reconcile.Request {
	ns, name, ok := strings.Cut(obj.GetAnnotations()[AnnotationOwner], "/")
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}}
}

// SetOwner annotates obj as belonging to owner; it reports whether it
// changed.
func SetOwner(obj, owner client.Object) bool {
	want := owner.GetNamespace() + "/" + owner.GetName()
	a := obj.GetAnnotations()
	if a[AnnotationOwner] == want {
		return false
	}
	if a == nil {
		a = map[string]string{}
	}
	a[AnnotationOwner] = want
	obj.SetAnnotations(a)
	return true
}

// Stopper stops the remote caches with the manager.
func (r *Clients) Stopper() manager.Runnable {
	return manager.RunnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		r.Stop()
		return nil
	})
}

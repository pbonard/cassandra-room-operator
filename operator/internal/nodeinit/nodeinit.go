// Package nodeinit prepares a node's start, in its pod's init container: it
// writes the flags that depend on the pod (its addresses, the seeds, the
// node it replaces) to the server's flags file.
package nodeinit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

// Options are node-init's inputs.
type Options struct {
	Namespace, Pod, ConfigMap string
	PodIP                     string
	Networking                v1.NetworkingMode
	PreferLocal               bool
	DataDir, Out              string
	// Poll is how often to look again while waiting (default 5s).
	Poll time.Duration
}

// Run waits for what the node needs to start and writes its flags.
func Run(ctx context.Context, kube kubernetes.Interface, o Options) error {
	if o.Poll == 0 {
		o.Poll = 5 * time.Second
	}
	if o.PodIP == "" {
		return errors.New("node-init: no pod IP")
	}
	for {
		flags, waiting, err := flags(ctx, kube, o)
		if err != nil {
			return err
		}
		if waiting == "" {
			return writeFile(o.Out, strings.Join(flags, "\n")+"\n")
		}
		fmt.Fprintf(os.Stderr, "node-init: waiting for %s\n", waiting)
		select {
		case <-ctx.Done():
			return fmt.Errorf("node-init: gave up waiting for %s: %w", waiting, ctx.Err())
		case <-time.After(o.Poll):
		}
	}
}

// flags computes the flags, or what they wait for.
func flags(ctx context.Context, kube kubernetes.Interface, o Options) ([]string, string, error) {
	cm, err := kube.CoreV1().ConfigMaps(o.Namespace).Get(ctx, o.ConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, "the node configuration (" + err.Error() + ")", nil
	}
	internode := net.JoinHostPort(o.PodIP, strconv.Itoa(names.InternodePort))
	out := []string{
		"-listen-address=" + internode,
		"-addr=" + net.JoinHostPort(o.PodIP, strconv.Itoa(names.NativePort)),
	}
	self := internode
	if o.Networking == v1.NetworkingLoadBalancer {
		svc, err := kube.CoreV1().Services(o.Namespace).Get(ctx, names.LoadBalancer(o.Pod), metav1.GetOptions{})
		if err != nil {
			return nil, "the node's load balancer (" + err.Error() + ")", nil
		}
		var ip string
		for _, in := range svc.Status.LoadBalancer.Ingress {
			switch {
			case in.IP != "":
				ip = in.IP
			case in.Hostname != "":
				if ips, err := net.LookupHost(in.Hostname); err == nil && len(ips) > 0 {
					ip = ips[0]
				}
			}
			if ip != "" {
				break
			}
		}
		if ip == "" {
			return nil, "the node's load balancer address", nil
		}
		self = net.JoinHostPort(ip, strconv.Itoa(names.InternodePort))
		out = append(out, "-broadcast-address="+self, "-broadcast-rpc-address="+ip)
		if o.PreferLocal {
			out = append(out, "-prefer-local")
		}
	}
	var seeds []string
	for _, s := range strings.Split(cm.Data["seeds"], ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}
	if len(seeds) == 0 {
		if cm.Data["bootstrap-seed"] != o.Pod {
			return nil, "a seed", nil
		}
		seeds = []string{self} // the cluster's first node
	}
	out = append(out, "-seeds="+strings.Join(seeds, ","))
	if host := cm.Data["replace."+o.Pod]; host != "" {
		empty, err := emptyDir(o.DataDir)
		if err != nil {
			return nil, "", err
		}
		if empty {
			out = append(out, "-replace-address="+host)
		}
	}
	return out, "", nil
}

// emptyDir reports whether a data directory holds no database yet.
func emptyDir(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.Name() != "lost+found" {
			return false, nil
		}
	}
	return true, nil
}

func writeFile(path, data string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(data), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Command cassandra-room-operator is the operator's single binary:
//
//	cassandra-room-operator manager    the controllers (control and data plane)
//	cassandra-room-operator node-init  a node's init container
//	cassandra-room-operator agent      a node's backup agent
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/agent"
	"github.com/pbonard/cassandra-room-operator/operator/internal/backup"
	"github.com/pbonard/cassandra-room-operator/operator/internal/cluster"
	"github.com/pbonard/cassandra-room-operator/operator/internal/dc"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/nodeinit"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
	"github.com/pbonard/cassandra-room-operator/operator/internal/remote"
	"github.com/pbonard/cassandra-room-operator/operator/internal/repair"
)

func main() {
	cmd := "manager"
	args := os.Args[1:]
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "manager":
		err = runManager(args)
	case "node-init":
		err = runNodeInit(args)
	case "agent":
		err = runAgent(args)
	default:
		err = fmt.Errorf("unknown command %q (manager, node-init or agent)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runManager(args []string) error {
	fs := flag.NewFlagSet("manager", flag.ExitOnError)
	controlPlane := fs.Bool("control-plane", true, "Run the control plane: CassandraRoomClusters, cluster backups, schedules and restores")
	dataPlane := fs.Bool("data-plane", true, "Run the data plane: CassandraRoomDatacenters, repairs, data center backups and restores")
	agentImage := fs.String("agent-image", os.Getenv("OPERATOR_IMAGE"), "Image of node-init and the backup agent (default: $OPERATOR_IMAGE)")
	watchNamespace := fs.String("namespace", "", "Namespace to watch (default: all)")
	clientConfigNs := fs.String("client-config-namespace", os.Getenv("POD_NAMESPACE"), "Namespace of the ClientConfigs")
	metricsAddr := fs.String("metrics-bind-address", ":8080", "Metrics address (0 disables)")
	probeAddr := fs.String("health-probe-bind-address", ":8081", "Health probes address")
	leaderElect := fs.Bool("leader-elect", false, "Elect a leader among replicas")
	opts := zap.Options{}
	opts.BindFlags(fs)
	_ = fs.Parse(args)
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := v1.AddToScheme(scheme); err != nil {
		return err
	}
	mopts := ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr, LeaderElection: *leaderElect, LeaderElectionID: "cassandra-room-operator.pbonard.github.io"}
	if *watchNamespace != "" {
		mopts.Cache = cache.Options{DefaultNamespaces: map[string]cache.Config{*watchNamespace: {}}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mopts)
	if err != nil {
		return err
	}
	c := mgr.GetClient()
	mgmtFor := dc.MgmtClients(c)
	roles := dc.CQLRoles{}
	type setup interface{ SetupWithManager(ctrl.Manager) error }
	var reconcilers []setup
	if *dataPlane {
		common := backup.Common{Client: c, Scheme: scheme, Recorder: mgr.GetEventRecorderFor("backup"),
			Agents: &agent.HTTPClient{}, Store: storeFor(c), Roles: roles}
		reconcilers = append(reconcilers,
			&dc.Reconciler{Client: c, Scheme: scheme, Recorder: mgr.GetEventRecorderFor("datacenter"), Mgmt: mgmtFor, Roles: roles, AgentImage: *agentImage},
			&repair.Reconciler{Client: c, Scheme: scheme, Recorder: mgr.GetEventRecorderFor("repair"), Mgmt: mgmtFor},
			&backup.DatacenterBackupReconciler{Common: common},
			&backup.DatacenterRestoreReconciler{Common: common},
		)
	}
	if *controlPlane {
		rem := &remote.Clients{Local: c, Scheme: scheme, Namespace: *clientConfigNs}
		cp := backup.ControlPlane{Client: c, Scheme: scheme, Recorder: mgr.GetEventRecorderFor("backup"), Remote: rem}
		reconcilers = append(reconcilers,
			&cluster.Reconciler{Client: c, Scheme: scheme, Recorder: mgr.GetEventRecorderFor("cluster"), Remote: rem},
			&backup.BackupReconciler{ControlPlane: cp}, &backup.ScheduleReconciler{ControlPlane: cp}, &backup.RestoreReconciler{ControlPlane: cp},
		)
	}
	for _, r := range reconcilers {
		if err := r.SetupWithManager(mgr); err != nil {
			return err
		}
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}

// storeFor opens a data center's object store, with the credentials of its
// secret.
func storeFor(c client.Client) backup.StoreFunc {
	return func(ctx context.Context, d *v1.CassandraRoomDatacenter) (objstore.Store, error) {
		b := d.Spec.Backup
		o := objstore.S3Options{Bucket: b.Bucket, Prefix: b.Prefix, Region: b.Region, Endpoint: b.Endpoint, PathStyle: b.ForcePathStyle}
		if b.CredentialsSecret != nil {
			s := &corev1.Secret{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: d.Namespace, Name: b.CredentialsSecret.SecretName}, s); err != nil {
				return nil, err
			}
			o.AccessKey, o.SecretKey = string(s.Data["access_key_id"]), string(s.Data["secret_access_key"])
		}
		return objstore.NewS3(ctx, o)
	}
}

func runNodeInit(args []string) error {
	fs := flag.NewFlagSet("node-init", flag.ExitOnError)
	configMap := fs.String("config-map", "", "The data center's node configuration")
	out := fs.String("out", "", "Flags file to write")
	dataDir := fs.String("data-dir", "", "The node's data directory")
	networking := fs.String("networking", string(v1.NetworkingPodIP), "PodIP, HostNetwork or LoadBalancer")
	preferLocal := fs.Bool("prefer-local", false, "Connect to nodes of the data center at their pod addresses")
	timeout := fs.Duration("timeout", time.Hour, "How long to wait for seeds and addresses")
	_ = fs.Parse(args)
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return nodeinit.Run(ctx, kube, nodeinit.Options{Namespace: os.Getenv("POD_NAMESPACE"), Pod: os.Getenv("POD_NAME"),
		ConfigMap: *configMap, PodIP: os.Getenv("POD_IP"), Networking: v1.NetworkingMode(*networking), PreferLocal: *preferLocal,
		DataDir: *dataDir, Out: *out})
}

func runAgent(args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	listen := fs.String("listen", ":8090", "Address to serve the agent's API on")
	management := fs.String("management", "127.0.0.1:7199", "The node's management API")
	managementTLS := fs.String("management-tls", "", "Directory of ca.crt, tls.crt and tls.key for the management API")
	dataDir := fs.String("data-dir", "/var/lib/cassandra-room", "The node's data directory")
	o := objstore.S3Options{}
	fs.StringVar(&o.Bucket, "bucket", "", "Bucket")
	fs.StringVar(&o.Prefix, "prefix", "", "Prefix of the objects")
	fs.StringVar(&o.Region, "region", "", "Region")
	fs.StringVar(&o.Endpoint, "endpoint", "", "Endpoint of an S3-compatible store")
	fs.BoolVar(&o.PathStyle, "path-style", false, "Address buckets by path")
	concurrency := fs.Int("concurrency", 4, "Files transferred at once")
	cluster := fs.String("cluster", "", "The node's cluster (for its commit log's objects)")
	datacenter := fs.String("datacenter", "", "The node's data center")
	pod := fs.String("pod", os.Getenv("POD_NAME"), "The node's pod (default: $POD_NAME)")
	commitLogEvery := fs.Duration("commitlog-interval", 0, "Upload the node's archived commit log this often (0: never)")
	_ = fs.Parse(args)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := objstore.NewS3(ctx, o)
	if err != nil {
		return err
	}
	cl := mgmt.Client(mgmt.New(nil))
	if *managementTLS != "" {
		read := func(name string) []byte { b, _ := os.ReadFile(filepath.Join(*managementTLS, name)); return b }
		cfg, err := dc.TLSFromPEM(read("ca.crt"), read("tls.crt"), read("tls.key"))
		if err != nil {
			return err
		}
		cl = mgmt.New(cfg)
	}
	a := &agent.Agent{Store: store, Mgmt: cl, MgmtAddr: *management, DataDir: *dataDir, Concurrency: *concurrency,
		Cluster: *cluster, Datacenter: *datacenter, Pod: *pod}
	if *commitLogEvery > 0 {
		if *cluster == "" || *datacenter == "" || *pod == "" {
			return fmt.Errorf("--commitlog-interval needs --cluster, --datacenter and --pod")
		}
		go a.ArchiveCommitLogEvery(ctx, *commitLogEvery)
	}
	srv := &http.Server{Addr: *listen, Handler: a.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

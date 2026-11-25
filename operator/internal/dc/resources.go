package dc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/names"
)

// The desired objects of a data center.

func objMeta(dc *v1.CassandraRoomDatacenter, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: dc.Namespace, Labels: names.Labels(dc)}
}

// nodeRBAC lets node-init read its ConfigMap and its LoadBalancer Service.
func nodeRBAC(dc *v1.CassandraRoomDatacenter) (*corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding) {
	n := names.ServiceAccount(dc)
	sa := &corev1.ServiceAccount{ObjectMeta: objMeta(dc, n)}
	role := &rbacv1.Role{ObjectMeta: objMeta(dc, n), Rules: []rbacv1.PolicyRule{
		{APIGroups: []string{""}, Resources: []string{"configmaps", "services"}, Verbs: []string{"get", "list", "watch"}},
	}}
	rb := &rbacv1.RoleBinding{ObjectMeta: objMeta(dc, n),
		RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: n},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: n, Namespace: dc.Namespace}}}
	return sa, role, rb
}

func servicePorts(dc *v1.CassandraRoomDatacenter) []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: "native", Port: names.NativePort, TargetPort: intstr.FromInt32(names.NativePort)},
		{Name: "internode", Port: names.InternodePort, TargetPort: intstr.FromInt32(names.InternodePort)},
		{Name: "management", Port: names.MgmtPort(dc), TargetPort: intstr.FromInt32(names.MgmtPort(dc))},
	}
}

// headlessService resolves every pod, ready or not.
func headlessService(dc *v1.CassandraRoomDatacenter) *corev1.Service {
	return &corev1.Service{ObjectMeta: objMeta(dc, names.HeadlessService(dc)), Spec: corev1.ServiceSpec{
		ClusterIP: corev1.ClusterIPNone, PublishNotReadyAddresses: true, Selector: names.Selector(dc), Ports: servicePorts(dc)}}
}

// clientService balances clients' first connections over ready nodes.
func clientService(dc *v1.CassandraRoomDatacenter) *corev1.Service {
	return &corev1.Service{ObjectMeta: objMeta(dc, names.ClientService(dc)), Spec: corev1.ServiceSpec{
		Selector: names.Selector(dc),
		Ports:    []corev1.ServicePort{{Name: "native", Port: names.NativePort, TargetPort: intstr.FromInt32(names.NativePort)}}}}
}

// loadBalancerService is a pod's own address (LoadBalancer networking).
func loadBalancerService(dc *v1.CassandraRoomDatacenter, pod string) *corev1.Service {
	m := objMeta(dc, names.LoadBalancer(pod))
	m.Annotations = dc.Spec.Networking.LoadBalancerAnnotations
	sel := names.Selector(dc)
	sel["statefulset.kubernetes.io/pod-name"] = pod
	return &corev1.Service{ObjectMeta: m, Spec: corev1.ServiceSpec{
		Type: corev1.ServiceTypeLoadBalancer, Selector: sel, PublishNotReadyAddresses: true,
		Ports: []corev1.ServicePort{
			{Name: "native", Port: names.NativePort, TargetPort: intstr.FromInt32(names.NativePort)},
			{Name: "internode", Port: names.InternodePort, TargetPort: intstr.FromInt32(names.InternodePort)},
		}}}
}

// podDisruptionBudget allows one node down at a time.
func podDisruptionBudget(dc *v1.CassandraRoomDatacenter) *policyv1.PodDisruptionBudget {
	one := intstr.FromInt32(1)
	return &policyv1.PodDisruptionBudget{ObjectMeta: objMeta(dc, names.DCResource(dc, "pdb")), Spec: policyv1.PodDisruptionBudgetSpec{
		MaxUnavailable: &one, Selector: &metav1.LabelSelector{MatchLabels: names.Selector(dc)}}}
}

// serverArgs are the flags of the server that do not depend on the pod;
// node-init writes the others (addresses, seeds) to the flags file.
func serverArgs(dc *v1.CassandraRoomDatacenter, rack string) []string {
	c := dc.Spec.Config
	args := []string{
		"-flags-file=" + names.FlagsFile,
		"-cluster-name=" + dc.Spec.ClusterName,
		"-dc=" + dc.DCName(),
		"-rack=" + rack,
		"-authenticator=PasswordAuthenticator",
		fmt.Sprintf("-admin-address=0.0.0.0:%d", names.MgmtPort(dc)),
		"-data-dir=" + names.DataDir,
	}
	if b := dc.Spec.Backup; b != nil && b.CommitLogArchive != nil {
		// On the data volume (hard links), uploaded by the agent.
		args = append(args, "-commitlog-archive-dir="+names.CommitLogArchive)
	}
	authorizer := c.Authorizer
	if authorizer == "" {
		authorizer = "CassandraAuthorizer"
	}
	args = append(args, "-authorizer="+authorizer)
	if c.NumTokens != nil {
		args = append(args, fmt.Sprintf("-num-tokens=%d", *c.NumTokens))
	}
	if c.CommitlogSync != "" {
		args = append(args, "-commitlog-sync="+c.CommitlogSync)
	}
	if c.CommitlogSyncPeriod != "" {
		args = append(args, "-commitlog-sync-period="+c.CommitlogSyncPeriod)
	}
	if c.MemtableSpace != nil {
		args = append(args, "-memtable-space="+strconv.FormatInt(c.MemtableSpace.Value(), 10))
	}
	t := dc.Spec.TLS
	if t.Internode != nil {
		d := names.TLSDir + "/internode/"
		args = append(args, "-internode-cert="+d+"tls.crt", "-internode-key="+d+"tls.key", "-internode-ca="+d+"ca.crt")
		if t.Internode.VerifyHostname {
			args = append(args, "-internode-verify-hostname")
		}
	}
	if t.Client != nil {
		d := names.TLSDir + "/client/"
		args = append(args, "-client-cert="+d+"tls.crt", "-client-key="+d+"tls.key")
		if t.Client.RequireClientAuth {
			args = append(args, "-client-ca="+d+"ca.crt")
		}
		if t.Client.Optional {
			args = append(args, "-client-tls-optional")
		}
	}
	if t.Management != nil {
		d := names.TLSDir + "/management/"
		args = append(args, "-admin-cert="+d+"tls.crt", "-admin-key="+d+"tls.key")
	}
	return append(args, c.ExtraArgs...)
}

func tlsVolumes(dc *v1.CassandraRoomDatacenter) ([]corev1.Volume, []corev1.VolumeMount) {
	var vols []corev1.Volume
	var mounts []corev1.VolumeMount
	add := func(name, secret string) {
		vols = append(vols, corev1.Volume{Name: "tls-" + name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secret}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "tls-" + name, MountPath: names.TLSDir + "/" + name, ReadOnly: true})
	}
	t := dc.Spec.TLS
	if t.Internode != nil {
		add("internode", t.Internode.SecretName)
	}
	if t.Client != nil {
		add("client", t.Client.SecretName)
	}
	if t.Management != nil {
		add("management", t.Management.SecretName)
	}
	return vols, mounts
}

func scheme(dc *v1.CassandraRoomDatacenter) corev1.URIScheme {
	if dc.Spec.TLS.Management != nil {
		return corev1.URISchemeHTTPS
	}
	return corev1.URISchemeHTTP
}

func env(name, field string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: field}}}
}

// statefulSet is a rack's nodes.
func statefulSet(dc *v1.CassandraRoomDatacenter, rack v1.Rack, replicas int32, agentImage string) *appsv1.StatefulSet {
	labels := names.RackLabels(dc, rack.Name)
	tlsVols, tlsMounts := tlsVolumes(dc)
	dataMount := corev1.VolumeMount{Name: names.DataVolume, MountPath: names.DataDir}
	configMount := corev1.VolumeMount{Name: names.ConfigVol, MountPath: names.ConfigDir}
	pullPolicy := corev1.PullPolicy(dc.Spec.ImagePullPolicy)
	mode := dc.Spec.Networking.Mode
	if mode == "" {
		mode = v1.NetworkingPodIP
	}
	initArgs := []string{"node-init",
		"--config-map=" + names.NodeConfig(dc), "--out=" + names.FlagsFile, "--data-dir=" + names.DataDir,
		"--networking=" + string(mode),
	}
	if dc.Spec.Networking.PreferLocal {
		initArgs = append(initArgs, "--prefer-local")
	}
	probe := func(path string, period, failures int32) *corev1.Probe {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: path, Port: intstr.FromInt32(names.MgmtPort(dc)), Scheme: scheme(dc)}},
			PeriodSeconds: period, FailureThreshold: failures, TimeoutSeconds: 5}
	}
	server := corev1.Container{
		Name: "cassandra-room", Image: dc.Spec.Image, ImagePullPolicy: pullPolicy,
		Args: serverArgs(dc, rack.Name),
		Ports: []corev1.ContainerPort{
			{Name: "native", ContainerPort: names.NativePort},
			{Name: "internode", ContainerPort: names.InternodePort},
			{Name: "management", ContainerPort: names.MgmtPort(dc)},
		},
		Resources:    dc.Spec.Pod.Resources,
		VolumeMounts: append([]corev1.VolumeMount{dataMount, configMount}, tlsMounts...),
		// Bootstrapping streams data: the startup probe waits up to 4 hours.
		StartupProbe:   probe("/api/v1/health", 10, 1440),
		LivenessProbe:  probe("/api/v1/health", 15, 4),
		ReadinessProbe: probe("/api/v1/ready", 5, 2),
	}
	containers := []corev1.Container{server}
	if b := dc.Spec.Backup; b != nil {
		containers = append(containers, agentContainer(dc, b, agentImage, dataMount, tlsMounts))
	}
	pod := corev1.PodSpec{
		ServiceAccountName: names.ServiceAccount(dc),
		InitContainers: []corev1.Container{{
			Name: "node-init", Image: agentImage, ImagePullPolicy: pullPolicy, Args: initArgs,
			Env: []corev1.EnvVar{env("POD_NAME", "metadata.name"), env("POD_NAMESPACE", "metadata.namespace"),
				env("POD_IP", "status.podIP"), env("HOST_IP", "status.hostIP")},
			VolumeMounts: []corev1.VolumeMount{dataMount, configMount},
		}},
		Containers:                    containers,
		Volumes:                       append([]corev1.Volume{{Name: names.ConfigVol, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}, tlsVols...),
		TerminationGracePeriodSeconds: ptr(int64(120)),
		NodeSelector:                  dc.Spec.Pod.NodeSelector,
		Tolerations:                   dc.Spec.Pod.Tolerations,
		SecurityContext:               &corev1.PodSecurityContext{FSGroup: ptr(int64(65532)), RunAsUser: ptr(int64(65532)), RunAsNonRoot: ptr(true)},
		Affinity:                      affinity(dc, rack),
	}
	if dc.Spec.Pod.ServiceAccountName != "" {
		pod.ServiceAccountName = dc.Spec.Pod.ServiceAccountName
	}
	for _, s := range dc.Spec.ImagePullSecrets {
		pod.ImagePullSecrets = append(pod.ImagePullSecrets, corev1.LocalObjectReference{Name: s.SecretName})
	}
	if mode == v1.NetworkingHostNetwork {
		pod.HostNetwork, pod.DNSPolicy = true, corev1.DNSClusterFirstWithHostNet
	}
	size := resource.MustParse("10Gi")
	if dc.Spec.Storage.Size != nil {
		size = *dc.Spec.Storage.Size
	}
	// Prometheus scrapes the management API's /metrics (the usual
	// annotations; a ServiceMonitor can select the pods by their labels).
	annotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   strconv.Itoa(int(names.MgmtPort(dc))),
		"prometheus.io/path":   "/metrics",
		"prometheus.io/scheme": strings.ToLower(string(scheme(dc))),
	}
	if dc.Spec.RollingRestartRequestedAt != "" {
		annotations[names.AnnotationRestartedAt] = dc.Spec.RollingRestartRequestedAt
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: names.StatefulSet(dc, rack.Name), Namespace: dc.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			ServiceName:         names.HeadlessService(dc),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy:      appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:            &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       pod,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: names.DataVolume, Labels: labels},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: dc.Spec.Storage.StorageClassName,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
				},
			}},
			// Volumes outlive scale-downs; the operator deletes those of
			// decommissioned nodes itself.
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
		},
	}
}

// agentContainer is the backup agent, beside the node, sharing its data.
func agentContainer(dc *v1.CassandraRoomDatacenter, b *v1.S3Storage, image string, data corev1.VolumeMount, tlsMounts []corev1.VolumeMount) corev1.Container {
	args := []string{"agent",
		fmt.Sprintf("--listen=:%d", names.AgentPort),
		fmt.Sprintf("--management=127.0.0.1:%d", names.MgmtPort(dc)),
		"--data-dir=" + names.DataDir,
		"--bucket=" + b.Bucket, "--prefix=" + b.Prefix, "--region=" + b.Region, "--endpoint=" + b.Endpoint,
		fmt.Sprintf("--concurrency=%d", max(b.Concurrency, 4)),
		"--cluster=" + dc.Spec.ClusterName, "--datacenter=" + dc.DCName(),
	}
	if a := b.CommitLogArchive; a != nil {
		every := time.Minute
		if a.Interval != nil && a.Interval.Duration > 0 {
			every = a.Interval.Duration
		}
		args = append(args, "--commitlog-interval="+every.String())
	}
	if b.ForcePathStyle {
		args = append(args, "--path-style")
	}
	if dc.Spec.TLS.Management != nil {
		args = append(args, "--management-tls="+names.TLSDir+"/management")
	}
	envs := []corev1.EnvVar{env("POD_NAME", "metadata.name")}
	if b.CredentialsSecret != nil {
		ref := func(key string) *corev1.EnvVarSource {
			return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: b.CredentialsSecret.SecretName}, Key: key}}
		}
		envs = append(envs,
			corev1.EnvVar{Name: "AWS_ACCESS_KEY_ID", ValueFrom: ref("access_key_id")},
			corev1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: ref("secret_access_key")})
	}
	return corev1.Container{Name: "backup-agent", Image: image, ImagePullPolicy: corev1.PullPolicy(dc.Spec.ImagePullPolicy),
		Args: args, Env: envs, Ports: []corev1.ContainerPort{{Name: "agent", ContainerPort: names.AgentPort}},
		VolumeMounts: append([]corev1.VolumeMount{data}, tlsMounts...),
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}}}
}

func affinity(dc *v1.CassandraRoomDatacenter, rack v1.Rack) *corev1.Affinity {
	a := &corev1.Affinity{}
	if len(rack.NodeAffinityLabels) > 0 {
		var reqs []corev1.NodeSelectorRequirement
		for k, v := range rack.NodeAffinityLabels {
			reqs = append(reqs, corev1.NodeSelectorRequirement{Key: k, Operator: corev1.NodeSelectorOpIn, Values: []string{v}})
		}
		sortRequirements(reqs)
		a.NodeAffinity = &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}}}}
	}
	if !dc.Spec.Pod.AllowMultipleNodesPerWorker {
		a.PodAntiAffinity = &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: "kubernetes.io/hostname",
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				names.LabelName: names.AppName, names.LabelCluster: names.Sanitize(dc.Spec.ClusterName)}},
		}}}
	}
	return a
}

func ptr[T any](v T) *T { return &v }

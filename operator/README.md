# cassandra-room-operator

A Kubernetes operator for [cassandra-room](https://github.com/pbonard/cassandra-room)
clusters. It does what k8ssandra-operator, cass-operator, Reaper and Medusa do
for Cassandra, as one Go binary built on controller-runtime:

| | Cassandra | here |
|---|---|---|
| Data centers | cass-operator (`CassandraDatacenter`) | `CassandraRoomDatacenter` |
| Clusters across Kubernetes clusters | k8ssandra-operator (`K8ssandraCluster`, `ClientConfig`) | `CassandraRoomCluster`, `ClientConfig` |
| Node agent | Management API for Apache Cassandra (a JVM agent) | cassandra-room's own management API (`-admin-address`) |
| Configuration | cass-config-builder, `cassandra.yaml` | server flags, and a flags file node-init writes per pod |
| Repairs | Reaper | `RepairSchedule` |
| Backups | Medusa (`MedusaBackupJob`, `MedusaBackupSchedule`, `MedusaRestoreJob`) | `CassandraRoomBackup`, `CassandraRoomBackupSchedule`, `CassandraRoomRestore` |

```
operator/
  api/v1alpha1/        the API (CRDs generated in config/crd)
  cmd/                 the binary: manager | node-init | agent
  internal/dc/         data centers (the data plane)
  internal/cluster/    clusters across Kubernetes clusters (the control plane)
  internal/remote/     clients of remote Kubernetes clusters (ClientConfig)
  internal/repair/     scheduled repairs
  internal/backup/     backups and restores (data and control plane)
  internal/agent/      the backup agent beside each node
  internal/catalog/    the backups' layout in the object store
  internal/objstore/   S3 (and S3-compatible) storage
  internal/nodeinit/   the nodes' init container
  internal/mgmt/       the management API's client
  config/              CRDs, RBAC, deployment (kustomize), samples
  charts/              the Helm chart
```

## Install

```
kubectl apply -k operator/config/default
# or
helm install cassandra-room-operator operator/charts/cassandra-room-operator -n cassandra-room-system --create-namespace
```

The image is built from `operator/Dockerfile`; cassandra-room's from its own
`Dockerfile`.

## A data center

Each data center is a StatefulSet per rack, nodes spread evenly over the
racks. Each pod runs:

- **node-init** (init container, the operator's image): writes the flags that
  depend on the pod to a file the server reads (`-flags-file`): its
  addresses, the seeds, and the node it replaces, if any.
- **cassandra-room**: probes on its management API (`/api/v1/health`,
  `/api/v1/ready`), with a startup probe long enough for a bootstrap's
  streaming.
- **backup-agent** (with backups configured, the operator's image): shares
  the data volume.

The controller:

- **scales up** one node at a time, the rack with the fewest nodes first. A
  node that had joined before (its volume exists, after a stop) restarts at
  once;
- **scales down** by decommissioning the last node of the fullest rack. Once
  it has left the ring, its pod and volume go;
- **replaces** dead nodes listed in `replaceNodes`: the pod comes back with an
  empty volume and `-replace-address` of the dead node's host id;
- **rolls out** template changes (image, configuration,
  `rollingRestartRequestedAt`) one rack at a time, each rack's pods one at a
  time;
- **stops** a data center (`stopped: true`): no pods, volumes kept;
- **creates the superuser role** from its secret (generated if missing). It
  logs in once as the default superuser, then disables that login;
- **keeps the metadata service's members**: three nodes spread over the
  racks, with `manageCMS` or from the cluster;
- **reports** each node's host id, address and ring status, and the seeds.

Seeds are the first pod of each of the first three racks. A pod's IP changes
when it restarts: cassandra-room nodes keep their identity (host id) and their
place in the metadata service across address changes.

### Addresses

`networking.mode`:

- **PodIP** (the default): pod IPs, for one Kubernetes cluster, or clusters
  whose pod networks are routed to each other.
- **HostNetwork**: the hosts' addresses.
- **LoadBalancer**: a Service of type LoadBalancer per node, whose address is
  the node's broadcast address (and its client address). With `preferLocal`,
  nodes of a data center talk to each other at their pod addresses.

### TLS

Secrets in the format cert-manager writes (`tls.crt`, `tls.key`, `ca.crt`):

- `tls.internode`: mutually authenticated;
- `tls.client`: the native port, optionally requiring client certificates or
  also accepting plaintext;
- `tls.management`: the management API.

In a multi-cluster deployment, the control plane copies the secrets it has
into the data centers' clusters when they are missing there.

## Clusters across Kubernetes clusters

A `CassandraRoomCluster` lists data centers. Each may name a `ClientConfig`
(`k8sContext`): a kubeconfig in a secret, giving access to the Kubernetes
cluster that runs it. The operator in the control plane's cluster:

- creates each data center's `CassandraRoomDatacenter` there, in order, each
  once the previous ones are ready;
- links them: each data center's seeds are the others' `additionalSeeds`;
- spreads the metadata service's members over the data centers (an odd
  number, at least three, at most seven) and has the first data center
  maintain them;
- copies the superuser, TLS and backup credentials secrets;
- creates the cluster's repair schedules beside the first data center;
- removes a data center dropped from the spec: its nodes are decommissioned,
  then the object deleted. First change the keyspaces so they no longer
  replicate to it.

The control plane watches the objects it creates in remote clusters (data
centers, and the per-data-center parts of backups and restores) through a
cache per remote cluster, restarted when its kubeconfig changes. Each such
object is annotated with the object it belongs to, since owner references
cannot cross clusters; it resyncs only every few minutes as a safety net. The
kubeconfig's user must be able to list and watch those objects.

Every Kubernetes cluster runs the operator. The ones without the control
plane run `manager --control-plane=false`. See `config/samples/multi-dc.yaml`
for nine nodes in three data centers in three clusters.

## Repairs (`RepairSchedule`)

A run splits each keyspace's ring into segments: about `segmentsPerNode` per
node. It repairs each segment through the management API of a replica in the
schedule's data center; every replica of the range takes part. The scheduler:

- runs `parallelism` segments at once, never two on the same replicas;
- pauses after a segment for `duration × (1/intensity − 1)`;
- retries a failing segment `maxRetries` times;
- stops starting segments while `paused` (the run resumes where it was);
- keeps the plan in a ConfigMap, so a run survives an operator restart;
- schedules runs by cron (`schedule`), or at once when `runRequestedAt`
  changes;
- keeps the latest runs' results (repaired, failed, mismatches, bytes
  streamed) in the status.

## Backups and restores (S3)

Every node's agent backs it up:

1. a snapshot through the management API (hard links: no copy);
2. the snapshot's files uploaded under their content's SHA-256: a file an
   earlier backup uploaded is not uploaded again (sstables are immutable), so
   every backup is incremental and still complete;
3. a manifest of the node's tables and files.

Each data center's part is complete once all its nodes' are (an index object
records it).

- **Layout:** `<cluster>/data/<sha256>`, `<cluster>/backups/<backup>/<dc>/<pod>.json`,
  `<cluster>/backups/<backup>/<dc>/index.json`.
- **Deleting** a `CassandraRoomBackup` deletes its manifests, then the files no
  other backup names.
- **Schedules:** a `CassandraRoomBackupSchedule` creates backups by cron and
  deletes the oldest beyond `keep`.

A `CassandraRoomRestore` restores a backup. The first data center truncates
the backup's tables, which gives them new, empty storage on every node;
then, by `mode`:

- **Import** (the default; the cluster the backup was taken from, with the
  same data centers and pods, as Medusa requires): every node's agent
  downloads its own files, checks them against their hashes, and imports
  them.
- **Load** (any topology: more or fewer nodes, other tokens or replication,
  or another cluster with `sourceCluster`, its name in the same bucket):
  the first data center's nodes share out one data center's files of the
  backup (`sourceDatacenter`, default its first) and write them through
  themselves as coordinators, to the replicas that own each partition now
  (sstableloader), at `consistency` (default `ALL`). Timestamps and
  deletions are kept. The tables must exist: create the schema first.

Writes to the restored tables meanwhile are lost, so stop writing to them
first.

### Point in time

With `backup.commitLogArchive`, each node keeps every commit log segment
(`-commitlog-archive-dir`, hard links on its volume) and its agent uploads
them every `interval` (default 1m) to
`<cluster>/commitlog/<dc>/<pod>/<round>-<segment>`, then deletes them
locally. Each upload round first closes the node's current segment, so a
round holds every write acknowledged before it.

A restore with `pointInTime` (Import only) imports the last backup before
that time, then each node replays its segments from the backup's snapshot
up to the first round after the point in time, leaving out cells and
deletions written later (by write timestamp, as Cassandra's
`restore_point_in_time` does). Writes are matched to tables by the storage
ids the backup recorded, so rows truncated before the backup do not come
back. Deleting backups prunes the segments older than the oldest backup
kept.

Storage: `backup.bucket`, `prefix`, `region`, `endpoint` with
`forcePathStyle` for MinIO and similar stores, and `credentialsSecret`
(`access_key_id`, `secret_access_key`). Without credentials, the default AWS
chain applies (IRSA, instance roles).

## Development

```
cd operator
make generate manifests   # deepcopy, CRDs, RBAC (controller-gen)
make test                 # unit and envtest tests (setup-envtest)
```

- **Controller tests:** a real API server (envtest), with a fake kubelet
  (pods, volumes, StatefulSet rollouts) and fake nodes. The multi-cluster test
  runs two API servers.
- **`internal/e2e`:** runs against a real cassandra-room binary
  (`CASSANDRA_ROOM_BIN=...`): the management API client, the superuser setup
  over CQL, and a backup, truncate and restore through the agent.
- **`internal/samples`:** creates every sample on the API server, which
  validates them against the CRDs.

## Metrics

Every node serves Prometheus metrics on its management API (`/metrics`:
client request latencies and errors by kind and consistency level, table
sizes, compactions, hints, dropped messages, thread pools, nodes up and down,
Accord). The pods carry the `prometheus.io/scrape`, `port`, `path` and
`scheme` annotations; with the Prometheus operator, a PodMonitor selecting
`app.kubernetes.io/name: cassandra-room` on the `management` port does the
same. The operator exposes controller-runtime's metrics.

## Not done

See [`docs/gap-analysis.md`](docs/gap-analysis.md) for a comparison with
k8ssandra-operator and cass-operator, and what remains.

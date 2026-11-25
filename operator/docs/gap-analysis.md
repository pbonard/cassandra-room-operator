# Compared with k8ssandra-operator and cass-operator

This compares cassandra-room-operator with k8ssandra-operator and
cass-operator (their `main` branches, November 2026: the `apis/` and
`controllers/` trees), and lists what remains to do, most important first.

## Can it deploy a cassandra-room cluster today?

In principle, yes. Nothing has run on a real Kubernetes cluster yet. What
has been tested:

- Every controller runs against a real API server (envtest), with a fake
  kubelet and fake nodes. This includes the multi-cluster tests with two
  API servers.
- The management client, the superuser setup, and backups, restores,
  loads and point-in-time restores run against a real cassandra-room
  binary (`internal/e2e`).
- The samples are validated by the CRDs.

What a first real deployment needs:

1. **Images.** Build and push cassandra-room's image (its `Dockerfile`) and
   the operator's (`operator/Dockerfile`, which also provides node-init and
   the backup agent). Then set them in the chart's `image` value and in the
   `CassandraRoomCluster`'s `image`.
2. **Install.** `helm install` or `kubectl apply -k config/default`, then
   apply `config/samples/single-dc.yaml`.
3. **Check on kind, then a cloud cluster.** These are the things envtest
   cannot show:
   - pods restarting at new pod IPs;
   - real PVC binding and pod scheduling with the anti-affinity rules;
   - probe timings while a bootstrap streams data;
   - running as the non-root user 65532 with `fsGroup`, which must be able
     to write the volume;
   - the agent's S3 access (IRSA or keys);
   - TLS secrets from cert-manager;
   - LoadBalancer addresses.
4. **Multi-cluster.**
   - Create a kubeconfig secret per remote cluster, whose user can
     create, list and watch the operator's CRDs, secrets and configmaps
     there.
   - Make the pod networks routable between clusters (or use
     `networking.mode: LoadBalancer` or `HostNetwork`).

The CI does not yet build images or run an end-to-end test on kind. That
is the first item below.

## What both have that is done here

| | k8ssandra-operator / cass-operator | here |
|---|---|---|
| Data centers, racks, scale up and down, decommission | `CassandraDatacenter` | `CassandraRoomDatacenter` |
| Replace dead nodes | `replaceNodes`, `CassandraTask replacenode` | `replaceNodes` |
| Rolling restarts, stop and resume | `rollingRestartRequested`, `stopped` | same |
| Superuser | `superuserSecretName` | `superuserSecret` (generated if missing) |
| Multi-cluster | `K8ssandraCluster`, `ClientConfig` | `CassandraRoomCluster`, `ClientConfig`, with watches through remote caches |
| Additional seeds, external seeds | `additionalSeeds` | same, wired across data centers |
| Host network, load balancers | `networking.hostNetwork`, `nodePort` | `networking.mode` (PodIP, HostNetwork, LoadBalancer) |
| Internode and client TLS | keystores (`*EncryptionStores`) | PEM secrets (cert-manager's format), management TLS too |
| PodDisruptionBudget | yes | yes |
| Repairs | Reaper (a separate service with its own storage) | `RepairSchedule`, in the operator (segments, intensity, parallelism, pause, retries) |
| Backups | Medusa (a Python sidecar) | `CassandraRoomBackup`, schedules, retention (`keep`), content-addressed incremental files |
| Restores | `MedusaRestoreJob`, same topology | Import (same topology), **Load into any topology or cluster**, **point in time** |
| Point in time | Medusa `pitr` (commit log archiving to the bucket) | `commitLogArchive`: rounds uploaded by the agent, replay with timestamp cutoff |
| Metrics | MCAC or the management API's `/metrics`, `ServiceMonitor` | `/metrics` on every node, scrape annotations |
| Metadata service (CMS) | none: Cassandra 5's TCM is not managed | members spread over data centers and racks |

## What remains, by priority

### 1. Must have before production

- **Build images and run an end-to-end test on kind in CI.** Build both
  images, install the chart, create a three-node data center, write with
  cqlsh, scale up and down, back up to MinIO and restore. Nothing replaces
  this.
- **Admission webhooks.** k8ssandra validates updates
  (`k8ssandracluster_webhook.go`, `validate_cluster_update.go`): it refuses
  to rename data centers, change the storage class, or remove a data center
  that keyspaces still replicate to. Here, such changes are only caught
  while reconciling, or not at all. CEL rules (`x-kubernetes-validations`)
  in the CRDs need no webhook server and cover most of this:
  - immutable `datacenterName`, `clusterName`, `storage`;
  - `size` at least the number of racks;
  - `mode: Load` with `pointInTime` refused.
- **Adding a data center to a cluster that has data.** k8ssandra changes
  the replication of the system keyspaces (`updateReplicationOfSystemKeyspaces`)
  and, with the `k8ssandra.io/dc-replication` annotation, of the user
  keyspaces. It then rebuilds the new data center's nodes from an existing
  one (`reconcileDcRebuild`, a `CassandraTask rebuild`).
  - Here, roles live in the metadata log, so system keyspaces need nothing.
  - But a new data center gets no data until the user alters the keyspaces
    and a repair runs.
  - To do: a `replication` field per data center that the cluster
    controller applies once the data center is ready (`ALTER KEYSPACE`).
    Then a repair run of the new data center (or a cassandra-room
    `rebuild` endpoint streaming from a source data center, which would be
    faster).
- **Volume expansion.** cass-operator resizes PVCs when the storage class
  allows it (`AllowStorageChangesAnnotation` in `reconcile_racks.go`).
  Here, changing `storage.size` does nothing: StatefulSet volume claim
  templates are immutable. Doing this means patching each PVC, then
  recreating the StatefulSet with `--cascade=orphan`.

### 2. Operations users expect

- **Tasks** (cass-operator's `CassandraTask` and `ScheduledTask`,
  k8ssandra's `K8ssandraTask` across data centers). Here there are only
  `rollingRestartRequestedAt`, `replaceNodes`, and repairs. Proposed: a
  `CassandraRoomTask` with `restart`, `cleanup`, `flush`, `compact`,
  `rebuild` and `upgradesstables` for a data center or the whole cluster,
  with concurrency and a cron schedule.
  - cleanup and flush already exist on the management API;
  - compact, rebuild and garbage collection need endpoints in
    cassandra-room;
  - scrub, move and tsreload have no cassandra-room equivalent.
- **Canary upgrades and upgrade control** (`canaryUpgrade`,
  `canaryUpgradeCount`, `forceUpgradeRacks`, `maxUnavailable`,
  `minReadySeconds`): roll out a new image to N nodes first, then stop for
  a check.
- **Pod customization.** cass-operator has `podTemplateSpec`
  (`containers`, `initContainers`, `extraVolumes`, `additionalLabels`,
  `additionalAnnotations`, `priorityClassName`, `podSecurityContext`,
  `readOnlyRootFilesystem`, `additionalServiceConfig`). Here the pod is
  fixed, apart from resources, node selector, tolerations and service
  account. A strategic-merge `podTemplate` is the general answer.
- **Users** (cass-operator's `users`: roles created from secrets). Here
  only the superuser is managed.
- **A contact points service** (k8ssandra's `contact_points_service.go`):
  a cluster-wide Service clients can use. Each data center has its own
  service here.

### 3. Backups

- **Storage providers.** Medusa supports S3 and S3-compatible stores, GCS,
  Azure and local storage. Here only S3 and S3-compatible stores are
  supported. GCS works through its S3 interoperability (HMAC keys). Native
  GCS and Azure clients fit behind `objstore.Store`.
- **Purges by age** (`maxBackupAge`) and a separate purge task
  (`MedusaTask purge`). Here, schedules keep the last `keep`; there is no
  age limit and no purge of unscheduled backups.
- **Transfer limits** (`transferMaxBandwidth`, multipart thresholds). Here
  there is concurrency only.
- **Syncing the catalog into objects.** Medusa's `MedusaTask sync` creates
  `MedusaBackup` objects for the backups in the bucket, for example in a
  new Kubernetes cluster. Here a restore of another cluster names the
  backup directly (`sourceCluster`), but the backups are not listed as
  objects.
- **Schema in backups.** Load needs the target's schema to exist. Storing
  each keyspace's `DESCRIBE` output in the backup would let a restore
  create it.

### 4. Repairs

- **Reaper's UI and REST API, incremental repairs, and per-table
  schedules with blacklists.** `RepairSchedule` covers full repairs by
  segment. It has no UI and no incremental repairs (cassandra-room has no
  repaired/unrepaired sstable split).

### 5. Observability and secrets

- **A ServiceMonitor or PodMonitor** created by the operator when the
  Prometheus operator's CRDs exist (k8ssandra's `telemetry.prometheus`).
  Today it's annotations only.
- **Logs** (k8ssandra's Vector agent and the `server-system-logger`
  sidecar). cassandra-room logs to stdout, so any cluster log collector
  works. Nothing is missing unless structured logs are wanted.
- **Secret replication** (`ReplicatedSecret`) and external secret
  providers (`secretsProvider: external`, Vault injection). Here the
  control plane copies the superuser, TLS and backup credentials secrets it
  knows, but there is no general replication object and no external
  provider mode.
- **Management API authentication** (`managementApiAuth`). Here
  management TLS with client certificates covers it.

### 6. Not applicable

- `serverType` and `serverVersion` (DSE, HCD), `dseWorkloads`, the JMX
  init container, MCAC, cass-config-builder: there is one server, and its
  configuration is flags.
- CDC (`cdc`, Pulsar): cassandra-room has no CDC log.
- Stargate: removed from k8ssandra-operator. CQL is the API.

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Backups (Medusa's): every node snapshots its tables and its backup agent
// uploads the snapshot's files to the object store. Files are named by
// their content's SHA-256, so a file already uploaded by an earlier backup
// is not uploaded again (sstables are immutable), and each node's
// manifest lists the files of each table. A restore truncates the tables
// (new, empty storage on every node), then each node imports the files of
// its own manifest: the cluster must have the topology it had (the same
// pods), as with Medusa.

// BackupPhase is a backup's or restore's state.
type BackupPhase string

const (
	BackupPending   BackupPhase = "Pending"
	BackupRunning   BackupPhase = "Running"
	BackupSucceeded BackupPhase = "Succeeded"
	BackupFailed    BackupPhase = "Failed"
	// RestoreTruncating: the tables are being truncated.
	RestoreTruncating BackupPhase = "Truncating"
	// RestoreImporting: the nodes are importing their files.
	RestoreImporting BackupPhase = "Importing"
)

// NodeTransfer is a node's part of a backup or restore.
type NodeTransfer struct {
	Pod    string      `json:"pod"`
	HostID string      `json:"hostId,omitempty"`
	Phase  BackupPhase `json:"phase"`
	// Files and Bytes of the node's backup; Transferred the bytes uploaded
	// (new files) or downloaded.
	Files       int32  `json:"files"`
	Bytes       int64  `json:"bytes"`
	Transferred int64  `json:"transferred"`
	Error       string `json:"error,omitempty"`
}

// DatacenterBackupSpec backs up a data center's nodes.
type DatacenterBackupSpec struct {
	// Datacenter is the CassandraRoomDatacenter (in this namespace).
	Datacenter string `json:"datacenter"`
	// Backup is the backup's name, shared by the cluster's data centers
	// (default: the object's name).
	// +optional
	Backup string `json:"backup,omitempty"`
	// Keyspaces to back up (default: every user keyspace).
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
}

// DatacenterBackupStatus is a data center backup's state.
type DatacenterBackupStatus struct {
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// +optional
	Nodes []NodeTransfer `json:"nodes,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Datacenter",type=string,JSONPath=`.spec.datacenter`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startedAt`

// DatacenterBackup is a data center's part of a backup.
type DatacenterBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatacenterBackupSpec   `json:"spec,omitempty"`
	Status DatacenterBackupStatus `json:"status,omitempty"`
}

// BackupName is the backup's name in the object store.
func (b *DatacenterBackup) BackupName() string {
	if b.Spec.Backup != "" {
		return b.Spec.Backup
	}
	return b.Name
}

// +kubebuilder:object:root=true

// DatacenterBackupList lists data center backups.
type DatacenterBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatacenterBackup `json:"items"`
}

// DatacenterRestoreSpec restores a backup into a data center's nodes.
type DatacenterRestoreSpec struct {
	Datacenter string `json:"datacenter"`
	// Backup is the backup's name in the object store.
	Backup string `json:"backup"`
	// Keyspaces to restore (default: every keyspace of the backup).
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
	// Truncate makes this data center truncate the tables first (for the
	// whole cluster: one data center does it).
	// +optional
	Truncate bool `json:"truncate,omitempty"`
	// Import starts the nodes' imports (once the tables are truncated).
	// +optional
	Import bool `json:"import,omitempty"`
	// Load loads the files of SourceDatacenter's nodes through this data
	// center's nodes instead of importing each node's own.
	// +optional
	Load bool `json:"load,omitempty"`
	// SourceCluster is the cluster whose backup this is, by its name in the
	// object store (default: this data center's cluster). Needs Load.
	// +optional
	SourceCluster string `json:"sourceCluster,omitempty"`
	// SourceDatacenter is the backup's data center to load (default: its
	// first).
	// +optional
	SourceDatacenter string `json:"sourceDatacenter,omitempty"`
	// Consistency of a load's writes (default ALL).
	// +optional
	Consistency string `json:"consistency,omitempty"`
	// PointInTime replays each node's archived commit log after the import,
	// up to this time.
	// +optional
	PointInTime *metav1.Time `json:"pointInTime,omitempty"`
}

// DatacenterRestoreStatus is a data center restore's state.
type DatacenterRestoreStatus struct {
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`
	// Truncated is set once this data center truncated the tables.
	// +optional
	Truncated bool `json:"truncated,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// +optional
	Nodes []NodeTransfer `json:"nodes,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Datacenter",type=string,JSONPath=`.spec.datacenter`
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.backup`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// DatacenterRestore is a data center's part of a restore.
type DatacenterRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatacenterRestoreSpec   `json:"spec,omitempty"`
	Status DatacenterRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatacenterRestoreList lists data center restores.
type DatacenterRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatacenterRestore `json:"items"`
}

// CassandraRoomBackupSpec backs up every data center of a cluster.
type CassandraRoomBackupSpec struct {
	// Cluster is the CassandraRoomCluster (in this namespace).
	Cluster string `json:"cluster"`
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
}

// DatacenterProgress is a data center's part of a cluster-wide backup or
// restore.
type DatacenterProgress struct {
	Name  string      `json:"name"`
	Phase BackupPhase `json:"phase,omitempty"`
	Nodes int32       `json:"nodes"`
	Done  int32       `json:"done"`
	Bytes int64       `json:"bytes"`
}

// CassandraRoomBackupStatus is a cluster backup's state.
type CassandraRoomBackupStatus struct {
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// +optional
	Datacenters []DatacenterProgress `json:"datacenters,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=crbackup
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Started",type=date,JSONPath=`.status.startedAt`

// CassandraRoomBackup is a backup of a cluster; deleting it deletes the
// backup from the object store.
type CassandraRoomBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CassandraRoomBackupSpec   `json:"spec,omitempty"`
	Status CassandraRoomBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CassandraRoomBackupList lists cluster backups.
type CassandraRoomBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CassandraRoomBackup `json:"items"`
}

// CassandraRoomBackupScheduleSpec backs a cluster up on a schedule
// (MedusaBackupSchedule).
type CassandraRoomBackupScheduleSpec struct {
	Cluster string `json:"cluster"`
	// Schedule is a cron expression.
	Schedule string `json:"schedule"`
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
	// Keep is the number of this schedule's successful backups kept; older
	// ones are deleted (default: all kept).
	// +optional
	Keep int32 `json:"keep,omitempty"`
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// CassandraRoomBackupScheduleStatus is a backup schedule's state.
type CassandraRoomBackupScheduleStatus struct {
	// +optional
	LastBackup string `json:"lastBackup,omitempty"`
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	// +optional
	NextRunAt *metav1.Time `json:"nextRunAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Last",type=string,JSONPath=`.status.lastBackup`

// CassandraRoomBackupSchedule creates CassandraRoomBackups on a schedule.
type CassandraRoomBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CassandraRoomBackupScheduleSpec   `json:"spec,omitempty"`
	Status CassandraRoomBackupScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CassandraRoomBackupScheduleList lists backup schedules.
type CassandraRoomBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CassandraRoomBackupSchedule `json:"items"`
}

// RestoreMode is how a restore puts the files back.
// +kubebuilder:validation:Enum=Import;Load
type RestoreMode string

const (
	// RestoreImport: each node imports its own files (the same topology).
	RestoreImport RestoreMode = "Import"
	// RestoreLoad: the first data center's nodes load a data center's
	// files through themselves to the replicas (any topology).
	RestoreLoad RestoreMode = "Load"
)

// CassandraRoomRestoreSpec restores a backup into a cluster.
type CassandraRoomRestoreSpec struct {
	Cluster string `json:"cluster"`
	// Backup is the CassandraRoomBackup's name or, with SourceCluster, the
	// backup's name in the object store.
	Backup string `json:"backup"`
	// Keyspaces to restore (default: every keyspace of the backup).
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
	// Mode is Import (default) or Load.
	// +optional
	Mode RestoreMode `json:"mode,omitempty"`
	// SourceCluster restores a backup of another cluster (its name in the
	// object store, which must be the same bucket): Load only. The tables
	// must exist (create the schema first).
	// +optional
	SourceCluster string `json:"sourceCluster,omitempty"`
	// SourceDatacenter is the backup's data center that Load reads
	// (default: its first). Each data center holds every row it
	// replicates, so one suffices when it replicates every keyspace.
	// +optional
	SourceDatacenter string `json:"sourceDatacenter,omitempty"`
	// Consistency of Load's writes (default ALL).
	// +optional
	Consistency string `json:"consistency,omitempty"`
	// PointInTime restores the cluster as it was at this time: the backup,
	// then each node's archived commit log up to it, leaving out what was
	// written later (by write timestamp). Import only; the backup must be
	// the last one before it, and the data centers' backup storage must
	// archive commit logs.
	// +optional
	PointInTime *metav1.Time `json:"pointInTime,omitempty"`
}

// CassandraRoomRestoreStatus is a cluster restore's state.
type CassandraRoomRestoreStatus struct {
	// +optional
	Phase BackupPhase `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// +optional
	Datacenters []DatacenterProgress `json:"datacenters,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=crrestore
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.cluster`
// +kubebuilder:printcolumn:name="Backup",type=string,JSONPath=`.spec.backup`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// CassandraRoomRestore restores a backup into a cluster. Clients should not
// write to the restored tables meanwhile.
type CassandraRoomRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CassandraRoomRestoreSpec   `json:"spec,omitempty"`
	Status CassandraRoomRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// CassandraRoomRestoreList lists cluster restores.
type CassandraRoomRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CassandraRoomRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatacenterBackup{}, &DatacenterBackupList{}, &DatacenterRestore{}, &DatacenterRestoreList{},
		&CassandraRoomBackup{}, &CassandraRoomBackupList{}, &CassandraRoomBackupSchedule{}, &CassandraRoomBackupScheduleList{},
		&CassandraRoomRestore{}, &CassandraRoomRestoreList{})
}

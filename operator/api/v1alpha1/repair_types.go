package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RepairScheduleSettings are how and when a cluster is repaired (Reaper's
// repair schedules): each run splits the ring into segments and repairs
// them one by one through the nodes' management API, with pauses that
// keep the load down.
type RepairScheduleSettings struct {
	// Keyspaces to repair (default: every user keyspace).
	// +optional
	Keyspaces []string `json:"keyspaces,omitempty"`
	// Schedule is a cron expression (default "@weekly").
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// SegmentsPerNode splits each node's share of a keyspace's ring into
	// this many segments (default 16).
	// +optional
	SegmentsPerNode int32 `json:"segmentsPerNode,omitempty"`
	// Intensity in (0, 1]: after a segment taking d, the run pauses
	// d*(1/intensity-1) (default "1": no pause).
	// +optional
	Intensity string `json:"intensity,omitempty"`
	// Parallelism is the number of segments repaired at once, on
	// different nodes (default 1).
	// +optional
	Parallelism int32 `json:"parallelism,omitempty"`
	// SegmentTimeout bounds a segment's repair (default 30m).
	// +optional
	SegmentTimeout *metav1.Duration `json:"segmentTimeout,omitempty"`
	// MaxRetries of a failing segment before it is given up (default 3).
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`
	// Paused stops starting segments; a run resumes where it stopped.
	// +optional
	Paused bool `json:"paused,omitempty"`
	// RunRequestedAt starts a run now when it changes.
	// +optional
	RunRequestedAt string `json:"runRequestedAt,omitempty"`
	// History is the number of finished runs kept in the status (default 10).
	// +optional
	History int32 `json:"history,omitempty"`
}

// RepairScheduleSpec is a schedule of repairs run from a data center's
// nodes.
type RepairScheduleSpec struct {
	// Datacenter is the CassandraRoomDatacenter (in this namespace) whose
	// nodes coordinate the repairs; every replica of a range takes part.
	Datacenter             string `json:"datacenter"`
	RepairScheduleSettings `json:",inline"`
}

// RepairRunState is a run's state.
type RepairRunState string

const (
	RepairRunning  RepairRunState = "Running"
	RepairDone     RepairRunState = "Done"
	RepairErrored  RepairRunState = "Errored"
	RepairAborted  RepairRunState = "Aborted"
)

// RepairRun is a run of a schedule.
type RepairRun struct {
	ID        string         `json:"id"`
	State     RepairRunState `json:"state"`
	StartedAt metav1.Time    `json:"startedAt"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	Segments   int32        `json:"segments"`
	Done       int32        `json:"done"`
	Failed     int32        `json:"failed"`
	// Mismatches and StreamedBytes sum the segments' repairs.
	Mismatches    int64 `json:"mismatches"`
	StreamedBytes int64 `json:"streamedBytes"`
	// +optional
	Message string `json:"message,omitempty"`
}

// RepairScheduleStatus is a schedule's state.
type RepairScheduleStatus struct {
	// Current is the run in progress.
	// +optional
	Current *RepairRun `json:"current,omitempty"`
	// History are the latest finished runs, newest first.
	// +optional
	History []RepairRun `json:"history,omitempty"`
	// +optional
	NextRunAt *metav1.Time `json:"nextRunAt,omitempty"`
	// +optional
	LastRunRequest string `json:"lastRunRequest,omitempty"`
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Datacenter",type=string,JSONPath=`.spec.datacenter`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Done",type=integer,JSONPath=`.status.current.done`
// +kubebuilder:printcolumn:name="Segments",type=integer,JSONPath=`.status.current.segments`
// +kubebuilder:printcolumn:name="Next",type=date,JSONPath=`.status.nextRunAt`

// RepairSchedule repairs a cluster on a schedule.
type RepairSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RepairScheduleSpec   `json:"spec,omitempty"`
	Status RepairScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RepairScheduleList lists repair schedules.
type RepairScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RepairSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RepairSchedule{}, &RepairScheduleList{})
}

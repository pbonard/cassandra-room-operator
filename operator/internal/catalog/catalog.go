// Package catalog is the layout of backups in the object store:
//
//	<cluster>/data/<sha256>                          a file, by content
//	<cluster>/backups/<backup>/<dc>/<pod>.json       a node's manifest
//	<cluster>/backups/<backup>/<dc>/index.json       a data center's part, once complete
//	<cluster>/commitlog/<dc>/<pod>/<time>-<segment>  a node's archived commit log
//
// Commit log segments are uploaded in rounds: each round first closes the
// node's current segment, so a round's segments hold every write
// acknowledged before the round's time (in the key, in nanoseconds).
//
// Files are shared by every backup that has them (sstables are immutable:
// a backup uploads only the files no earlier one did). Deleting a backup
// deletes its manifests; files no manifest names are then collected.
package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
)

// File is a file of a backup.
type File struct {
	Name   string `json:"name"`   // its name in the table's snapshot
	SHA256 string `json:"sha256"` // its object
	Size   int64  `json:"size"`
}

// Table is a table's files in a node's backup.
type Table struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table"`
	ID       string `json:"id"`
	// Storage names the table's storage in the node's commit log.
	Storage uint64 `json:"storage,omitempty"`
	Files   []File `json:"files"`
}

// Manifest is a node's backup.
type Manifest struct {
	Cluster    string    `json:"cluster"`
	Datacenter string    `json:"datacenter"`
	Backup     string    `json:"backup"`
	Pod        string    `json:"pod"`
	HostID     string    `json:"hostId"`
	Address    string    `json:"address"`
	CreatedAt  time.Time `json:"createdAt"`
	// SnapshotAt is when the snapshot started: every write acknowledged
	// before is in its files.
	SnapshotAt time.Time `json:"snapshotAt,omitempty"`
	Tables     []Table   `json:"tables"`
}

// Size is the backup's bytes and files.
func (m *Manifest) Size() (files int, bytes int64) {
	for _, t := range m.Tables {
		for _, f := range t.Files {
			files++
			bytes += f.Size
		}
	}
	return
}

// Index is a data center's complete part of a backup.
type Index struct {
	Datacenter string    `json:"datacenter"`
	Pods       []string  `json:"pods"`
	Keyspaces  []string  `json:"keyspaces,omitempty"`
	FinishedAt time.Time `json:"finishedAt"`
}

// DataKey is a file's object.
func DataKey(cluster, sha string) string { return path.Join(cluster, "data", sha) }

// BackupPrefix holds a backup's manifests.
func BackupPrefix(cluster, backup string) string { return path.Join(cluster, "backups", backup) + "/" }

// ManifestKey is a node's manifest.
func ManifestKey(cluster, backup, dc, pod string) string {
	return path.Join(cluster, "backups", backup, dc, pod+".json")
}

// IndexKey is a data center's index.
func IndexKey(cluster, backup, dc string) string {
	return path.Join(cluster, "backups", backup, dc, "index.json")
}

// PutJSON stores a value as JSON.
func PutJSON(ctx context.Context, s objstore.Store, key string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return s.Put(ctx, key, bytes.NewReader(b), int64(len(b)))
}

// GetJSON loads a JSON object.
func GetJSON(ctx context.Context, s objstore.Store, key string, v any) error {
	r, err := s.Get(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// DeleteDatacenter deletes a data center's part of a backup.
func DeleteDatacenter(ctx context.Context, s objstore.Store, cluster, backup, dc string) error {
	objs, err := s.List(ctx, BackupPrefix(cluster, backup)+dc+"/")
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := s.Delete(ctx, o.Key); err != nil {
			return err
		}
	}
	return nil
}

// Collect deletes the files no manifest names, older than grace (a
// backup in progress may have uploaded files it has not named yet). It
// returns the number deleted.
func Collect(ctx context.Context, s objstore.Store, cluster string, grace time.Duration, now time.Time) (int, error) {
	objs, err := s.List(ctx, path.Join(cluster, "backups")+"/")
	if err != nil {
		return 0, err
	}
	used := map[string]bool{}
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, ".json") || strings.HasSuffix(o.Key, "/index.json") {
			continue
		}
		var m Manifest
		if err := GetJSON(ctx, s, o.Key, &m); err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue
			}
			return 0, fmt.Errorf("reading %s: %w", o.Key, err)
		}
		for _, t := range m.Tables {
			for _, f := range t.Files {
				used[f.SHA256] = true
			}
		}
	}
	data, err := s.List(ctx, path.Join(cluster, "data")+"/")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range data {
		if used[path.Base(o.Key)] || now.Sub(o.LastModified) < grace {
			continue
		}
		if err := s.Delete(ctx, o.Key); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Backups lists the backups of a cluster with the data centers whose part
// is complete.
func Backups(ctx context.Context, s objstore.Store, cluster string) (map[string][]string, error) {
	objs, err := s.List(ctx, path.Join(cluster, "backups")+"/")
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, o := range objs {
		parts := strings.Split(strings.TrimPrefix(o.Key, path.Join(cluster, "backups")+"/"), "/")
		if len(parts) == 3 && parts[2] == "index.json" {
			out[parts[0]] = append(out[parts[0]], parts[1])
		}
	}
	for _, dcs := range out {
		sort.Strings(dcs)
	}
	return out, nil
}

// CommitLogPrefix holds a node's archived commit log.
func CommitLogPrefix(cluster, dc, pod string) string {
	return path.Join(cluster, "commitlog", dc, pod) + "/"
}

// CommitLogKey is an archived segment, uploaded in the round of time at.
func CommitLogKey(cluster, dc, pod string, at time.Time, segment string) string {
	return CommitLogPrefix(cluster, dc, pod) + fmt.Sprintf("%020d-%s", at.UnixNano(), segment)
}

// Segment is an archived commit log segment.
type Segment struct {
	Key  string
	Name string    // the segment's file name
	At   time.Time // its upload round
}

// CommitLog lists a node's archived segments, oldest round first.
func CommitLog(ctx context.Context, s objstore.Store, cluster, dc, pod string) ([]Segment, error) {
	objs, err := s.List(ctx, CommitLogPrefix(cluster, dc, pod))
	if err != nil {
		return nil, err
	}
	var out []Segment
	for _, o := range objs {
		ts, name, ok := strings.Cut(path.Base(o.Key), "-")
		if !ok {
			continue
		}
		var ns int64
		if _, err := fmt.Sscanf(ts, "%d", &ns); err != nil {
			continue
		}
		out = append(out, Segment{Key: o.Key, Name: name, At: time.Unix(0, ns)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// SegmentsFor are the segments to replay on a backup taken at from to
// reach until: those of the rounds from the backup on (a segment closed
// before the snapshot holds only writes the snapshot has), up to the
// first round at or after until. ok is false if no round reached until
// yet: writes up to until may not all be uploaded.
func SegmentsFor(segs []Segment, from, until time.Time) (out []Segment, ok bool) {
	var reached time.Time
	for _, sg := range segs {
		if !reached.IsZero() && sg.At.After(reached) {
			break
		}
		if sg.At.Before(from) {
			continue
		}
		out = append(out, sg)
		if reached.IsZero() && !sg.At.Before(until) {
			reached = sg.At
		}
	}
	return out, !reached.IsZero()
}

// PruneCommitLog deletes the archived segments of rounds before before (no
// backup kept is older), returning the number deleted.
func PruneCommitLog(ctx context.Context, s objstore.Store, cluster string, before time.Time) (int, error) {
	objs, err := s.List(ctx, path.Join(cluster, "commitlog")+"/")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range objs {
		ts, _, ok := strings.Cut(path.Base(o.Key), "-")
		var ns int64
		if !ok {
			continue
		}
		if _, err := fmt.Sscanf(ts, "%d", &ns); err != nil || !time.Unix(0, ns).Before(before) {
			continue
		}
		if err := s.Delete(ctx, o.Key); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// OldestBackup is when the oldest of a cluster's backups was taken (zero
// if there is none).
func OldestBackup(ctx context.Context, s objstore.Store, cluster string) (time.Time, error) {
	objs, err := s.List(ctx, path.Join(cluster, "backups")+"/")
	if err != nil {
		return time.Time{}, err
	}
	var oldest time.Time
	for _, o := range objs {
		if !strings.HasSuffix(o.Key, ".json") || strings.HasSuffix(o.Key, "/index.json") {
			continue
		}
		var m Manifest
		if err := GetJSON(ctx, s, o.Key, &m); err != nil {
			if errors.Is(err, objstore.ErrNotFound) {
				continue
			}
			return time.Time{}, err
		}
		at := m.SnapshotAt
		if at.IsZero() {
			at = m.CreatedAt
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	return oldest, nil
}

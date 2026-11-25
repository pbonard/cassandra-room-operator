// Package agent is the backup agent beside each node (Medusa's): it backs
// the node up to the object store (a snapshot through the management API,
// then the files no earlier backup uploaded), uploads its archived commit
// log, and restores it (downloads a backup's files, then imports them, or
// loads another node's through it; then replays the commit log to a point
// in time).
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbonard/cassandra-room-operator/operator/internal/catalog"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
)

// Job phases.
const (
	Running     = "Running"
	Downloaded  = "Downloaded"
	Importing   = "Importing"
	Succeeded   = "Succeeded"
	Failed      = "Failed"
	Downloading = "Downloading"
)

// Status is a backup's or restore's progress on the node.
type Status struct {
	Phase       string `json:"phase"`
	HostID      string `json:"hostId,omitempty"`
	Files       int    `json:"files"`
	Bytes       int64  `json:"bytes"`
	Transferred int64  `json:"transferred"`
	Error       string `json:"error,omitempty"`
}

// BackupRequest asks for the node's backup.
type BackupRequest struct {
	Cluster    string   `json:"cluster"`
	Datacenter string   `json:"datacenter"`
	Backup     string   `json:"backup"`
	Pod        string   `json:"pod"`
	Keyspaces  []string `json:"keyspaces,omitempty"`
}

// RestoreRequest asks to download a node's part of a backup (the manifest
// of Pod in Datacenter).
type RestoreRequest struct {
	Cluster    string   `json:"cluster"`
	Datacenter string   `json:"datacenter"`
	Backup     string   `json:"backup"`
	Pod        string   `json:"pod"`
	Keyspaces  []string `json:"keyspaces,omitempty"`
	// Load writes the files of the Sources' manifests (pods of Datacenter in
	// Cluster, the backup's) through the node to their replicas, instead of
	// importing the node's own: a restore into another topology or cluster.
	Load        bool     `json:"load,omitempty"`
	Sources     []string `json:"sources,omitempty"`
	Consistency string   `json:"consistency,omitempty"`
	// PointInTime, after the import, replays the node's archived commit log
	// from the backup up to that time.
	PointInTime *time.Time `json:"pointInTime,omitempty"`
}

// Agent runs a node's backups and restores.
type Agent struct {
	Store       objstore.Store
	Mgmt        mgmt.Client
	MgmtAddr    string
	DataDir     string
	Concurrency int
	// The node's place, for its commit log's objects.
	Cluster, Datacenter, Pod string

	mu       sync.Mutex
	clMu     sync.Mutex // one commit log upload round at a time
	backups  map[string]*Status
	restores map[string]*restore
}

type restore struct {
	status Status
	req    RestoreRequest
	files  map[[2]string][]string // keyspace, table -> local files
	// For a point in time: the downloaded segments and the tables they name.
	segments []string
	storage  map[string]string // storage id -> keyspace.table
}

// StartBackup starts the node's backup (or reports the one started).
func (a *Agent) StartBackup(req BackupRequest) Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.backups == nil {
		a.backups = map[string]*Status{}
	}
	if st, ok := a.backups[req.Backup]; ok {
		return *st
	}
	st := &Status{Phase: Running}
	a.backups[req.Backup] = st
	go a.backup(req, st)
	return *st
}

// BackupStatus reports a backup's progress; ok is false if this agent
// has none of that name (it restarted).
func (a *Agent) BackupStatus(backup string) (Status, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	st, ok := a.backups[backup]
	if !ok {
		return Status{}, false
	}
	return *st, true
}

func (a *Agent) update(st *Status, f func(*Status)) {
	a.mu.Lock()
	f(st)
	a.mu.Unlock()
}

func (a *Agent) backup(req BackupRequest, st *Status) {
	ctx := context.Background()
	err := a.runBackup(ctx, req, st)
	a.update(st, func(s *Status) {
		if err != nil {
			s.Phase, s.Error = Failed, err.Error()
			log.Printf("backup %s: %v", req.Backup, err)
		} else {
			s.Phase = Succeeded
		}
	})
}

func (a *Agent) runBackup(ctx context.Context, req BackupRequest, st *Status) error {
	node, err := a.Mgmt.Node(ctx, a.MgmtAddr)
	if err != nil {
		return fmt.Errorf("asking the node: %w", err)
	}
	a.update(st, func(s *Status) { s.HostID = node.HostID })
	snapshot := "backup-" + req.Backup
	snapshotAt := time.Now().UTC()
	tables, err := a.Mgmt.Snapshot(ctx, a.MgmtAddr, snapshot, req.Keyspaces)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	defer func() { _ = a.Mgmt.ClearSnapshot(context.Background(), a.MgmtAddr, snapshot) }()
	m := catalog.Manifest{Cluster: req.Cluster, Datacenter: req.Datacenter, Backup: req.Backup, Pod: req.Pod,
		HostID: node.HostID, Address: node.Address, CreatedAt: time.Now().UTC(), SnapshotAt: snapshotAt}
	type item struct {
		table, file int
		path        string
	}
	var items []item
	for ti, t := range tables {
		m.Tables = append(m.Tables, catalog.Table{Keyspace: t.Keyspace, Table: t.Table, ID: t.ID, Storage: t.Storage, Files: make([]catalog.File, len(t.Files))})
		for fi, p := range t.Files {
			items = append(items, item{ti, fi, p})
		}
	}
	a.update(st, func(s *Status) { s.Files = len(items) })
	err = parallel(len(items), a.concurrency(), func(i int) error {
		it := items[i]
		f, err := a.upload(ctx, req.Cluster, it.path, st)
		if err != nil {
			return err
		}
		a.mu.Lock()
		m.Tables[it.table].Files[it.file] = f
		a.mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	return catalog.PutJSON(ctx, a.Store, catalog.ManifestKey(req.Cluster, req.Backup, req.Datacenter, req.Pod), &m)
}

// upload stores a file under its content's hash unless it is there.
func (a *Agent) upload(ctx context.Context, cluster, p string, st *Status) (catalog.File, error) {
	sum, size, err := hashFile(p)
	if err != nil {
		return catalog.File{}, err
	}
	f := catalog.File{Name: filepath.Base(p), SHA256: sum, Size: size}
	a.update(st, func(s *Status) { s.Bytes += size })
	key := catalog.DataKey(cluster, sum)
	if o, err := a.Store.Stat(ctx, key); err == nil && o.Size == size {
		return f, nil // uploaded by an earlier backup
	} else if err != nil && !errors.Is(err, objstore.ErrNotFound) {
		return catalog.File{}, err
	}
	r, err := os.Open(p)
	if err != nil {
		return catalog.File{}, err
	}
	defer r.Close()
	if err := a.Store.Put(ctx, key, r, size); err != nil {
		return catalog.File{}, fmt.Errorf("uploading %s: %w", f.Name, err)
	}
	a.update(st, func(s *Status) { s.Transferred += size })
	return f, nil
}

func hashFile(p string) (string, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func (a *Agent) concurrency() int {
	if a.Concurrency > 0 {
		return a.Concurrency
	}
	return 4
}

// parallel runs f(0..n-1) on k workers; it returns the first error.
func parallel(n, k int, f func(int) error) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	next := 0
	for range min(k, max(n, 1)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next >= n || first != nil {
					mu.Unlock()
					return
				}
				i := next
				next++
				mu.Unlock()
				if err := f(i); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return first
}

func (a *Agent) restoreDir(backup string) string {
	return filepath.Join(a.DataDir, "restores", backup)
}

// StartRestore starts downloading a node's part of a backup (or reports
// the download started).
func (a *Agent) StartRestore(req RestoreRequest) Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.restores == nil {
		a.restores = map[string]*restore{}
	}
	if r, ok := a.restores[req.Backup]; ok {
		return r.status
	}
	r := &restore{status: Status{Phase: Downloading}, req: req}
	a.restores[req.Backup] = r
	go a.download(r)
	return r.status
}

// RestoreStatus reports a restore's progress.
func (a *Agent) RestoreStatus(backup string) (Status, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.restores[backup]
	if !ok {
		return Status{}, false
	}
	return r.status, true
}

func (a *Agent) download(r *restore) {
	ctx := context.Background()
	err := a.runDownload(ctx, r)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		r.status.Phase, r.status.Error = Failed, err.Error()
		log.Printf("restore %s: %v", r.req.Backup, err)
	} else {
		r.status.Phase = Downloaded
	}
}

func (a *Agent) runDownload(ctx context.Context, r *restore) error {
	req := r.req
	sources := []string{req.Pod}
	if req.Load {
		sources = req.Sources
	}
	dir := a.restoreDir(req.Backup)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	type item struct {
		key  [2]string
		f    catalog.File
		path string
	}
	var items []item
	files := map[[2]string][]string{}
	storage := map[string]string{}
	var snapshotAt time.Time
	for _, src := range sources {
		var m catalog.Manifest
		if err := catalog.GetJSON(ctx, a.Store, catalog.ManifestKey(req.Cluster, req.Backup, req.Datacenter, src), &m); err != nil {
			return fmt.Errorf("the manifest of %s: %w", src, err)
		}
		snapshotAt = m.SnapshotAt
		if snapshotAt.IsZero() {
			snapshotAt = m.CreatedAt
		}
		for _, t := range m.Tables {
			if len(req.Keyspaces) > 0 && !slices.Contains(req.Keyspaces, t.Keyspace) {
				continue
			}
			k := [2]string{t.Keyspace, t.Table}
			if files[k] == nil {
				files[k] = []string{}
			}
			if t.Storage != 0 {
				storage[strconv.FormatUint(t.Storage, 10)] = t.Keyspace + "." + t.Table
			}
			for _, f := range t.Files {
				p := filepath.Join(dir, src, t.Keyspace, t.Table, f.Name)
				items = append(items, item{k, f, p})
				files[k] = append(files[k], p)
			}
		}
	}
	var segments []catalog.Segment
	if req.PointInTime != nil {
		if req.Load {
			return errors.New("a point in time needs the node's own backup")
		}
		if len(storage) == 0 && len(files) > 0 {
			return errors.New("the backup predates commit log archiving (no storage ids)")
		}
		// Upload what the node has not yet, then pick the rounds.
		if err := a.ArchiveCommitLog(ctx); err != nil {
			return fmt.Errorf("archiving the commit log: %w", err)
		}
		all, err := catalog.CommitLog(ctx, a.Store, req.Cluster, req.Datacenter, req.Pod)
		if err != nil {
			return err
		}
		var ok bool
		segments, ok = catalog.SegmentsFor(all, snapshotAt, *req.PointInTime)
		if !ok {
			return fmt.Errorf("the archived commit log does not reach %s", req.PointInTime.UTC().Format(time.RFC3339))
		}
	}
	a.mu.Lock()
	r.status.Files = len(items) + len(segments)
	for _, it := range items {
		r.status.Bytes += it.f.Size
	}
	a.mu.Unlock()
	err := parallel(len(items), a.concurrency(), func(i int) error {
		it := items[i]
		if err := os.MkdirAll(filepath.Dir(it.path), 0o755); err != nil {
			return err
		}
		if err := a.fetch(ctx, req.Cluster, it.f, it.path); err != nil {
			return err
		}
		a.mu.Lock()
		r.status.Transferred += it.f.Size
		a.mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	var segFiles []string
	for _, sg := range segments {
		// The segment keeps its name: its number is part of its records.
		p := filepath.Join(dir, "commitlog", fmt.Sprintf("%06d", len(segFiles)), sg.Name)
		n, err := a.fetchObject(ctx, sg.Key, p)
		if err != nil {
			return err
		}
		a.mu.Lock()
		r.status.Bytes += n
		r.status.Transferred += n
		a.mu.Unlock()
		segFiles = append(segFiles, p)
	}
	a.mu.Lock()
	r.files, r.segments, r.storage = files, segFiles, storage
	a.mu.Unlock()
	return nil
}

// fetchObject downloads an object to a file.
func (a *Agent) fetchObject(ctx context.Context, key, p string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return 0, err
	}
	rc, err := a.Store.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("downloading %s: %w", key, err)
	}
	defer rc.Close()
	out, err := os.Create(p)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(out, rc)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return n, err
}

// ArchiveCommitLog runs an upload round of the node's commit log: the
// node closes its segment, then every archived segment is uploaded and
// removed from the node.
func (a *Agent) ArchiveCommitLog(ctx context.Context) error {
	a.clMu.Lock()
	defer a.clMu.Unlock()
	at := time.Now()
	files, err := a.Mgmt.ArchiveCommitLog(ctx, a.MgmtAddr)
	if err != nil {
		return err
	}
	for _, f := range files {
		local := f
		if !filepath.IsAbs(local) {
			local = filepath.Join(a.DataDir, f)
		}
		st, err := os.Stat(local)
		if err != nil {
			return err
		}
		in, err := os.Open(local)
		if err != nil {
			return err
		}
		err = a.Store.Put(ctx, catalog.CommitLogKey(a.Cluster, a.Datacenter, a.Pod, at, filepath.Base(f)), in, st.Size())
		in.Close()
		if err != nil {
			return fmt.Errorf("uploading %s: %w", f, err)
		}
		if err := os.Remove(local); err != nil {
			return err
		}
	}
	return nil
}

// ArchiveCommitLogEvery runs upload rounds until ctx is done.
func (a *Agent) ArchiveCommitLogEvery(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.ArchiveCommitLog(ctx); err != nil {
				log.Printf("commit log: %v", err)
			}
		}
	}
}

// fetch downloads a file and checks its content.
func (a *Agent) fetch(ctx context.Context, cluster string, f catalog.File, p string) error {
	rc, err := a.Store.Get(ctx, catalog.DataKey(cluster, f.SHA256))
	if err != nil {
		return fmt.Errorf("downloading %s: %w", f.Name, err)
	}
	defer rc.Close()
	out, err := os.Create(p)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(out, h), rc)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
		return fmt.Errorf("%s: content does not match its backup", f.Name)
	}
	return nil
}

// Import loads the downloaded files into the node's tables (they must
// have been truncated), then removes them.
func (a *Agent) Import(backup string) error {
	a.mu.Lock()
	r, ok := a.restores[backup]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("no restore %q", backup)
	}
	switch r.status.Phase {
	case Importing, Succeeded:
		a.mu.Unlock()
		return nil
	case Downloaded:
	default:
		a.mu.Unlock()
		return fmt.Errorf("restore %q is %s", backup, r.status.Phase)
	}
	r.status.Phase = Importing
	rr := *r
	a.mu.Unlock()
	go func() {
		err := a.runImport(&rr)
		a.mu.Lock()
		defer a.mu.Unlock()
		if err != nil {
			r.status.Phase, r.status.Error = Failed, err.Error()
			return
		}
		r.status.Phase = Succeeded
		_ = os.RemoveAll(a.restoreDir(backup))
	}()
	return nil
}

func (a *Agent) runImport(r *restore) error {
	ctx := context.Background()
	files := r.files
	keys := make([][2]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(x, y [2]string) int { return strings.Compare(x[0]+"."+x[1], y[0]+"."+y[1]) })
	for _, k := range keys {
		if len(files[k]) == 0 {
			continue
		}
		if r.req.Load {
			if _, err := a.Mgmt.Load(ctx, a.MgmtAddr, k[0], k[1], files[k], r.req.Consistency); err != nil {
				return fmt.Errorf("loading %s.%s: %w", k[0], k[1], err)
			}
			continue
		}
		if err := a.Mgmt.Import(ctx, a.MgmtAddr, k[0], k[1], files[k]); err != nil {
			return fmt.Errorf("importing %s.%s: %w", k[0], k[1], err)
		}
	}
	if r.req.PointInTime != nil && len(r.segments) > 0 {
		_, err := a.Mgmt.ReplayCommitLog(ctx, a.MgmtAddr, mgmt.ReplayRequest{Files: r.segments, Tables: r.storage,
			Until: r.req.PointInTime.UnixMicro()})
		if err != nil {
			return fmt.Errorf("replaying the commit log: %w", err)
		}
	}
	return nil
}

// Handler serves the agent's API:
//
//	POST /v1/backups                 BackupRequest -> Status
//	GET  /v1/backups/{backup}
//	POST /v1/restores                RestoreRequest -> Status (download)
//	GET  /v1/restores/{backup}
//	POST /v1/restores/{backup}/import
//	GET  /v1/health
func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/backups", func(w http.ResponseWriter, r *http.Request) {
		var req BackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Backup == "" || req.Cluster == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad backup request"})
			return
		}
		writeJSON(w, http.StatusAccepted, a.StartBackup(req))
	})
	mux.HandleFunc("GET /v1/backups/{backup}", func(w http.ResponseWriter, r *http.Request) {
		st, ok := a.BackupStatus(r.PathValue("backup"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such backup"})
			return
		}
		writeJSON(w, http.StatusOK, st)
	})
	mux.HandleFunc("POST /v1/restores", func(w http.ResponseWriter, r *http.Request) {
		var req RestoreRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Backup == "" || req.Pod == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad restore request"})
			return
		}
		writeJSON(w, http.StatusAccepted, a.StartRestore(req))
	})
	mux.HandleFunc("GET /v1/restores/{backup}", func(w http.ResponseWriter, r *http.Request) {
		st, ok := a.RestoreStatus(r.PathValue("backup"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such restore"})
			return
		}
		writeJSON(w, http.StatusOK, st)
	})
	mux.HandleFunc("POST /v1/restores/{backup}/import", func(w http.ResponseWriter, r *http.Request) {
		if err := a.Import(r.PathValue("backup")); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "importing"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

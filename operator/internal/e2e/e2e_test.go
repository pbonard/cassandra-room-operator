// Package e2e tests the operator's parts against a real cassandra-room
// process: the management API client, the superuser setup and truncation
// over CQL, and a backup and restore through the agent. It runs when
// CASSANDRA_ROOM_BIN names a cassandra-room binary.
package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/pbonard/cassandra-room-operator/operator/internal/agent"
	"github.com/pbonard/cassandra-room-operator/operator/internal/dc"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
	"github.com/pbonard/cassandra-room-operator/operator/internal/objstore"
)

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func session(t *testing.T, port int, user, pass string) (*gocql.Session, error) {
	c := gocql.NewCluster("127.0.0.1")
	c.Port = port
	c.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: pass}
	c.Timeout, c.ConnectTimeout = 10*time.Second, 5*time.Second
	c.DisableInitialHostLookup = true
	return c.CreateSession()
}

func ints(t *testing.T, s *gocql.Session) []int {
	iter := s.Query("SELECT k FROM ks.kv").Iter()
	var out []int
	var k int
	for iter.Scan(&k) {
		out = append(out, k)
	}
	require.NoError(t, iter.Close())
	sort.Ints(out)
	return out
}

func TestAgainstCassandraRoom(t *testing.T) {
	bin := os.Getenv("CASSANDRA_ROOM_BIN")
	if bin == "" {
		t.Skip("CASSANDRA_ROOM_BIN is not set")
	}
	ctx := context.Background()
	native, admin := freePort(t), freePort(t)
	dataDir := t.TempDir()
	// The flags file, as node-init writes it.
	flags := filepath.Join(t.TempDir(), "flags")
	require.NoError(t, os.WriteFile(flags, []byte(fmt.Sprintf("-addr=127.0.0.1:%d\n", native)), 0o644))
	cmd := exec.Command(bin, "-flags-file="+flags, "-data-dir="+dataDir, fmt.Sprintf("-admin-address=127.0.0.1:%d", admin),
		"-authenticator=PasswordAuthenticator", "-authorizer=CassandraAuthorizer", "-cluster-name=e2e")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })

	m := mgmt.New(nil)
	addr := fmt.Sprintf("127.0.0.1:%d", admin)
	require.Eventually(t, func() bool { ok, _ := m.Ready(ctx, addr); return ok }, 20*time.Second, 100*time.Millisecond)
	node, err := m.Node(ctx, addr)
	require.NoError(t, err)
	require.Equal(t, "NORMAL", node.Status)

	// The superuser, as the data center controller sets it up.
	conn := dc.Conn{Hosts: []string{"127.0.0.1"}, Port: native}
	roles := dc.CQLRoles{}
	require.NoError(t, roles.EnsureSuperuser(ctx, conn, "admin", "s3cret"))
	require.NoError(t, roles.EnsureSuperuser(ctx, conn, "admin", "s3cret"), "idempotent")
	_, err = session(t, native, "cassandra", "cassandra")
	require.Error(t, err, "the default superuser can no longer log in")
	s, err := session(t, native, "admin", "s3cret")
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Query("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	require.NoError(t, s.Query("CREATE TABLE ks.kv (k int PRIMARY KEY, v text)").Exec())
	for i := 0; i < 50; i++ {
		require.NoError(t, s.Query("INSERT INTO ks.kv (k, v) VALUES (?, 'x')", i).Exec())
	}
	ks, err := m.Keyspaces(ctx, addr)
	require.NoError(t, err)
	require.Equal(t, []string{"ks"}, ks)
	ring, err := m.Ring(ctx, addr, "ks")
	require.NoError(t, err)
	require.Len(t, ring, 1)

	// Backup through the agent.
	store := objstore.NewMemory()
	a := &agent.Agent{Store: store, Mgmt: m, MgmtAddr: addr, DataDir: dataDir}
	a.StartBackup(agent.BackupRequest{Cluster: "e2e", Datacenter: "datacenter1", Backup: "b1", Pod: "p0"})
	var st agent.Status
	require.Eventually(t, func() bool {
		st, _ = a.BackupStatus("b1")
		return st.Phase == agent.Succeeded || st.Phase == agent.Failed
	},
		20*time.Second, 50*time.Millisecond)
	require.Equal(t, agent.Succeeded, st.Phase, st.Error)
	require.Positive(t, st.Files)

	// Writes after the backup are lost by the restore.
	for i := 50; i < 60; i++ {
		require.NoError(t, s.Query("INSERT INTO ks.kv (k, v) VALUES (?, 'y')", i).Exec())
	}
	require.Len(t, ints(t, s), 60)

	// Restore: download, truncate (as the restore controller does), import.
	a.StartRestore(agent.RestoreRequest{Cluster: "e2e", Datacenter: "datacenter1", Backup: "b1", Pod: "p0"})
	require.Eventually(t, func() bool {
		st, _ = a.RestoreStatus("b1")
		return st.Phase == agent.Downloaded || st.Phase == agent.Failed
	},
		20*time.Second, 50*time.Millisecond)
	require.Equal(t, agent.Downloaded, st.Phase, st.Error)
	require.NoError(t, roles.Truncate(ctx, conn, "admin", "s3cret", []string{"ks.kv"}))
	require.Empty(t, ints(t, s))
	require.NoError(t, a.Import("b1"))
	require.Eventually(t, func() bool {
		st, _ = a.RestoreStatus("b1")
		return st.Phase == agent.Succeeded || st.Phase == agent.Failed
	},
		20*time.Second, 50*time.Millisecond)
	require.Equal(t, agent.Succeeded, st.Phase, st.Error)
	got := ints(t, s)
	require.Len(t, got, 50)
	require.Equal(t, 49, got[49])
}

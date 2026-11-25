package dc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/pbonard/cassandra-room-operator/operator/api/v1alpha1"
	"github.com/pbonard/cassandra-room-operator/operator/internal/mgmt"
)

// Conn is how to reach a data center's nodes over CQL.
type Conn struct {
	Hosts []string
	Port  int
	TLS   *tls.Config
}

// RoleManager sets up roles over CQL.
type RoleManager interface {
	// EnsureSuperuser creates the superuser role, logging in as the
	// default superuser (cassandra/cassandra), and disables the default
	// one's login. It does nothing once the superuser can log in.
	EnsureSuperuser(ctx context.Context, c Conn, user, password string) error
	// Truncate truncates tables ("keyspace.table") as the superuser.
	Truncate(ctx context.Context, c Conn, user, password string, tables []string) error
}

// CQLRoles is the RoleManager over gocql.
type CQLRoles struct{}

func session(c Conn, user, password string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(c.Hosts...)
	cluster.Port = c.Port
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: password}
	cluster.Timeout, cluster.ConnectTimeout = 10*time.Second, 5*time.Second
	cluster.DisableInitialHostLookup = true // the pods' addresses, not the advertised ones
	cluster.Consistency = gocql.Quorum
	if c.TLS != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: c.TLS}
	}
	return cluster.CreateSession()
}

func (CQLRoles) EnsureSuperuser(ctx context.Context, c Conn, user, password string) error {
	if len(c.Hosts) == 0 {
		return errors.New("no node to connect to")
	}
	if s, err := session(c, user, password); err == nil {
		s.Close()
		return nil // set up already
	}
	s, err := session(c, "cassandra", "cassandra")
	if err != nil {
		return fmt.Errorf("logging in as the default superuser: %w", err)
	}
	defer s.Close()
	// Role options take literals, not bind markers (as in Cassandra).
	if err := s.Query("CREATE ROLE IF NOT EXISTS " + quoteIdent(user) + " WITH PASSWORD = " + quoteString(password) +
		" AND SUPERUSER = true AND LOGIN = true").WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("creating the superuser: %w", err)
	}
	// The default superuser can no longer log in (its password, known to
	// everyone, is replaced too).
	s2, err := session(c, user, password)
	if err != nil {
		return fmt.Errorf("logging in as the new superuser: %w", err)
	}
	defer s2.Close()
	return s2.Query("ALTER ROLE cassandra WITH PASSWORD = " + quoteString(randomPassword()) + " AND LOGIN = false").WithContext(ctx).Exec()
}

func (CQLRoles) Truncate(ctx context.Context, c Conn, user, password string, tables []string) error {
	s, err := session(c, user, password)
	if err != nil {
		return err
	}
	defer s.Close()
	for _, t := range tables {
		ks, table, ok := strings.Cut(t, ".")
		if !ok {
			return fmt.Errorf("bad table %q", t)
		}
		if err := s.Query("TRUNCATE " + quoteIdent(ks) + "." + quoteIdent(table)).Consistency(gocql.All).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("truncating %s: %w", t, err)
		}
	}
	return nil
}

// SuperuserConn returns how to reach a data center's ready nodes over CQL
// as the superuser.
func SuperuserConn(ctx context.Context, c client.Client, dc *v1.CassandraRoomDatacenter, hosts []string) (Conn, string, string, error) {
	s := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: dc.Namespace, Name: superuserSecret(dc)}, s); err != nil {
		return Conn{}, "", "", err
	}
	cfg, err := clientTLS(ctx, c, dc)
	if err != nil {
		return Conn{}, "", "", err
	}
	return Conn{Hosts: hosts, Port: 9042, TLS: cfg}, string(s.Data["username"]), string(s.Data["password"]), nil
}

// quoteString is a CQL string literal.
func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func quoteIdent(s string) string {
	out := []byte{'"'}
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"')
		}
		out = append(out, s[i])
	}
	return string(append(out, '"'))
}

// secretTLS builds a client TLS configuration from a kubernetes.io/tls
// secret: its CA verifies the server (without checking its name: pods are
// reached by IP), its certificate is presented as the client's.
func secretTLS(ctx context.Context, c client.Client, namespace, name string) (*tls.Config, error) {
	s := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, s); err != nil {
		return nil, err
	}
	return TLSFromPEM(s.Data["ca.crt"], s.Data["tls.crt"], s.Data["tls.key"])
}

// TLSFromPEM builds a client TLS configuration: ca verifies the server's
// chain (not its name), cert and key (optional) authenticate the client.
func TLSFromPEM(ca, cert, key []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if len(ca) > 0 && !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("no CA certificate in ca.crt")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return errors.New("no server certificate")
		}
		inter := x509.NewCertPool()
		for _, c := range cs.PeerCertificates[1:] {
			inter.AddCert(c)
		}
		_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter})
		return err
	}
	if len(cert) > 0 && len(key) > 0 {
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// clientTLS is how the operator connects to the native port.
func clientTLS(ctx context.Context, c client.Client, dc *v1.CassandraRoomDatacenter) (*tls.Config, error) {
	if dc.Spec.TLS.Client == nil || dc.Spec.TLS.Client.Optional {
		return nil, nil
	}
	return secretTLS(ctx, c, dc.Namespace, dc.Spec.TLS.Client.SecretName)
}

// MgmtClients returns management API clients by data center: plain HTTP,
// or HTTPS with the management secret's CA.
func MgmtClients(c client.Client) func(ctx context.Context, dc *v1.CassandraRoomDatacenter) (mgmt.Client, error) {
	plain := mgmt.New(nil)
	return func(ctx context.Context, dc *v1.CassandraRoomDatacenter) (mgmt.Client, error) {
		if dc.Spec.TLS.Management == nil {
			return plain, nil
		}
		cfg, err := secretTLS(ctx, c, dc.Namespace, dc.Spec.TLS.Management.SecretName)
		if err != nil {
			return nil, err
		}
		return mgmt.New(cfg), nil
	}
}

func sortRequirements(reqs []corev1.NodeSelectorRequirement) {
	sort.Slice(reqs, func(i, j int) bool { return reqs[i].Key < reqs[j].Key })
}

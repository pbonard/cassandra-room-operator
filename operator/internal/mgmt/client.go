// Package mgmt is a client of cassandra-room's management API.
package mgmt

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// NodeInfo describes a node (GET /api/v1/node, /api/v1/nodes).
type NodeInfo struct {
	HostID       string `json:"hostId"`
	Address      string `json:"address"`
	InternalAddr string `json:"internalAddress,omitempty"`
	NativeAddr   string `json:"nativeAddress"`
	NativePort   int    `json:"nativePort"`
	DataCenter   string `json:"datacenter"`
	Rack         string `json:"rack"`
	Status       string `json:"status"`
	Up           bool   `json:"up"`
	Tokens       int    `json:"tokens"`
	Ordinal      int    `json:"ordinal,omitempty"`
	CMS          bool   `json:"cms,omitempty"`
	Self         bool   `json:"self,omitempty"`
}

// Range is a range (Start, End] of the ring and its replicas.
type Range struct {
	Start    int64    `json:"start,string"`
	End      int64    `json:"end,string"`
	Replicas []string `json:"replicas"`
}

// RepairRequest asks for a repair of a range of a keyspace.
type RepairRequest struct {
	Keyspace string `json:"keyspace"`
	Table    string `json:"table,omitempty"`
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
}

// RepairStats are a repair's results.
type RepairStats struct {
	Ranges     int   `json:"ranges"`
	Mismatches int   `json:"mismatches"`
	Streamed   int64 `json:"streamed"`
}

// SnapshotTable is a table's part of a snapshot.
type SnapshotTable struct {
	Keyspace string   `json:"keyspace"`
	Table    string   `json:"table"`
	ID       string   `json:"id"`
	Files    []string `json:"files"`
}

// Client is the management API of nodes, addressed as "host:port".
type Client interface {
	Ready(ctx context.Context, addr string) (bool, error)
	Node(ctx context.Context, addr string) (NodeInfo, error)
	Nodes(ctx context.Context, addr string) ([]NodeInfo, error)
	Ring(ctx context.Context, addr, keyspace string) ([]Range, error)
	Keyspaces(ctx context.Context, addr string) ([]string, error)
	Flush(ctx context.Context, addr string) error
	Decommission(ctx context.Context, addr string) error
	Cleanup(ctx context.Context, addr, keyspace string) error
	RemoveNode(ctx context.Context, addr, node string) error
	SetCMS(ctx context.Context, addr string, members []string) error
	Repair(ctx context.Context, addr string, req RepairRequest) (RepairStats, error)
	Snapshot(ctx context.Context, addr, name string, keyspaces []string) ([]SnapshotTable, error)
	ClearSnapshot(ctx context.Context, addr, name string) error
	Import(ctx context.Context, addr, keyspace, table string, files []string) error
}

// HTTPClient is the Client over HTTP(S).
type HTTPClient struct {
	HTTP *http.Client
	// TLS, if set, uses HTTPS with this configuration.
	TLS *tls.Config
}

// New returns a client; tlsConfig nil uses plain HTTP.
func New(tlsConfig *tls.Config) *HTTPClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsConfig
	return &HTTPClient{HTTP: &http.Client{Transport: tr, Timeout: 0}, TLS: tlsConfig}
}

// Error is an error the API answered.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("management API: %d: %s", e.Status, e.Message) }

func (c *HTTPClient) do(ctx context.Context, method, addr, path string, body, out any) error {
	scheme := "http"
	if c.TLS != nil {
		scheme = "https"
	}
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, scheme+"://"+addr+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		if json.Unmarshal(data, &e) != nil || e.Error == "" {
			e.Error = string(data)
		}
		return &Error{Status: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *HTTPClient) Ready(ctx context.Context, addr string) (bool, error) {
	err := c.do(ctx, "GET", addr, "/api/v1/ready", nil, nil)
	var e *Error
	if errors.As(err, &e) && e.Status == http.StatusServiceUnavailable {
		return false, nil
	}
	return err == nil, err
}

func (c *HTTPClient) Node(ctx context.Context, addr string) (n NodeInfo, err error) {
	err = c.do(ctx, "GET", addr, "/api/v1/node", nil, &n)
	return
}

func (c *HTTPClient) Nodes(ctx context.Context, addr string) (n []NodeInfo, err error) {
	err = c.do(ctx, "GET", addr, "/api/v1/nodes", nil, &n)
	return
}

func (c *HTTPClient) Ring(ctx context.Context, addr, keyspace string) (r []Range, err error) {
	err = c.do(ctx, "GET", addr, "/api/v1/ring?keyspace="+urlQuery(keyspace), nil, &r)
	return
}

func (c *HTTPClient) Keyspaces(ctx context.Context, addr string) (k []string, err error) {
	err = c.do(ctx, "GET", addr, "/api/v1/keyspaces", nil, &k)
	return
}

func (c *HTTPClient) Flush(ctx context.Context, addr string) error {
	return c.do(ctx, "POST", addr, "/api/v1/flush", nil, nil)
}

func (c *HTTPClient) Decommission(ctx context.Context, addr string) error {
	return c.do(ctx, "POST", addr, "/api/v1/decommission", nil, nil)
}

func (c *HTTPClient) Cleanup(ctx context.Context, addr, keyspace string) error {
	return c.do(ctx, "POST", addr, "/api/v1/cleanup?keyspace="+urlQuery(keyspace), nil, nil)
}

func (c *HTTPClient) RemoveNode(ctx context.Context, addr, node string) error {
	return c.do(ctx, "POST", addr, "/api/v1/removenode?node="+urlQuery(node), nil, nil)
}

func (c *HTTPClient) SetCMS(ctx context.Context, addr string, members []string) error {
	q := ""
	for i, m := range members {
		if i > 0 {
			q += ","
		}
		q += m
	}
	return c.do(ctx, "POST", addr, "/api/v1/cms?members="+urlQuery(q), nil, nil)
}

func (c *HTTPClient) Repair(ctx context.Context, addr string, req RepairRequest) (st RepairStats, err error) {
	err = c.do(ctx, "POST", addr, "/api/v1/repair", req, &st)
	return
}

func (c *HTTPClient) Snapshot(ctx context.Context, addr, name string, keyspaces []string) (t []SnapshotTable, err error) {
	err = c.do(ctx, "POST", addr, "/api/v1/snapshots", map[string]any{"name": name, "keyspaces": keyspaces}, &t)
	return
}

func (c *HTTPClient) ClearSnapshot(ctx context.Context, addr, name string) error {
	return c.do(ctx, "DELETE", addr, "/api/v1/snapshots/"+urlPath(name), nil, nil)
}

func (c *HTTPClient) Import(ctx context.Context, addr, keyspace, table string, files []string) error {
	return c.do(ctx, "POST", addr, "/api/v1/import", map[string]any{"keyspace": keyspace, "table": table, "files": files}, nil)
}

// Token renders a token for a repair request.
func Token(t int64) string { return strconv.FormatInt(t, 10) }

func urlQuery(s string) string { return url.QueryEscape(s) }
func urlPath(s string) string  { return url.PathEscape(s) }

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ErrNoJob is returned for a job the agent does not know (it restarted).
var ErrNoJob = errors.New("agent: no such job")

// Client talks to nodes' agents, addressed as "host:port".
type Client interface {
	StartBackup(ctx context.Context, addr string, req BackupRequest) (Status, error)
	BackupStatus(ctx context.Context, addr, backup string) (Status, error)
	StartRestore(ctx context.Context, addr string, req RestoreRequest) (Status, error)
	RestoreStatus(ctx context.Context, addr, backup string) (Status, error)
	Import(ctx context.Context, addr, backup string) error
}

// HTTPClient is the Client over HTTP.
type HTTPClient struct{ HTTP *http.Client }

func (c *HTTPClient) do(ctx context.Context, method, addr, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, r)
	if err != nil {
		return err
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound {
		return ErrNoJob
	}
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		_ = json.Unmarshal(data, &e)
		return fmt.Errorf("agent %s: %d: %s", addr, resp.StatusCode, e.Error)
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (c *HTTPClient) StartBackup(ctx context.Context, addr string, req BackupRequest) (st Status, err error) {
	err = c.do(ctx, "POST", addr, "/v1/backups", req, &st)
	return
}

func (c *HTTPClient) BackupStatus(ctx context.Context, addr, backup string) (st Status, err error) {
	err = c.do(ctx, "GET", addr, "/v1/backups/"+url.PathEscape(backup), nil, &st)
	return
}

func (c *HTTPClient) StartRestore(ctx context.Context, addr string, req RestoreRequest) (st Status, err error) {
	err = c.do(ctx, "POST", addr, "/v1/restores", req, &st)
	return
}

func (c *HTTPClient) RestoreStatus(ctx context.Context, addr, backup string) (st Status, err error) {
	err = c.do(ctx, "GET", addr, "/v1/restores/"+url.PathEscape(backup), nil, &st)
	return
}

func (c *HTTPClient) Import(ctx context.Context, addr, backup string) error {
	return c.do(ctx, "POST", addr, "/v1/restores/"+url.PathEscape(backup)+"/import", nil, nil)
}

// Local is a Client of agents in this process, by address (tests).
type Local struct {
	mu     sync.Mutex
	agents map[string]*Agent
}

// Set registers the agent at addr.
func (l *Local) Set(addr string, a *Agent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.agents == nil {
		l.agents = map[string]*Agent{}
	}
	l.agents[addr] = a
}

func (l *Local) get(addr string) (*Agent, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.agents[addr]
	if a == nil {
		return nil, fmt.Errorf("agent %s: connection refused", addr)
	}
	return a, nil
}

func (l *Local) StartBackup(ctx context.Context, addr string, req BackupRequest) (Status, error) {
	a, err := l.get(addr)
	if err != nil {
		return Status{}, err
	}
	return a.StartBackup(req), nil
}

func (l *Local) BackupStatus(ctx context.Context, addr, backup string) (Status, error) {
	a, err := l.get(addr)
	if err != nil {
		return Status{}, err
	}
	st, ok := a.BackupStatus(backup)
	if !ok {
		return Status{}, ErrNoJob
	}
	return st, nil
}

func (l *Local) StartRestore(ctx context.Context, addr string, req RestoreRequest) (Status, error) {
	a, err := l.get(addr)
	if err != nil {
		return Status{}, err
	}
	return a.StartRestore(req), nil
}

func (l *Local) RestoreStatus(ctx context.Context, addr, backup string) (Status, error) {
	a, err := l.get(addr)
	if err != nil {
		return Status{}, err
	}
	st, ok := a.RestoreStatus(backup)
	if !ok {
		return Status{}, ErrNoJob
	}
	return st, nil
}

func (l *Local) Import(ctx context.Context, addr, backup string) error {
	a, err := l.get(addr)
	if err != nil {
		return err
	}
	return a.Import(backup)
}

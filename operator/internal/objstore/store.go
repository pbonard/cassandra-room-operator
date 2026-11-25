// Package objstore is the object store backups go to: S3 (or a compatible
// store), or memory for tests.
package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNotFound is returned for a missing object.
var ErrNotFound = errors.New("objstore: object not found")

// Object describes a stored object.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is an object store.
type Store interface {
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Stat returns ErrNotFound for a missing object.
	Stat(ctx context.Context, key string) (Object, error)
	// List returns the objects whose key starts with prefix, sorted.
	List(ctx context.Context, prefix string) ([]Object, error)
	Delete(ctx context.Context, key string) error
}

// Memory is a Store in memory.
type Memory struct {
	mu      sync.Mutex
	objects map[string][]byte
	times   map[string]time.Time
	// Puts counts the objects written.
	Puts int
}

func NewMemory() *Memory {
	return &Memory{objects: map[string][]byte{}, times: map[string]time.Time{}}
}

func (m *Memory) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key], m.times[key] = b, time.Now()
	m.Puts++
	return nil
}

func (m *Memory) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *Memory) Stat(ctx context.Context, key string) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	return Object{Key: key, Size: int64(len(b)), LastModified: m.times[key]}, nil
}

func (m *Memory) List(ctx context.Context, prefix string) ([]Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Object
	for k, b := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, Object{Key: k, Size: int64(len(b)), LastModified: m.times[k]})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *Memory) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	delete(m.times, key)
	return nil
}

// Age sets an object's modification time (tests).
func (m *Memory) Age(key string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.times[key]; ok {
		m.times[key] = m.times[key].Add(-d)
	}
}

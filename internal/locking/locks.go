package locking

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Manager struct {
	mu    sync.Mutex
	locks map[string]entry // path -> entry
}

func NewManager() *Manager {
	return &Manager{locks: make(map[string]entry)}
}

var ErrLocked = errors.New("locked by another holder")

type entry struct {
	holder string
	since  time.Time
}

func (m *Manager) TryLock(path, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.locks[path]; ok && e.holder != holder {
		return ErrLocked
	}
	if _, ok := m.locks[path]; !ok {
		m.locks[path] = entry{holder: holder, since: time.Now()}
	} else {
		// already held by same holder; keep original since
	}
	return nil
}

func (m *Manager) Unlock(path, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.locks[path]; ok && e.holder == holder {
		delete(m.locks, path)
	}
}

func (m *Manager) Holder(path string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.locks[path]
	if !ok {
		return "", false
	}
	return e.holder, true
}

type Info struct {
	Path   string    `json:"path"`
	Holder string    `json:"holder"`
	Since  time.Time `json:"since"`
}

// List returns all current locks.
func (m *Manager) List() []Info {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Info, 0, len(m.locks))
	for p, e := range m.locks {
		out = append(out, Info{Path: p, Holder: e.holder, Since: e.since})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// ReleaseByHolder releases all locks held by the specified holder.
// Returns the number of locks released.
func (m *Manager) ReleaseByHolder(holder string) int {
	if holder == "" {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for p, e := range m.locks {
		if e.holder == holder {
			delete(m.locks, p)
			n++
		}
	}
	return n
}

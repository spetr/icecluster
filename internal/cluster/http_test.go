package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spetr/icecluster/internal/locking"
)

type fakeHooks struct {
	deny  map[string]bool
	patch map[string]map[string]any
}

func (f *fakeHooks) Fire(ctx context.Context, event string, payload map[string]any) {}
func (f *fakeHooks) Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string) {
	if f.deny != nil && f.deny[event] {
		return false, nil, "denied"
	}
	if f.patch != nil {
		if p, ok := f.patch[event]; ok {
			return true, p, ""
		}
	}
	return true, nil, ""
}

type noopSvc struct{}

func (noopSvc) ApplyPut(path string, r io.Reader) error { return nil }
func (noopSvc) ApplyDelete(path string) error           { return nil }

type fakeLocker struct{ unlocked [][2]string }

func (fakeLocker) TryLock(path, holder string) error { return nil }
func (l *fakeLocker) Unlock(path, holder string) {
	l.unlocked = append(l.unlocked, [2]string{path, holder})
}
func (fakeLocker) List() []locking.Info { return nil }

func TestHandleFilePutDeniedByHook(t *testing.T) {
	dir := t.TempDir()
	h := NewHTTPServer(noopSvc{}, dir, NewPeers("http://self"), &fakeLocker{})
	h.Hooks = &fakeHooks{deny: map[string]bool{"file_put": true}}
	r := httptest.NewRequest(http.MethodPut, "/v1/file?path="+urlQuery("/a.txt"), nil)
	w := httptest.NewRecorder()
	h.Mux.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestHandleFilePutPatchedPath(t *testing.T) {
	dir := t.TempDir()
	h := NewHTTPServer(noopSvc{}, dir, NewPeers("http://self"), &fakeLocker{})
	h.Hooks = &fakeHooks{patch: map[string]map[string]any{"file_put": {"path": "b.txt"}}}
	r := httptest.NewRequest(http.MethodPut, "/v1/file?path="+urlQuery("/a.txt"), strings.NewReader("x"))
	w := httptest.NewRecorder()
	h.Mux.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if _, err := os.Stat(filepath.Join(dir, "b.txt")); err != nil {
		t.Fatalf("expected b.txt, err=%v", err)
	}
}

func TestLockGrantDeniedByHook(t *testing.T) {
	h := NewHTTPServer(noopSvc{}, t.TempDir(), NewPeers("http://self"), &fakeLocker{})
	h.Hooks = &fakeHooks{deny: map[string]bool{"lock_grant": true}}
	r := httptest.NewRequest(http.MethodPost, "/v1/lock?path="+urlQuery("/a")+"&holder=h", nil)
	w := httptest.NewRecorder()
	h.Mux.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

// helper
func urlQuery(s string) string { return (&url.URL{Path: s}).EscapedPath() }

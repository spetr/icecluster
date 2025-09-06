package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spetr/icecluster/internal/locking"
	"github.com/spetr/icecluster/internal/ui"
	"github.com/spetr/icecluster/internal/version"
)

type Node struct {
	ID      string `json:"id"`
	APIAddr string `json:"api_addr"`
}

type Service interface {
	ApplyPut(path string, r io.Reader) error
	ApplyDelete(path string) error
}

type HTTPServer struct {
	Mux    *http.ServeMux
	Svc    Service
	Root   string
	Peers  *Peers
	Locker Locker
	// ReadyFunc, if set, determines readiness. When nil, node is considered ready.
	ReadyFunc func() bool
	Token     string
	Hooks     interface {
		Fire(ctx context.Context, event string, payload map[string]any)
		Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string)
	}
	// NodeID is this node's identity (used by peers to release locks on down nodes)
	NodeID string
}

type Locker interface {
	TryLock(path, holder string) error
	Unlock(path, holder string)
	List() []locking.Info
}

func NewHTTPServer(svc Service, root string, peers *Peers, locker Locker) *HTTPServer {
	h := &HTTPServer{Mux: http.NewServeMux(), Svc: svc, Root: root, Peers: peers, Locker: locker}
	h.routes()
	return h
}

func (h *HTTPServer) routes() {
	h.Mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	h.Mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		ready := true
		if h.ReadyFunc != nil {
			ready = h.ReadyFunc()
		}
		if ready {
			w.WriteHeader(200)
			w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(503)
		w.Write([]byte("not ready"))
	})
	h.Mux.HandleFunc("/v1/time", func(w http.ResponseWriter, r *http.Request) {
		JSON(w, 200, map[string]any{"unix": time.Now().UnixNano()})
	})
	h.Mux.HandleFunc("/v1/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, version.Get())
	})
	h.Mux.HandleFunc("/v1/node", func(w http.ResponseWriter, r *http.Request) {
		JSON(w, 200, map[string]any{"id": h.NodeID})
	})
	h.Mux.HandleFunc("/v1/ready", func(w http.ResponseWriter, r *http.Request) {
		ready := true
		if h.ReadyFunc != nil {
			ready = h.ReadyFunc()
		}
		JSON(w, func() int {
			if ready {
				return 200
			}
			return 503
		}(), map[string]any{"ready": ready})
	})

	// secured routes
	h.Mux.HandleFunc("/v1/file", h.secure(h.handleFile))
	h.Mux.HandleFunc("/v1/index", h.secure(h.handleIndex))
	h.Mux.HandleFunc("/v1/peers", h.secure(h.handlePeers))
	h.Mux.HandleFunc("/v1/peers/register", h.secure(h.handleRegister))
	h.Mux.HandleFunc("/v1/peers/unregister", h.secure(h.handleUnregister))
	h.Mux.HandleFunc("/v1/lock", h.secure(h.handleLock))
	h.Mux.HandleFunc("/v1/unlock", h.secure(h.handleUnlock))
	h.Mux.HandleFunc("/v1/locks", h.secure(h.handleLocks))
	h.Mux.HandleFunc("/v1/stats", h.secure(h.handleStats))
	h.Mux.HandleFunc("/v1/stats/reset", h.secure(h.handleStatsReset))
	h.Mux.HandleFunc("/v1/locks/release_by_holder", h.secure(h.handleReleaseByHolder))

	// UI: served under /ui
	h.Mux.Handle("/ui", ui.Handler())
	h.Mux.Handle("/ui/", ui.Handler())
}

func (h *HTTPServer) handleFile(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := q.Get("path")
	if p == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("missing path"))
		return
	}
	// normalize to a relative path under Root; allow leading slash but strip it
	clean := filepath.Clean(p)
	clean = strings.TrimPrefix(clean, "/")
	// disallow parent traversal and empty/curdir
	if strings.Contains(clean, "..") || clean == "." || clean == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid path"))
		return
	}
	switch r.Method {
	case http.MethodPut:
		if h.Hooks != nil {
			allow, patch, reason := h.Hooks.Decide(r.Context(), "file_put", map[string]any{"path": clean})
			if !allow {
				http.Error(w, reason, http.StatusForbidden)
				return
			}
			if v, ok := patch["path"].(string); ok && v != "" {
				vClean := filepath.Clean(v)
				vClean = strings.TrimPrefix(vClean, "/")
				if strings.Contains(vClean, "..") || vClean == "." || vClean == "" {
					http.Error(w, "invalid patched path", http.StatusBadRequest)
					return
				}
				clean = vClean
			}
		}
		// write locally only to avoid replication loops, atomically
		if err := writeLocal(filepath.Join(h.Root, clean), r.Body); err != nil {
			log.Printf("write %s: %v", clean, err)
			w.WriteHeader(500)
			return
		}
		if h.Hooks != nil {
			h.Hooks.Fire(r.Context(), "file_put", map[string]any{"path": clean})
		}
		w.WriteHeader(204)
	case http.MethodDelete:
		if h.Hooks != nil {
			allow, patch, reason := h.Hooks.Decide(r.Context(), "file_delete", map[string]any{"path": clean})
			if !allow {
				http.Error(w, reason, http.StatusForbidden)
				return
			}
			if v, ok := patch["path"].(string); ok && v != "" {
				vClean := filepath.Clean(v)
				vClean = strings.TrimPrefix(vClean, "/")
				if strings.Contains(vClean, "..") || vClean == "." || vClean == "" {
					http.Error(w, "invalid patched path", http.StatusBadRequest)
					return
				}
				clean = vClean
			}
		}
		// delete locally only
		if err := os.Remove(filepath.Join(h.Root, clean)); err != nil && !os.IsNotExist(err) {
			log.Printf("delete %s: %v", clean, err)
			w.WriteHeader(500)
			return
		}
		if h.Hooks != nil {
			h.Hooks.Fire(r.Context(), "file_delete", map[string]any{"path": clean})
		}
		w.WriteHeader(204)
	case http.MethodGet:
		if h.Hooks != nil {
			if allow, _, reason := h.Hooks.Decide(r.Context(), "file_read", map[string]any{"path": clean}); !allow {
				http.Error(w, reason, http.StatusForbidden)
				return
			}
		}
		f, err := os.Open(filepath.Join(h.Root, clean))
		if err != nil {
			w.WriteHeader(404)
			return
		}
		defer f.Close()
		if h.Hooks != nil {
			h.Hooks.Fire(r.Context(), "file_read", map[string]any{"path": clean})
		}
		io.Copy(w, f)
	default:
		w.WriteHeader(405)
	}
}

func (h *HTTPServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	if h.Peers == nil {
		JSON(w, 200, []string{})
		return
	}
	JSON(w, 200, h.Peers.List())
}

func (h *HTTPServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	if h.Peers == nil {
		w.WriteHeader(200)
		return
	}
	u := r.URL.Query().Get("url")
	if u == "" {
		w.WriteHeader(400)
		w.Write([]byte("missing url"))
		return
	}
	if h.Hooks != nil {
		if allow, _, reason := h.Hooks.Decide(r.Context(), "peer_join", map[string]any{"peer": u}); !allow {
			http.Error(w, reason, http.StatusForbidden)
			return
		}
	}
	isNew := h.Peers.Add(u)
	// respond with our current peers so the joiner can sync
	JSON(w, 200, h.Peers.List())
	// notify others about the new peer
	if isNew {
		if h.Hooks != nil {
			h.Hooks.Fire(r.Context(), "peer_join", map[string]any{"peer": u})
		}
		joiner := u
		h.Peers.ForEach(func(peer string) {
			if peer == joiner {
				return
			}
			// best-effort registration fan-out
			req, _ := http.NewRequest(http.MethodPost, peer+"/v1/peers/register?url="+url.QueryEscape(joiner), nil)
			if h.Token != "" {
				req.Header.Set("Authorization", "Bearer "+h.Token)
			}
			_, _ = http.DefaultClient.Do(req)
		})
	}
}

func (h *HTTPServer) handleUnregister(w http.ResponseWriter, r *http.Request) {
	if h.Peers == nil {
		w.WriteHeader(200)
		return
	}
	u := r.URL.Query().Get("url")
	if u == "" {
		w.WriteHeader(400)
		w.Write([]byte("missing url"))
		return
	}
	if h.Hooks != nil {
		if allow, _, reason := h.Hooks.Decide(r.Context(), "peer_leave", map[string]any{"peer": u}); !allow {
			http.Error(w, reason, http.StatusForbidden)
			return
		}
	}
	h.Peers.Remove(u)
	if h.Hooks != nil {
		h.Hooks.Fire(r.Context(), "peer_leave", map[string]any{"peer": u})
	}
	w.WriteHeader(204)
}

func (h *HTTPServer) handleLock(w http.ResponseWriter, r *http.Request) {
	if h.Locker == nil {
		w.WriteHeader(503)
		return
	}
	t0 := time.Now()
	path := r.URL.Query().Get("path")
	holder := r.URL.Query().Get("holder")
	if path == "" || holder == "" {
		w.WriteHeader(400)
		return
	}
	if err := h.Locker.TryLock(filepath.Clean(path), holder); err != nil {
		log.Printf("lock: deny path=%s holder=%s after %s err=%v", path, holder, time.Since(t0), err)
		if h.Hooks != nil {
			h.Hooks.Fire(r.Context(), "lock_deny", map[string]any{"path": path, "holder": holder, "error": err.Error()})
		}
		w.WriteHeader(409)
		w.Write([]byte(err.Error()))
		return
	}
	if h.Hooks != nil {
		if allow, _, reason := h.Hooks.Decide(r.Context(), "lock_grant", map[string]any{"path": path, "holder": holder}); !allow {
			// revert lock
			h.Locker.Unlock(filepath.Clean(path), holder)
			http.Error(w, reason, http.StatusForbidden)
			return
		}
	}
	log.Printf("lock: grant path=%s holder=%s in %s", path, holder, time.Since(t0))
	if r.URL.Query().Get("nobroadcast") != "1" && h.Peers != nil && h.Peers.Coordinator() == h.Peers.Self() {
		h.broadcastLock("lock", path, holder)
	}
	if h.Hooks != nil {
		h.Hooks.Fire(r.Context(), "lock_grant", map[string]any{"path": path, "holder": holder})
	}
	w.WriteHeader(204)
}

func (h *HTTPServer) handleUnlock(w http.ResponseWriter, r *http.Request) {
	if h.Locker == nil {
		w.WriteHeader(503)
		return
	}
	t0 := time.Now()
	path := r.URL.Query().Get("path")
	holder := r.URL.Query().Get("holder")
	if path == "" || holder == "" {
		w.WriteHeader(400)
		return
	}
	if h.Hooks != nil {
		if allow, _, reason := h.Hooks.Decide(r.Context(), "lock_release", map[string]any{"path": path, "holder": holder}); !allow {
			http.Error(w, reason, http.StatusForbidden)
			return
		}
	}
	h.Locker.Unlock(filepath.Clean(path), holder)
	log.Printf("unlock: path=%s holder=%s in %s", path, holder, time.Since(t0))
	if r.URL.Query().Get("nobroadcast") != "1" && h.Peers != nil && h.Peers.Coordinator() == h.Peers.Self() {
		h.broadcastLock("unlock", path, holder)
	}
	if h.Hooks != nil {
		h.Hooks.Fire(r.Context(), "lock_release", map[string]any{"path": path, "holder": holder})
	}
	w.WriteHeader(204)
}

func (h *HTTPServer) handleLocks(w http.ResponseWriter, r *http.Request) {
	if h.Locker == nil {
		w.WriteHeader(503)
		return
	}
	JSON(w, 200, h.Locker.List())
}

func (h *HTTPServer) broadcastLock(op, path, holder string) {
	if h.Peers == nil {
		return
	}
	for _, peer := range h.Peers.List() {
		if peer == h.Peers.Self() {
			continue
		}
		req, _ := http.NewRequest(http.MethodPost, peer+"/v1/"+op+"?path="+url.QueryEscape(path)+"&holder="+url.QueryEscape(holder)+"&nobroadcast=1", nil)
		if h.Token != "" {
			req.Header.Set("Authorization", "Bearer "+h.Token)
		}
		_, _ = (&http.Client{Timeout: 5 * time.Second}).Do(req)
	}
}

// handleStats returns per-peer operation statistics.
func (h *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	JSON(w, 200, StatsSnapshot())
}

func (h *HTTPServer) handleStatsReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(405)
		return
	}
	StatsReset()
	w.WriteHeader(204)
}

// handleReleaseByHolder releases all locks held by the provided holder ID.
func (h *HTTPServer) handleReleaseByHolder(w http.ResponseWriter, r *http.Request) {
	holder := r.URL.Query().Get("holder")
	if holder == "" {
		w.WriteHeader(400)
		w.Write([]byte("missing holder"))
		return
	}
	type rel interface{ ReleaseByHolder(holder string) int }
	if m, ok := h.Locker.(rel); ok {
		n := m.ReleaseByHolder(holder)
		JSON(w, 200, map[string]any{"released": n})
		return
	}
	w.WriteHeader(501)
}

// secure wraps a handler to enforce bearer token when configured.
func (h *HTTPServer) secure(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h.Token != "" {
			hdr := r.Header.Get("Authorization")
			const pfx = "Bearer "
			if len(hdr) < len(pfx) || hdr[:len(pfx)] != pfx || hdr[len(pfx):] != h.Token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// handleIndex returns a recursive listing of files under Root with size and mtime ns
func (h *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	type fi struct {
		Path  string `json:"path"`
		Size  int64  `json:"size"`
		MTime int64  `json:"mtime"`
	}
	out := []fi{}
	err := filepath.Walk(h.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(h.Root, path)
		if err != nil {
			return nil
		}
		out = append(out, fi{Path: rel, Size: info.Size(), MTime: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		w.WriteHeader(500)
		return
	}
	JSON(w, 200, out)
}

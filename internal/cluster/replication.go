package cluster

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Replicator struct {
	Peers *Peers
	Root  string
	Token string
	// MyNodeID is this node's ID; peers with the same NodeID are skipped to prevent self-replication via alternate URLs
	MyNodeID string
	// Locker allows the replicator to respect locks by skipping replication when a file is locked
	Locker interface {
		Holder(path string) (string, bool)
	}
}

type ApplyService interface {
	ApplyPut(path string, r io.Reader) error
	ApplyDelete(path string) error
}

func NewReplicator(peers *Peers, root string, token string) *Replicator {
	return &Replicator{Peers: peers, Root: root, Token: token}
}

// WithNodeID sets this node's ID to help avoid self-replication when peers list contains our own URL under a different alias.
func (r *Replicator) WithNodeID(id string) *Replicator { r.MyNodeID = id; return r }

func (r *Replicator) ApplyPut(path string, body io.Reader) error {
	// write to local backing dir
	start := time.Now()
	// If locked by anyone (including self), skip replication fan-out; still write local to keep state
	if r.Locker != nil {
		if holder, ok := r.Locker.Holder("/" + path); ok && holder != "" {
			// write local only, but do not fan out
			if err := writeLocal(filepath.Join(r.Root, path), body); err != nil {
				return err
			}
			log.Printf("replicate: PUT skipped (locked by %s) %s", holder, path)
			return nil
		}
	}
	buf := new(bytes.Buffer)
	tee := io.TeeReader(body, buf)
	if err := writeLocal(filepath.Join(r.Root, path), tee); err != nil {
		return err
	}
	size := buf.Len()
	log.Printf("replicate: local PUT %s size=%d in %s", path, size, time.Since(start))
	// fan out to peers, skipping ourselves by URL and by node ID if known
	targets := make([]string, 0)
	for _, p := range r.Peers.List() {
		if p == r.Peers.Self() {
			continue
		}
		if r.MyNodeID != "" && r.Peers.NodeID(p) == r.MyNodeID {
			continue
		}
		targets = append(targets, p)
	}
	delivered := make([]string, 0, len(targets))
	for _, peer := range targets {
		t0 := time.Now()
		req, _ := http.NewRequest(http.MethodPut, peer+"/v1/file?path="+url.QueryEscape(path), bytes.NewReader(buf.Bytes()))
		if r.Token != "" {
			req.Header.Set("Authorization", "Bearer "+r.Token)
		}
		resp, err := http.DefaultClient.Do(req)
		dur := time.Since(t0)
		if err != nil {
			log.Printf("replicate: PUT %s -> %s failed after %s: %v", path, peer, dur, err)
			RecordStat(peer, "PUT", dur, false)
			continue
		}
		_ = resp.Body.Close()
		ok := resp.StatusCode/100 == 2
		log.Printf("replicate: PUT %s -> %s status=%d in %s", path, peer, resp.StatusCode, dur)
		RecordStat(peer, "PUT", dur, ok)
		if ok {
			delivered = append(delivered, peer)
		}
	}
	log.Printf("replicate: PUT %s delivered=[%s]", path, strings.Join(delivered, ","))
	return nil
}

func (r *Replicator) ApplyDelete(path string) error {
	// If locked, skip replication (and local delete) to honor lock semantics
	if r.Locker != nil {
		if holder, ok := r.Locker.Holder("/" + path); ok && holder != "" {
			log.Printf("replicate: DEL skipped (locked by %s) %s", holder, path)
			return nil
		}
	}
	t0 := time.Now()
	_ = removeLocal(filepath.Join(r.Root, path))
	log.Printf("replicate: local DEL %s in %s", path, time.Since(t0))
	targets := make([]string, 0)
	for _, p := range r.Peers.List() {
		if p == r.Peers.Self() {
			continue
		}
		if r.MyNodeID != "" && r.Peers.NodeID(p) == r.MyNodeID {
			continue
		}
		targets = append(targets, p)
	}
	delivered := make([]string, 0, len(targets))
	for _, peer := range targets {
		ts := time.Now()
		req, _ := http.NewRequest(http.MethodDelete, peer+"/v1/file?path="+url.QueryEscape(path), nil)
		if r.Token != "" {
			req.Header.Set("Authorization", "Bearer "+r.Token)
		}
		resp, err := http.DefaultClient.Do(req)
		dur := time.Since(ts)
		if err != nil {
			log.Printf("replicate: DEL %s -> %s failed after %s: %v", path, peer, dur, err)
			RecordStat(peer, "DEL", dur, false)
			continue
		}
		_ = resp.Body.Close()
		ok := resp.StatusCode/100 == 2
		log.Printf("replicate: DEL %s -> %s status=%d in %s", path, peer, resp.StatusCode, dur)
		RecordStat(peer, "DEL", dur, ok)
		if ok {
			delivered = append(delivered, peer)
		}
	}
	log.Printf("replicate: DEL %s delivered=[%s]", path, strings.Join(delivered, ","))
	return nil
}

// writeLocal stores the reader atomically to the given absolute path within Root.
func writeLocal(path string, r io.Reader) error {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return err
	}
	// write to a temp file in the same directory then atomically rename
	dir := filepath.Dir(path)
	tmp, err := createTemp(dir, ".ice.tmp-*")
	if err != nil {
		return err
	}
	// ensure cleanup on error
	var tmpName string
	if n, ok := tmp.(interface{ Name() string }); ok {
		tmpName = n.Name()
	}
	success := false
	defer func() {
		if !success && tmpName != "" {
			_ = remove(tmpName)
		}
	}()
	if _, err := io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		return err
	}
	if s, ok := tmp.(interface{ Sync() error }); ok {
		if err := s.Sync(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := rename(tmpName, path); err != nil {
		return err
	}
	success = true
	return nil
}

func removeLocal(path string) error {
	return remove(path)
}

// filesystem helpers in separate variables for easier testing/mocking
var (
	ensureDir  = func(dir string) error { return mkdirAll(dir) }
	createTemp = func(dir, pattern string) (io.WriteCloser, error) { return createTempFile(dir, pattern) }
	remove     = func(path string) error { return rm(path) }
	rename     = func(oldPath, newPath string) error { return mv(oldPath, newPath) }
)

// os-level: these will be replaced in fuse process anyway
var (
	mkdirAll       = func(dir string) error { return os.MkdirAll(dir, 0755) }
	createTempFile = func(dir, pattern string) (io.WriteCloser, error) { return os.CreateTemp(dir, pattern) }
	rm             = func(path string) error { return os.Remove(path) }
	mv             = func(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }
)

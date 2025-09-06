package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/spetr/icecluster/internal/cluster"
)

func TestRunConsistencyDetectsMismatch(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "foo"), []byte("bar"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	peers := cluster.NewPeers("http://self")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/index" {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"Path":"foo","Size":4,"MTime":1}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	peers.Add(srv.URL)

	if inc := runConsistency(peers, root, "", ""); inc == 0 {
		t.Fatalf("expected inconsistency detected")
	}
}

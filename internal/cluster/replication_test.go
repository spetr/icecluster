package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteLocalAndRemoveLocal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "x", "a.txt")
	data := []byte("hello world")
	if err := writeLocal(p, bytes.NewReader(data)); err != nil {
		t.Fatalf("writeLocal: %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch: %q", string(got))
	}
	// remove
	if err := removeLocal(p); err != nil {
		t.Fatalf("removeLocal: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("expected not exists, err=%v", err)
	}
}

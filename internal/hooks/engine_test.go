package hooks

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDecideAllowDenyPatch(t *testing.T) {
	tmp := t.TempDir()
	// Script that denies bad path and patches good path
	write(t, tmp, "file_put.lua", `
	function decide(event, payload)
	  if payload.path == "bad" then return {allow=false, reason="nope"} end
	  if payload.path == "old" then return {allow=true, patch={path="new"}} end
	  return {allow=true}
	end
	`)
	eng := New(tmp, time.Second, func(string, ...any) {})
	// deny
	ok, _, reason := eng.Decide(context.Background(), "file_put", map[string]any{"path": "bad"})
	if ok || !strings.Contains(reason, "nope") {
		t.Fatalf("expected deny nope, got ok=%v reason=%q", ok, reason)
	}
	// patch
	ok, out, _ := eng.Decide(context.Background(), "file_put", map[string]any{"path": "old"})
	if !ok {
		t.Fatalf("expected allow")
	}
	if out["path"].(string) != "new" {
		t.Fatalf("expected patch to new, got %v", out["path"])
	}
}

func TestFireRunsScripts(t *testing.T) {
	tmp := t.TempDir()
	var logged []string
	eng := New(tmp, time.Second, func(format string, args ...any) { logged = append(logged, "x") })
	write(t, tmp, "all.lua", `log("hello")`)
	eng.Fire(context.Background(), "anything", map[string]any{"a": 1})
	if len(logged) == 0 {
		t.Fatalf("expected hook log to be called")
	}
}

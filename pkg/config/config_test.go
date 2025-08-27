package config

import (
	os "os"
	testing "testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()
	if cfg.Keepalive <= 0 || cfg.LockTimeout <= 0 {
		t.Fatalf("unexpected defaults")
	}
}

func TestFromFileOverrides(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "cfg-*.yml")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("node_id: test\nkeepalive: 2s\nhooks_log_file: /tmp/x.log\n")
	if _, err := tmp.Write(data); err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	cfg, err := FromFile(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != "test" {
		t.Fatalf("node id not applied")
	}
	if cfg.HooksLogFile != "/tmp/x.log" {
		t.Fatalf("hooks log not parsed: %q", cfg.HooksLogFile)
	}
}

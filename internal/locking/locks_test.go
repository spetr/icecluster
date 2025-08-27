package locking

import "testing"

func TestTryLockUnlock(t *testing.T) {
	m := NewManager()
	// First lock should succeed
	if err := m.TryLock("/a.txt", "n1"); err != nil {
		t.Fatalf("expected lock ok, got %v", err)
	}
	// Same holder re-lock is ok
	if err := m.TryLock("/a.txt", "n1"); err != nil {
		t.Fatalf("expected re-lock ok, got %v", err)
	}
	// Different holder should fail
	if err := m.TryLock("/a.txt", "n2"); err == nil {
		t.Fatalf("expected conflict error")
	}
	// Unlock by non-holder shouldn't remove
	m.Unlock("/a.txt", "n2")
	if h, ok := m.Holder("/a.txt"); !ok || h != "n1" {
		t.Fatalf("expected held by n1, got %v %v", h, ok)
	}
	// Unlock by holder
	m.Unlock("/a.txt", "n1")
	if _, ok := m.Holder("/a.txt"); ok {
		t.Fatalf("expected unlocked")
	}
}

func TestReleaseByHolder(t *testing.T) {
	m := NewManager()
	_ = m.TryLock("/a", "n1")
	_ = m.TryLock("/b", "n1")
	_ = m.TryLock("/c", "n2")
	n := m.ReleaseByHolder("n1")
	if n != 2 {
		t.Fatalf("expected release 2, got %d", n)
	}
	if _, ok := m.Holder("/a"); ok {
		t.Fatalf("/a should be released")
	}
	if _, ok := m.Holder("/b"); ok {
		t.Fatalf("/b should be released")
	}
	if _, ok := m.Holder("/c"); !ok {
		t.Fatalf("/c should remain held")
	}
}

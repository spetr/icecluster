package cluster

import "testing"

func TestPeersAddListCoordinator(t *testing.T) {
	p := NewPeers("http://self:1")
	p.Add("http://b:2")
	p.Add("http://a:3")
	list := p.List()
	if len(list) != 3 {
		t.Fatalf("expected 3, got %d", len(list))
	}
	if p.Coordinator() != list[0] {
		t.Fatalf("coordinator should be first sorted peer")
	}
}

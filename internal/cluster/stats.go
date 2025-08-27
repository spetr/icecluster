package cluster

import (
	"sync"
	"time"
)

// Stats tracks per-peer, per-op counters and timings.
type Stats struct {
	mu    sync.RWMutex
	peers map[string]*peerStats
}

type peerStats struct {
	ops   map[string]*opStats
	total int64
}

type opStats struct {
	req     int64
	success int64
	fail    int64
	totalNs int64
	lastNs  int64
}

// global singleton stats
var stats = &Stats{peers: make(map[string]*peerStats)}

// RecordStat records an operation result for a peer.
func RecordStat(peer, op string, dur time.Duration, success bool) {
	if peer == "" {
		return
	}
	stats.mu.Lock()
	ps := stats.peers[peer]
	if ps == nil {
		ps = &peerStats{ops: make(map[string]*opStats)}
		stats.peers[peer] = ps
	}
	os := ps.ops[op]
	if os == nil {
		os = &opStats{}
		ps.ops[op] = os
	}
	os.req++
	if success {
		os.success++
	} else {
		os.fail++
	}
	ns := dur.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	os.totalNs += ns
	os.lastNs = ns
	ps.total++
	stats.mu.Unlock()
}

type OpSnapshot struct {
	Requests int64   `json:"requests"`
	Success  int64   `json:"success"`
	Fail     int64   `json:"fail"`
	AvgMs    float64 `json:"avg_ms"`
	LastMs   float64 `json:"last_ms"`
}

type PeerSnapshot struct {
	Total int64                 `json:"total"`
	Ops   map[string]OpSnapshot `json:"ops"`
}

// StatsSnapshot returns a copy of current stats suitable for JSON.
func StatsSnapshot() map[string]PeerSnapshot {
	out := make(map[string]PeerSnapshot)
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	for peer, ps := range stats.peers {
		ops := make(map[string]OpSnapshot, len(ps.ops))
		for name, os := range ps.ops {
			avg := 0.0
			if os.req > 0 {
				avg = float64(os.totalNs) / float64(os.req) / 1e6
			}
			ops[name] = OpSnapshot{
				Requests: os.req,
				Success:  os.success,
				Fail:     os.fail,
				AvgMs:    avg,
				LastMs:   float64(os.lastNs) / 1e6,
			}
		}
		out[peer] = PeerSnapshot{Total: ps.total, Ops: ops}
	}
	return out
}

// StatsReset clears all counters.
func StatsReset() {
	stats.mu.Lock()
	stats.peers = make(map[string]*peerStats)
	stats.mu.Unlock()
}

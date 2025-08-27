package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type Peers struct {
	self  string
	mu    sync.RWMutex
	set   map[string]struct{}
	fail  map[string]int      // consecutive failures
	down  map[string]struct{} // peers marked DOWN but still probed for recovery
	hooks interface {
		Fire(ctx context.Context, event string, payload map[string]any)
	}
	nodeIDs map[string]string // last known node IDs per peer URL
	token   string            // optional bearer token for peer-to-peer calls
}

func NewPeers(self string) *Peers {
	p := &Peers{self: normalize(self), set: make(map[string]struct{}), fail: make(map[string]int), down: make(map[string]struct{}), nodeIDs: make(map[string]string)}
	p.set[p.self] = struct{}{}
	return p
}

// NodeID returns last known node ID for a peer URL, or empty if unknown.
func (p *Peers) NodeID(peer string) string {
	p.mu.RLock()
	id := p.nodeIDs[peer]
	p.mu.RUnlock()
	return id
}

// setNodeID records node ID for a peer.
func (p *Peers) setNodeID(peer, id string) {
	if peer == "" || id == "" {
		return
	}
	p.mu.Lock()
	p.nodeIDs[peer] = id
	p.mu.Unlock()
}

// WithHooks attaches a hooks engine to peers.
func (p *Peers) WithHooks(h interface {
	Fire(ctx context.Context, event string, payload map[string]any)
}) *Peers {
	p.hooks = h
	return p
}

// WithToken sets a bearer token used for internal peer-to-peer HTTP calls.
func (p *Peers) WithToken(token string) *Peers { p.token = token; return p }

func normalize(u string) string {
	if u == "" {
		return ""
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "http://" + u
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	parsed.Path = ""
	return parsed.String()
}

func (p *Peers) Self() string { return p.self }

func (p *Peers) Add(u string) bool {
	n := normalize(u)
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.set[n]; ok {
		return false
	}
	p.set[n] = struct{}{}
	delete(p.fail, n)
	delete(p.down, n)
	return true
}

func (p *Peers) AddMany(urls []string) {
	for _, u := range urls {
		if u == "" {
			continue
		}
		p.Add(u)
	}
}

func (p *Peers) List() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	res := make([]string, 0, len(p.set))
	for u := range p.set {
		res = append(res, u)
	}
	sort.Strings(res)
	return res
}

func (p *Peers) ForEach(fn func(peer string)) {
	for _, u := range p.List() {
		if u == p.self {
			continue
		}
		fn(u)
	}
}

func (p *Peers) Coordinator() string {
	list := p.List()
	if len(list) == 0 {
		return p.self
	}
	return list[0]
}

// Best-effort join: notify target of our presence
func (p *Peers) Join(target string) {
	target = normalize(target)
	p.Add(target)
	// tell target to register us
	our := p.Self()
	req, _ := http.NewRequest(http.MethodPost, target+"/v1/peers/register?url="+url.QueryEscape(our), nil)
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err == nil && resp != nil && resp.Body != nil {
		defer resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			// expect JSON array of peers
			data, _ := io.ReadAll(resp.Body)
			var list []string
			if json.Unmarshal(data, &list) == nil {
				p.AddMany(list)
			}
		}
	}
}

// JoinMany accepts comma-separated list and joins each.
func (p *Peers) JoinMany(list string) {
	if list == "" {
		return
	}
	parts := strings.Split(list, ",")
	for _, it := range parts {
		t := strings.TrimSpace(it)
		if t == "" {
			continue
		}
		p.Join(t)
	}
}

// KeepAlive periodically checks health of peers and prunes dead ones; logs status.
// onRecover is invoked when a peer is probed healthy after being down.
// onDown is invoked when a peer transitions to DOWN (after failThreshold consecutive failures).
func (p *Peers) KeepAlive(interval time.Duration, logf func(format string, args ...any), failThreshold int, onRecover func(peer string), onDown func(peer string)) {
	if interval <= 0 {
		interval = time.Minute
	}
	if failThreshold <= 0 {
		failThreshold = 1
	}
	t := time.NewTicker(interval)
	go func() {
		for range t.C {
			active := make([]string, 0)
			// build probe list: current active peers + down peers
			p.mu.RLock()
			probe := make([]string, 0, len(p.set)+len(p.down))
			for u := range p.set {
				probe = append(probe, u)
			}
			for u := range p.down {
				probe = append(probe, u)
			}
			p.mu.RUnlock()
			for _, peer := range probe {
				if peer == p.self {
					active = append(active, peer)
					continue
				}
				client := http.Client{Timeout: 5 * time.Second}
				reqH, _ := http.NewRequest(http.MethodGet, peer+"/health", nil)
				if p.token != "" {
					reqH.Header.Set("Authorization", "Bearer "+p.token)
				}
				resp, err := client.Do(reqH)
				if err != nil || resp.StatusCode != http.StatusOK {
					p.mu.Lock()
					p.fail[peer]++
					f := p.fail[peer]
					if f >= failThreshold {
						if _, wasActive := p.set[peer]; wasActive {
							logf("keepalive: peer %s marked DOWN after %d failures (%v)", peer, f, err)
							delete(p.set, peer)
							p.down[peer] = struct{}{}
							if onDown != nil {
								onDown(peer)
							}
							if p.hooks != nil {
								p.hooks.Fire(context.Background(), "peer_down", map[string]any{"peer": peer, "error": fmt.Sprint(err)})
							}
						}
					} else {
						logf("keepalive: peer %s failure %d/%d (%v)", peer, f, failThreshold, err)
					}
					p.mu.Unlock()
					continue
				}
				_ = resp.Body.Close()
				// opportunistically fetch node ID for the peer
				reqN, _ := http.NewRequest(http.MethodGet, peer+"/v1/node", nil)
				if p.token != "" {
					reqN.Header.Set("Authorization", "Bearer "+p.token)
				}
				if resp2, err2 := client.Do(reqN); err2 == nil && resp2.StatusCode == http.StatusOK {
					var nd struct {
						ID string `json:"id"`
					}
					if data, err3 := io.ReadAll(resp2.Body); err3 == nil {
						_ = json.Unmarshal(data, &nd)
						if nd.ID != "" {
							p.setNodeID(peer, nd.ID)
						}
					}
					_ = resp2.Body.Close()
				}
				p.mu.Lock()
				_, inActive := p.set[peer]
				if !inActive {
					logf("keepalive: peer %s recovered, re-adding", peer)
					p.set[peer] = struct{}{}
					delete(p.down, peer)
					if p.hooks != nil {
						p.hooks.Fire(context.Background(), "peer_recover", map[string]any{"peer": peer})
					}
					// trigger recovery callback outside lock
					go func(pr string) {
						if onRecover != nil {
							onRecover(pr)
						}
					}(peer)
				}
				p.fail[peer] = 0
				p.mu.Unlock()
				active = append(active, peer)
			}
			// Log active peers
			b, _ := json.Marshal(active)
			logf("keepalive: active peers %s", string(b))
		}
	}()
}

// Remove explicitly removes a peer from the set.
func (p *Peers) Remove(u string) {
	n := normalize(u)
	p.mu.Lock()
	delete(p.set, n)
	delete(p.fail, n)
	delete(p.down, n)
	p.mu.Unlock()
}

// Leave notifies peers to remove us.
func (p *Peers) Leave() {
	our := p.Self()
	p.ForEach(func(peer string) {
		req, _ := http.NewRequest(http.MethodPost, peer+"/v1/peers/unregister?url="+url.QueryEscape(our), nil)
		if p.token != "" {
			req.Header.Set("Authorization", "Bearer "+p.token)
		}
		client := &http.Client{Timeout: 5 * time.Second}
		_, _ = client.Do(req)
	})
}

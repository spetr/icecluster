package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spetr/icecluster/internal/cluster"
	"github.com/spetr/icecluster/internal/fusefs"
	"github.com/spetr/icecluster/internal/hooks"
	"github.com/spetr/icecluster/internal/locking"
	"github.com/spetr/icecluster/internal/logsink"
	"github.com/spetr/icecluster/internal/version"
	"github.com/spetr/icecluster/pkg/config"
)

func main() {
	cfg := config.FromFlags()
	// configure logging sink
	if err := logsink.Configure(cfg.LogFile, cfg.LogVerbose); err != nil {
		log.Fatalf("log configure: %v", err)
	}
	// ensure dirs
	must(os.MkdirAll(cfg.DataDir, 0755))
	must(os.MkdirAll(cfg.Backing, 0755))
	must(os.MkdirAll(cfg.Mount, 0755))

	peers := cluster.NewPeers("http://" + trimHTTPHost(cfg.BindAddr))
	if cfg.APIToken != "" {
		peers.WithToken(cfg.APIToken)
	}
	log.Printf("icecluster version: %s", version.Get())
	// Initialize Lua hooks if configured
	var hookEng *hooks.Engine
	var hooksLogFile *os.File
	if cfg.HooksDir != "" {
		// If hooks_log_file is set, use a separate logger for Lua; else reuse main log
		luaLog := log.Printf
		if cfg.HooksLogFile != "" {
			if f, err := os.OpenFile(cfg.HooksLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
				// close previous if any
				if hooksLogFile != nil {
					_ = hooksLogFile.Close()
				}
				hooksLogFile = f
				logger := log.New(f, "", log.LstdFlags|log.Lmicroseconds)
				luaLog = logger.Printf
			} else {
				log.Printf("hooks: failed to open hooks_log_file %s: %v (falling back to main log)", cfg.HooksLogFile, err)
			}
		}
		hookEng = hooks.New(cfg.HooksDir, cfg.HooksTimeout, luaLog)
		peers.WithHooks(hookEng)
	}
	if cfg.Join != "" {
		peers.JoinMany(cfg.Join)
		// After joining peers, perform initial file synchronization
		if err := initialSync(peers, cfg.Backing, cfg.SyncMode, cfg.APIToken); err != nil {
			log.Fatalf("initial sync failed: %v", err)
		}
	}
	// Validate clock skew versus peers before starting services
	if err := checkClockSkew(peers, cfg.ClockSkewMax); err != nil {
		log.Fatalf("clock skew check failed: %v", err)
	}
	// Print active peers after initial joins
	log.Printf("peers: %v", peers.List())
	// Create lock manager early so KeepAlive onDown can use it
	locks := locking.NewManager()

	// Start keepalive with recovery hook that re-registers and reconciles files
	peers.KeepAlive(cfg.Keepalive, log.Printf, cfg.KeepaliveFailures, func(peer string) {
		// On recovery, try to register with the recovered peer to ensure mutual visibility
		our := peers.Self()
		req, _ := http.NewRequest(http.MethodPost, peer+"/v1/peers/register?url="+url.QueryEscape(our), nil)
		if cfg.APIToken != "" {
			req.Header.Set("Authorization", "Bearer "+cfg.APIToken)
		}
		_, _ = (&http.Client{Timeout: 5 * time.Second}).Do(req)
		// Then run a targeted sync in 'latest' mode to reconcile drift caused by outage
		if err := initialSync(peers, cfg.Backing, "latest", cfg.APIToken); err != nil {
			log.Printf("recovery sync with %s failed: %v", peer, err)
		}
	}, func(peer string) {
		// On down, release all locks held by that peer's node ID (if known)
		nodeID := peers.NodeID(peer)
		if nodeID == "" {
			log.Printf("keepalive: peer %s down but node ID unknown; skipping lock release", peer)
			return
		}
		released := 0
		for _, info := range locks.List() {
			if info.Holder == nodeID {
				locks.Unlock(info.Path, nodeID)
				released++
				if hookEng != nil {
					hookEng.Fire(context.Background(), "lock_release", map[string]any{"path": info.Path, "holder": nodeID, "reason": "peer_down"})
				}
			}
		}
		log.Printf("keepalive: released %d locks held by node %s (%s)", released, nodeID, peer)
	})

	repl := cluster.NewReplicator(peers, cfg.Backing, cfg.APIToken)
	lockClient := cluster.NewLockClient(peers, cfg.NodeID)
	lockClient.Configure(cfg.LockTimeout, cfg.LockRetry)
	lockClient.WithToken(cfg.APIToken)

	httpSrv := cluster.NewHTTPServer(repl, cfg.Backing, peers, locks)
	httpSrv.Token = cfg.APIToken
	httpSrv.Hooks = hookEng
	httpSrv.NodeID = cfg.NodeID
	server := &http.Server{Addr: cfg.BindAddr, Handler: httpSrv.Mux}
	// readiness flag
	var readyFlag atomic.Bool
	httpSrv.ReadyFunc = func() bool {
		if cfg.ReadyRequirePeer {
			return readyFlag.Load() && len(peers.List()) > 1
		}
		return readyFlag.Load()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Handle SIGHUP to reopen log file for rotation
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if err := logsink.Reopen(); err != nil {
					log.Printf("log reopen failed: %v", err)
				} else {
					log.Printf("log reopened on SIGHUP")
				}
				// Also reload configuration file and hooks
				if cfg.SourcePath != "" {
					if newCfg, err := config.FromFile(cfg.SourcePath); err == nil {
						// reconfigure hooks logger if path changed
						if hookEng != nil {
							luaLog := log.Printf
							if newCfg.HooksLogFile != "" {
								if f, err := os.OpenFile(newCfg.HooksLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
									if hooksLogFile != nil && hooksLogFile.Name() != f.Name() {
										_ = hooksLogFile.Close()
									}
									hooksLogFile = f
									logger := log.New(f, "", log.LstdFlags|log.Lmicroseconds)
									luaLog = logger.Printf
								} else {
									log.Printf("hooks: failed to open hooks_log_file %s: %v (keeping previous)", newCfg.HooksLogFile, err)
								}
							}
							hookEng.SetOptions(newCfg.HooksDir, newCfg.HooksTimeout, luaLog)
							hookEng.Reload()
						}
						// save new config
						cfg = newCfg
					}
				}
			}
		}
	}()

	go func() {
		log.Printf("http listening on %s", cfg.BindAddr)
		var err error
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			err = server.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey)
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	go func() {
		if err := fusefs.MountAndServe(ctx, cfg.Mount, cfg.Backing, repl, lockClient, hookEng); err != nil {
			log.Printf("fuse: %v", err)
			cancel()
		}
	}()

	// Periodic consistency check and optional auto-heal
	if cfg.ConsistencyInterval > 0 {
		go func() {
			ticker := time.NewTicker(cfg.ConsistencyInterval)
			defer ticker.Stop()
			log.Printf("consistency: running every %v with mode=%s", cfg.ConsistencyInterval, cfg.ConsistencyMode)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					runConsistency(peers, cfg.Backing, cfg.ConsistencyMode, cfg.APIToken)
				}
			}
		}()
	}

	// Mark ready after initial setup; optionally require successful join-sync
	if cfg.ReadyRequireSync && cfg.Join != "" {
		// If we reached here, initialSync completed; mark ready
		readyFlag.Store(true)
	} else {
		readyFlag.Store(true)
	}

	<-ctx.Done()
	// Notify peers about shutdown so they can remove us
	peers.Leave()
	readyFlag.Store(false)
	_ = server.Shutdown(context.Background())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func trimHTTPHost(bind string) string {
	if bind == "" {
		return "localhost:0"
	}
	if bind[0] == ':' {
		return "localhost" + bind
	}
	return bind
}

func checkClockSkew(peers *cluster.Peers, maxSkew time.Duration) error {
	if maxSkew <= 0 {
		maxSkew = time.Second
	}
	type timeResp struct {
		Unix int64 `json:"unix"`
	}
	client := &http.Client{Timeout: 5 * time.Second}
	now := time.Now()
	var errs []string
	for _, p := range peers.List() {
		// skip self
		if p == peers.Self() {
			continue
		}
		resp, err := client.Get(p + "/v1/time")
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", p, err))
			continue
		}
		var tr timeResp
		if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
			_ = resp.Body.Close()
			errs = append(errs, fmt.Sprintf("%s: %v", p, err))
			continue
		}
		_ = resp.Body.Close()
		peerTime := time.Unix(0, tr.Unix)
		skew := peerTime.Sub(now)
		if skew < 0 {
			skew = -skew
		}
		if skew > maxSkew {
			errs = append(errs, fmt.Sprintf("%s: skew %v > %v", p, skew, maxSkew))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("clock skew errors: %v", errs)
	}
	return nil
}

// initialSync reconciles files from cluster to this node based on mode.
// mode:
//   - "latest": pick newest mtime among peers and our local file
//   - "force": always copy from a peer (first that has the file)
func initialSync(peers *cluster.Peers, root string, mode string, token string) error {
	// Fetch indexes from peers
	type entry struct {
		Path  string
		Size  int64
		MTime int64
	}
	idx := map[string][]entry{}
	client := &http.Client{Timeout: 30 * time.Second}
	for _, p := range peers.List() {
		if p == peers.Self() {
			continue
		}
		req, _ := http.NewRequest(http.MethodGet, p+"/v1/index", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var list []entry
		if err := json.NewDecoder(resp.Body).Decode(&list); err == nil {
			for _, e := range list {
				idx[e.Path] = append(idx[e.Path], e)
			}
		}
		_ = resp.Body.Close()
	}
	// Build local index
	local := map[string]entry{}
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil
		}
		local[rel] = entry{Path: rel, Size: info.Size(), MTime: info.ModTime().UnixNano()}
		return nil
	})
	// For each path in union of (peers ∪ local), decide action
	union := map[string]struct{}{}
	for k := range local {
		union[k] = struct{}{}
	}
	for k := range idx {
		union[k] = struct{}{}
	}
	for path := range union {
		// find newest among peers
		var newest *entry
		for _, e := range idx[path] {
			if newest == nil || e.MTime > newest.MTime {
				cp := e
				newest = &cp
			}
		}
		le, hasLocal := local[path]
		switch mode {
		case "force":
			if newest != nil {
				if err := fetchFileFromAnyPeer(peers, path, root, client, token); err != nil {
					return err
				}
			}
		default: // latest
			if newest == nil {
				// only local exists; nothing to do
				continue
			}
			if !hasLocal || newest.MTime > le.MTime {
				if err := fetchFileFromAnyPeer(peers, path, root, client, token); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func fetchFileFromAnyPeer(peers *cluster.Peers, path, root string, client *http.Client, token string) error {
	var lastErr error
	for _, p := range peers.List() {
		if p == peers.Self() {
			continue
		}
		req, _ := http.NewRequest(http.MethodGet, p+"/v1/file?path="+url.QueryEscape(path), nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != 200 {
			if resp != nil {
				resp.Body.Close()
			}
			lastErr = err
			continue
		}
		// atomic write: write to temp in same dir, then rename
		finalPath := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
			resp.Body.Close()
			return err
		}
		tmp, err := os.CreateTemp(filepath.Dir(finalPath), ".ice.sync-*")
		if err != nil {
			resp.Body.Close()
			return err
		}
		tmpName := tmp.Name()
		if _, err := io.Copy(tmp, resp.Body); err != nil {
			tmp.Close()
			resp.Body.Close()
			_ = os.Remove(tmpName)
			return err
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			resp.Body.Close()
			_ = os.Remove(tmpName)
			return err
		}
		if err := tmp.Close(); err != nil {
			resp.Body.Close()
			_ = os.Remove(tmpName)
			return err
		}
		if err := os.Rename(tmpName, finalPath); err != nil {
			resp.Body.Close()
			_ = os.Remove(tmpName)
			return err
		}
		resp.Body.Close()
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no peer had file %s", path)
}

// runConsistency computes differences across peers and logs a concise summary.
// If mode is 'latest' or 'force', it will call initialSync to heal.
func runConsistency(peers *cluster.Peers, root string, mode string, token string) {
	// Gather peer indexes
	type entry struct {
		Path  string
		Size  int64
		MTime int64
	}
	client := &http.Client{Timeout: 30 * time.Second}
	indexes := map[string]map[string]entry{} // peer -> path -> entry
	for _, p := range peers.List() {
		if p == peers.Self() {
			// Build local index for self
			local := map[string]entry{}
			_ = filepath.Walk(root, func(pth string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				rel, err := filepath.Rel(root, pth)
				if err != nil {
					return nil
				}
				local[rel] = entry{Path: rel, Size: info.Size(), MTime: info.ModTime().UnixNano()}
				return nil
			})
			indexes[p] = local
			continue
		}
		req, _ := http.NewRequest(http.MethodGet, p+"/v1/index", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var list []entry
		if err := json.NewDecoder(resp.Body).Decode(&list); err == nil {
			m := make(map[string]entry, len(list))
			for _, e := range list {
				m[e.Path] = e
			}
			indexes[p] = m
		}
		_ = resp.Body.Close()
	}
	// Compute union of all paths
	union := map[string]struct{}{}
	for _, m := range indexes {
		for path := range m {
			union[path] = struct{}{}
		}
	}
	// Identify inconsistent files
	inconsistent := 0
	for path := range union {
		var base *entry
		same := true
		for _, m := range indexes {
			if e, ok := m[path]; ok {
				if base == nil {
					tmp := e
					base = &tmp
				} else if e.MTime != base.MTime || e.Size != base.Size {
					same = false
					break
				}
			} else {
				same = false
				break
			}
		}
		if !same {
			inconsistent++
		}
	}
	if inconsistent > 0 {
		log.Printf("consistency: %d inconsistent file(s) across %d peer(s)", inconsistent, len(indexes))
	} else {
		log.Printf("consistency: OK across %d peer(s)", len(indexes))
	}
	// Auto-heal if requested
	switch mode {
	case "latest":
		if err := initialSync(peers, root, "latest", token); err != nil {
			log.Printf("consistency: auto-heal (latest) failed: %v", err)
		}
	case "force":
		if err := initialSync(peers, root, "force", token); err != nil {
			log.Printf("consistency: auto-heal (force) failed: %v", err)
		}
	default: // report only
	}
}

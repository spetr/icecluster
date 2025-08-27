package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	// version is embedded via internal/version

	vinfo "github.com/spetr/icecluster/internal/version"
)

type Cmd struct {
	Base string
}

func main() {
	base := flag.String("base", "http://localhost:9000", "base URL of a node (http://host:port)")
	to := flag.Duration("timeout", 10*time.Second, "HTTP timeout")
	resetStats := flag.Bool("reset-stats", false, "reset stats after printing (only for 'stats' command)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "icectl - cluster control\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  icectl version\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] peers\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] health\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] get -path /p\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] put -path /p < file\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] del -path /p\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] lock -path /p -holder id\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] unlock -path /p -holder id\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] locks\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] consistency\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] repair -mode latest|force-from [-source http://host:port]\n")
		fmt.Fprintf(os.Stderr, "  icectl [flags] stats\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	cmd := &Cmd{Base: strings.TrimRight(*base, "/")}
	client := &http.Client{Timeout: *to}
	sub := flag.Arg(0)
	switch sub {
	case "version":
		fmt.Println(vinfo.Get())
		return
	case "peers":
		getJSON(client, cmd.Base+"/v1/peers")
	case "health":
		resp, err := client.Get(cmd.Base + "/health")
		check(err)
		fmt.Println(resp.Status)
	case "get":
		path := mustFlag("-path")
		resp, err := client.Get(cmd.Base + "/v1/file?path=" + url.QueryEscape(path))
		check(err)
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Status)
			os.Exit(1)
		}
		io.Copy(os.Stdout, resp.Body)
	case "put":
		path := mustFlag("-path")
		_, err := http.NewRequest(http.MethodPut, cmd.Base+"/v1/file?path="+url.QueryEscape(path), nil)
		_ = err
		data, err := io.ReadAll(os.Stdin)
		check(err)
		req, _ := http.NewRequest(http.MethodPut, cmd.Base+"/v1/file?path="+url.QueryEscape(path), bytes.NewReader(data))
		resp, err := client.Do(req)
		check(err)
		fmt.Println(resp.Status)
	case "del":
		path := mustFlag("-path")
		req, _ := http.NewRequest(http.MethodDelete, cmd.Base+"/v1/file?path="+url.QueryEscape(path), nil)
		resp, err := client.Do(req)
		check(err)
		fmt.Println(resp.Status)
	case "lock":
		path := mustFlag("-path")
		holder := mustFlag("-holder")
		req, _ := http.NewRequest(http.MethodPost, cmd.Base+"/v1/lock?path="+url.QueryEscape(path)+"&holder="+url.QueryEscape(holder), nil)
		resp, err := client.Do(req)
		check(err)
		fmt.Println(resp.Status)
	case "unlock":
		path := mustFlag("-path")
		holder := mustFlag("-holder")
		req, _ := http.NewRequest(http.MethodPost, cmd.Base+"/v1/unlock?path="+url.QueryEscape(path)+"&holder="+url.QueryEscape(holder), nil)
		resp, err := client.Do(req)
		check(err)
		fmt.Println(resp.Status)
	case "locks":
		resp, err := client.Get(cmd.Base + "/v1/locks")
		check(err)
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Status)
			os.Exit(1)
		}
		// decode and print with elapsed
		var items []struct {
			Path, Holder string
			Since        time.Time
		}
		check(json.NewDecoder(resp.Body).Decode(&items))
		now := time.Now()
		for _, it := range items {
			fmt.Printf("%s\t%s\t%v\n", it.Path, it.Holder, now.Sub(it.Since).Round(time.Second))
		}
	case "consistency":
		checkConsistency(client, cmd.Base)
	case "repair":
		mode := flagOrDefault("-mode", "latest")
		var source string
		if mode == "force-from" {
			source = mustFlag("-source")
		}
		runRepair(client, cmd.Base, mode, source)
	case "stats":
		showStats(client, cmd.Base)
		if *resetStats {
			req, _ := http.NewRequest(http.MethodPost, cmd.Base+"/v1/stats/reset", nil)
			if resp, err := client.Do(req); err == nil {
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
			}
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", sub)
		os.Exit(2)
	}
}

func mustFlag(name string) string {
	for i := 0; i < len(os.Args)-1; i++ {
		if os.Args[i] == name {
			return os.Args[i+1]
		}
	}
	fmt.Fprintf(os.Stderr, "missing %s\n", name)
	os.Exit(2)
	return ""
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func getJSON(client *http.Client, url string) {
	resp, err := client.Get(url)
	check(err)
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Status)
		os.Exit(1)
	}
	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		check(err)
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

// --- Stats ---

func showStats(client *http.Client, base string) {
	resp, err := client.Get(strings.TrimRight(base, "/") + "/v1/stats")
	check(err)
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "error: %s\n", resp.Status)
		os.Exit(1)
	}
	var data map[string]struct {
		Total int64 `json:"total"`
		Ops   map[string]struct {
			Requests int64   `json:"requests"`
			Success  int64   `json:"success"`
			Fail     int64   `json:"fail"`
			AvgMs    float64 `json:"avg_ms"`
			LastMs   float64 `json:"last_ms"`
		} `json:"ops"`
	}
	check(json.NewDecoder(resp.Body).Decode(&data))
	// pretty print
	for peer, snap := range data {
		fmt.Printf("%s total=%d\n", peer, snap.Total)
		for op, m := range snap.Ops {
			fmt.Printf("  %-4s req=%d ok=%d fail=%d avg=%.2fms last=%.2fms\n", op, m.Requests, m.Success, m.Fail, m.AvgMs, m.LastMs)
		}
	}
}

// --- Consistency and repair ---

type fileEntry struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	MTime int64  `json:"mtime"`
}

func getPeersList(client *http.Client, base string) []string {
	resp, err := client.Get(strings.TrimRight(base, "/") + "/v1/peers")
	check(err)
	defer resp.Body.Close()
	var peers []string
	check(json.NewDecoder(resp.Body).Decode(&peers))
	return peers
}

func fetchIndex(client *http.Client, peer string) ([]fileEntry, error) {
	resp, err := client.Get(strings.TrimRight(peer, "/") + "/v1/index")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var list []fileEntry
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func checkConsistency(client *http.Client, base string) {
	peers := getPeersList(client, base)
	// map[peer]map[path]meta
	perPeer := map[string]map[string]fileEntry{}
	union := map[string]struct{}{}
	for _, p := range peers {
		idx, err := fetchIndex(client, p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: index %s: %v\n", p, err)
			continue
		}
		m := map[string]fileEntry{}
		for _, e := range idx {
			m[e.Path] = e
			union[e.Path] = struct{}{}
		}
		perPeer[p] = m
	}
	inconsistent := 0
	for path := range union {
		var ref *fileEntry
		eq := true
		for _, p := range peers {
			e, ok := perPeer[p][path]
			if !ok {
				eq = false
				break
			}
			if ref == nil {
				tmp := e
				ref = &tmp
				continue
			}
			if e.MTime != ref.MTime || e.Size != ref.Size {
				eq = false
				break
			}
		}
		if !eq {
			inconsistent++
			fmt.Printf("INCONSISTENT %s\n", path)
			for _, p := range peers {
				if e, ok := perPeer[p][path]; ok {
					fmt.Printf("  %s\tmtime=%d\tsize=%d\n", p, e.MTime, e.Size)
				} else {
					fmt.Printf("  %s\tmissing\n", p)
				}
			}
		}
	}
	if inconsistent == 0 {
		fmt.Println("All peers consistent.")
	} else {
		fmt.Printf("Total inconsistent files: %d\n", inconsistent)
	}
}

func runRepair(client *http.Client, base, mode, source string) {
	peers := getPeersList(client, base)
	if len(peers) == 0 {
		fmt.Fprintln(os.Stderr, "no peers")
		os.Exit(1)
	}
	// Build per-peer index
	perPeer := map[string]map[string]fileEntry{}
	union := map[string]struct{}{}
	for _, p := range peers {
		idx, err := fetchIndex(client, p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: index %s: %v\n", p, err)
			continue
		}
		m := map[string]fileEntry{}
		for _, e := range idx {
			m[e.Path] = e
			union[e.Path] = struct{}{}
		}
		perPeer[p] = m
	}
	// If force-from, ensure source is in peers
	if mode == "force-from" {
		found := false
		for _, p := range peers {
			if strings.TrimRight(p, "/") == strings.TrimRight(source, "/") {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "source not in peers: %s\n", source)
			os.Exit(2)
		}
	}
	// For each path decide a source and sync
	for path := range union {
		var srcPeer string
		switch mode {
		case "force-from":
			srcPeer = strings.TrimRight(source, "/")
			// if source does not have file: delete from others
			if _, ok := perPeer[srcPeer][path]; !ok {
				for _, tgt := range peers {
					if tgt == srcPeer {
						continue
					}
					if _, ok := perPeer[tgt][path]; ok {
						// delete
						req, _ := http.NewRequest(http.MethodDelete, strings.TrimRight(tgt, "/")+"/v1/file?path="+url.QueryEscape(path), nil)
						if resp, err := client.Do(req); err == nil {
							if resp != nil && resp.Body != nil {
								resp.Body.Close()
							}
						}
					}
				}
				continue
			}
		default: // latest
			// pick peer with the newest mtime
			var bestPeer string
			var best fileEntry
			for _, p := range peers {
				if e, ok := perPeer[p][path]; ok {
					if bestPeer == "" || e.MTime > best.MTime {
						bestPeer, best = p, e
					}
				}
			}
			srcPeer = bestPeer
			if srcPeer == "" {
				continue
			}
		}
		// fetch once from src
		resp, err := client.Get(strings.TrimRight(srcPeer, "/") + "/v1/file?path=" + url.QueryEscape(path))
		if err != nil || resp.StatusCode != 200 {
			if resp != nil {
				resp.Body.Close()
			}
			fmt.Fprintf(os.Stderr, "fetch %s from %s failed: %v\n", path, srcPeer, err)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
			continue
		}
		// push to all targets that differ or miss
		for _, tgt := range peers {
			if tgt == srcPeer {
				continue
			}
			eSrc, _ := perPeer[srcPeer][path]
			eTgt, ok := perPeer[tgt][path]
			if ok && eTgt.MTime == eSrc.MTime && eTgt.Size == eSrc.Size {
				continue
			}
			req, _ := http.NewRequest(http.MethodPut, strings.TrimRight(tgt, "/")+"/v1/file?path="+url.QueryEscape(path), bytes.NewReader(data))
			if resp2, err := client.Do(req); err == nil {
				if resp2 != nil && resp2.Body != nil {
					resp2.Body.Close()
				}
			} else {
				fmt.Fprintf(os.Stderr, "push %s -> %s failed: %v\n", path, tgt, err)
			}
		}
	}
}

func flagOrDefault(name, def string) string {
	for i := 0; i < len(os.Args)-1; i++ {
		if os.Args[i] == name {
			return os.Args[i+1]
		}
	}
	return def
}

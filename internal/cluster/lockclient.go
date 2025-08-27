package cluster

import (
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/spetr/icecluster/internal/locking"
)

type LockClient struct {
	peers   *Peers
	holder  string
	timeout time.Duration
	retry   time.Duration
	token   string
}

func NewLockClient(peers *Peers, holder string) *LockClient {
	// default timeouts if not overridden later
	return &LockClient{peers: peers, holder: holder, timeout: 5 * time.Second, retry: 300 * time.Millisecond}
}

// Configure allows overriding lock wait parameters.
func (c *LockClient) Configure(timeout, retry time.Duration) { c.timeout, c.retry = timeout, retry }

// WithToken sets an optional bearer token for lock API calls.
func (c *LockClient) WithToken(token string) { c.token = token }

// Lock attempts to acquire the lock using configured timeout/retry.
func (c *LockClient) Lock(path string) error { return c.LockWithTimeout(path, c.timeout, c.retry) }

// LockWithTimeout retries TryLock until acquired or timeout expires.
func (c *LockClient) LockWithTimeout(path string, timeout, retry time.Duration) error {
	deadline := time.Now().Add(timeout)
	if timeout <= 0 { // try once
		return c.tryOnce(path)
	}
	if retry <= 0 {
		retry = 200 * time.Millisecond
	}
	for {
		if err := c.tryOnce(path); err == nil {
			return nil
		} else if !errors.Is(err, locking.ErrLocked) {
			// network or unexpected error: brief backoff and retry until timeout
		}
		if time.Now().After(deadline) {
			return locking.ErrLocked
		}
		time.Sleep(retry)
	}
}

func (c *LockClient) Unlock(path string) {
	target := c.peers.Coordinator()
	endpoint := target + "/v1/unlock?path=" + url.QueryEscape(path) + "&holder=" + url.QueryEscape(c.holder)
	req, _ := http.NewRequest(http.MethodPost, endpoint, nil)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	_, _ = client.Do(req)
}

func (c *LockClient) tryOnce(path string) error {
	target := c.peers.Coordinator()
	endpoint := target + "/v1/lock?path=" + url.QueryEscape(path) + "&holder=" + url.QueryEscape(c.holder)
	req, _ := http.NewRequest(http.MethodPost, endpoint, nil)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusConflict:
		return locking.ErrLocked
	default:
		return errors.New(resp.Status)
	}
}

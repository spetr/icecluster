//go:build !linux
// +build !linux

package fusefs

import (
	"context"
	"errors"
	"io"
)

type Apply interface {
	ApplyPut(path string, r io.Reader) error
	ApplyDelete(path string) error
}

type Locker interface {
	Lock(path string) error
	Unlock(path string)
}

type Options struct {
}

func MountAndServe(ctx context.Context, mountpoint, root string, applier Apply, locker Locker, hooks interface {
	Fire(ctx context.Context, event string, payload map[string]any)
	Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string)
}, opts ...Options) error {
	return errors.New("FUSE mount is only supported on Linux in this build")
}

// Unmount is a no-op on non-Linux builds.
func Unmount(mountpoint string) error { return nil }

// UpdateOptions is a no-op on non-Linux builds.
func UpdateOptions(o Options) {}

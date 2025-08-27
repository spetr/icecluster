//go:build linux
// +build linux

package fusefs

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type Apply interface {
	ApplyPut(path string, r io.Reader) error
	ApplyDelete(path string) error
}

type FS struct {
	RootDir string
	Apply   Apply
	Locker  Locker
	Hooks   interface {
		Fire(ctx context.Context, event string, payload map[string]any)
		Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string)
	}
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{fs: f, dir: f.RootDir}, nil
}

type Dir struct {
	fs  *FS
	dir string
}

var _ fs.Node = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRenamer = (*Dir)(nil)
var _ fs.NodeSetattrer = (*Dir)(nil)

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fi, err := os.Stat(d.dir)
	if err != nil {
		return err
	}
	a.Mode = fi.Mode()
	a.Mtime = fi.ModTime()
	if d.fs.Hooks != nil {
		d.fs.Hooks.Fire(ctx, "stat", map[string]any{
			"path":     relPath(d.fs.RootDir, d.dir),
			"kind":     "dir",
			"mode":     uint32(fi.Mode()),
			"size":     fi.Size(),
			"mtime_ns": fi.ModTime().UnixNano(),
		})
	}
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}
	res := make([]fuse.Dirent, 0, len(entries))
	for _, e := range entries {
		de := fuse.Dirent{Name: e.Name()}
		if e.IsDir() {
			de.Type = fuse.DT_Dir
		} else {
			de.Type = fuse.DT_File
		}
		res = append(res, de)
	}
	if d.fs.Hooks != nil {
		if allow, _, _ := d.fs.Hooks.Decide(ctx, "dir_list", map[string]any{"path": relPath(d.fs.RootDir, d.dir)}); !allow {
			return nil, fuse.EPERM
		}
		d.fs.Hooks.Fire(ctx, "dir_list", map[string]any{"path": relPath(d.fs.RootDir, d.dir)})
	}
	return res, nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	p := filepath.Join(d.dir, name)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if fi.IsDir() {
		return &Dir{fs: d.fs, dir: p}, nil
	}
	return &File{fs: d.fs, path: p}, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	p := filepath.Join(d.dir, req.Name)
	if d.fs.Hooks != nil {
		rel := relPath(d.fs.RootDir, p)
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "dir_create", map[string]any{"path": rel, "mode": uint32(req.Mode)}); !allow {
			log.Printf("hook deny dir_create %s: %s", rel, reason)
			return nil, fuse.EPERM
		} else if v, ok := patch["path"].(string); ok && v != "" {
			p = filepath.Join(d.fs.RootDir, v)
		}
	}
	if err := os.MkdirAll(p, req.Mode); err != nil {
		return nil, err
	}
	if d.fs.Hooks != nil {
		d.fs.Hooks.Fire(ctx, "dir_create", map[string]any{"path": relPath(d.fs.RootDir, p)})
	}
	return &Dir{fs: d.fs, dir: p}, nil
}

// Rename implements directory entry renaming for both files and directories.
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd, ok := newDir.(*Dir)
	if !ok {
		return fuse.EIO
	}
	oldPath := filepath.Join(d.dir, req.OldName)
	newPath := filepath.Join(nd.dir, req.NewName)
	if d.fs.Hooks != nil {
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "file_rename", map[string]any{"old": relPath(d.fs.RootDir, oldPath), "new": relPath(d.fs.RootDir, newPath)}); !allow {
			log.Printf("hook deny rename %s -> %s: %s", oldPath, newPath, reason)
			return fuse.EPERM
		} else {
			if v, ok := patch["old"].(string); ok && v != "" {
				oldPath = filepath.Join(d.fs.RootDir, v)
			}
			if v, ok := patch["new"].(string); ok && v != "" {
				newPath = filepath.Join(d.fs.RootDir, v)
			}
		}
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}
	if d.fs.Hooks != nil {
		// Determine whether it's a dir or file post-rename
		isDir := false
		if fi, err := os.Stat(newPath); err == nil {
			isDir = fi.IsDir()
		}
		payload := map[string]any{"old": relPath(d.fs.RootDir, oldPath), "new": relPath(d.fs.RootDir, newPath)}
		if isDir {
			d.fs.Hooks.Fire(ctx, "dir_rename", payload)
		} else {
			d.fs.Hooks.Fire(ctx, "file_rename", payload)
		}
	}
	return nil
}

// Setattr applies attribute changes to a directory and fires hooks.
func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	p := d.dir
	// Ownership
	if req.Valid.Uid() || req.Valid.Gid() {
		uid := -1
		gid := -1
		if req.Valid.Uid() {
			uid = int(req.Uid)
		}
		if req.Valid.Gid() {
			gid = int(req.Gid)
		}
		if err := os.Chown(p, uid, gid); err != nil {
			return err
		}
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "owner_change", map[string]any{"path": relPath(d.fs.RootDir, p), "uid": int(req.Uid), "gid": int(req.Gid), "kind": "dir"})
		}
	}
	// Mode
	if req.Valid.Mode() {
		if err := os.Chmod(p, req.Mode); err != nil {
			return err
		}
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "attr_change", map[string]any{"path": relPath(d.fs.RootDir, p), "mode": uint32(req.Mode), "kind": "dir"})
		}
	}
	// Times
	if req.Valid.Mtime() || req.Valid.Atime() {
		at := time.Now()
		mt := time.Now()
		if req.Valid.Atime() {
			at = req.Atime
		}
		if req.Valid.Mtime() {
			mt = req.Mtime
		}
		if err := os.Chtimes(p, at, mt); err != nil {
			return err
		}
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "attr_change", map[string]any{"path": relPath(d.fs.RootDir, p), "atime_ns": at.UnixNano(), "mtime_ns": mt.UnixNano(), "kind": "dir"})
		}
	}
	// Reply with updated attrs
	if fi, err := os.Stat(p); err == nil {
		resp.Attr.Mode = fi.Mode()
		resp.Attr.Mtime = fi.ModTime()
	}
	return nil
}

type File struct {
	fs   *FS
	path string
}
type Locker interface {
	Lock(path string) error
	Unlock(path string)
}

var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeRemover = (*Dir)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	fi, err := os.Stat(f.path)
	if err != nil {
		return err
	}
	a.Mode = fi.Mode()
	a.Size = uint64(fi.Size())
	a.Mtime = fi.ModTime()
	if f.fs.Hooks != nil {
		f.fs.Hooks.Fire(ctx, "stat", map[string]any{
			"path":     relPath(f.fs.RootDir, f.path),
			"kind":     "file",
			"mode":     uint32(fi.Mode()),
			"size":     fi.Size(),
			"mtime_ns": fi.ModTime().UnixNano(),
		})
	}
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	flags := int(req.Flags)
	// open underlying file
	var fl *os.File
	var err error
	// acquire cluster lock on write opens
	writeMode := flags&os.O_RDWR != 0 || flags&os.O_WRONLY != 0
	if writeMode && f.fs.Locker != nil {
		rp := relPath(f.fs.RootDir, f.path)
		t0 := time.Now()
		if err := f.fs.Locker.Lock(rp); err != nil {
			log.Printf("fuse lock: path=%s failed after %s: %v", rp, time.Since(t0), err)
			return nil, err
		}
		log.Printf("fuse lock: path=%s acquired in %s", rp, time.Since(t0))
	}
	var tmpPath string
	if writeMode {
		// ensure directory exists
		if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
			return nil, err
		}
		// create temp file in same dir
		tmp, err := os.CreateTemp(filepath.Dir(f.path), ".ice.local-*")
		if err != nil {
			return nil, err
		}
		tmpPath = tmp.Name()
		// if not truncation, pre-copy existing content to temp to preserve partial writes/appends
		if flags&os.O_TRUNC == 0 {
			if src, err := os.Open(f.path); err == nil {
				_, _ = io.Copy(tmp, src)
				_ = src.Close()
			}
		}
		fl = tmp
	} else {
		fl, err = os.Open(f.path)
	}
	if err != nil {
		return nil, err
	}
	return &FileHandle{f: f, fl: fl, tmpPath: tmpPath, writeMode: writeMode}, nil
}

type FileHandle struct {
	f         *File
	fl        *os.File
	tmpPath   string
	writeMode bool
}

var _ fs.Handle = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.fl.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buf[:n]
	if h.f.fs.Hooks != nil {
		if allow, _, _ := h.f.fs.Hooks.Decide(ctx, "file_read", map[string]any{"path": relPath(h.f.fs.RootDir, h.f.path), "offset": req.Offset, "size": n}); !allow {
			return fuse.EPERM
		}
		h.f.fs.Hooks.Fire(ctx, "file_read", map[string]any{"path": relPath(h.f.fs.RootDir, h.f.path), "offset": req.Offset, "size": n})
	}
	return nil
}

func (h *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.fl.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	resp.Size = n
	return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// propagate content to cluster on close if file was modified
	if err := h.fl.Sync(); err != nil {
		log.Printf("sync: %v", err)
	}
	if err := h.fl.Close(); err != nil {
		return err
	}
	// If this was a write handle, atomically replace the target file then replicate
	if h.writeMode && h.tmpPath != "" {
		// allow hook to block or rewrite the path before finalizing
		target := h.f.path
		if h.f.fs.Hooks != nil {
			rp := relPath(h.f.fs.RootDir, target)
			if allow, patch, reason := h.f.fs.Hooks.Decide(ctx, "file_put", map[string]any{"path": rp}); !allow {
				log.Printf("hook deny file_put %s: %s", rp, reason)
				_ = os.Remove(h.tmpPath)
				// keep lock handling outside; Release will still unlock below
				return fuse.EPERM
			} else if v, ok := patch["path"].(string); ok && v != "" {
				target = filepath.Join(h.f.fs.RootDir, v)
			}
		}
		// ensure dir exists for target
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			_ = os.Remove(h.tmpPath)
			return err
		}
		// rename temp to final path (possibly patched)
		if err := os.Rename(h.tmpPath, target); err != nil {
			// attempt cleanup of temp on failure
			_ = os.Remove(h.tmpPath)
			return err
		}
		// open final to replicate
		f, err := os.Open(target)
		if err == nil {
			defer f.Close()
			if err := h.f.fs.Apply.ApplyPut(relPath(h.f.fs.RootDir, target), f); err != nil {
				log.Printf("replicate put: %v", err)
			}
		}
	}
	if h.f.fs.Locker != nil {
		rp := relPath(h.f.fs.RootDir, h.f.path)
		t0 := time.Now()
		h.f.fs.Locker.Unlock(rp)
		log.Printf("fuse unlock: path=%s in %s", rp, time.Since(t0))
	}
	return nil
}

// Setattr applies attribute/owner/size changes to a file and fires hooks.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	p := f.path
	// Size (truncate)
	if req.Valid.Size() {
		if err := os.Truncate(p, int64(req.Size)); err != nil {
			return err
		}
		if f.fs.Hooks != nil {
			f.fs.Hooks.Fire(ctx, "attr_change", map[string]any{"path": relPath(f.fs.RootDir, p), "size": int64(req.Size), "kind": "file"})
		}
	}
	// Ownership
	if req.Valid.Uid() || req.Valid.Gid() {
		uid := -1
		gid := -1
		if req.Valid.Uid() {
			uid = int(req.Uid)
		}
		if req.Valid.Gid() {
			gid = int(req.Gid)
		}
		if err := os.Chown(p, uid, gid); err != nil {
			return err
		}
		if f.fs.Hooks != nil {
			f.fs.Hooks.Fire(ctx, "owner_change", map[string]any{"path": relPath(f.fs.RootDir, p), "uid": int(req.Uid), "gid": int(req.Gid), "kind": "file"})
		}
	}
	// Mode
	if req.Valid.Mode() {
		if err := os.Chmod(p, req.Mode); err != nil {
			return err
		}
		if f.fs.Hooks != nil {
			f.fs.Hooks.Fire(ctx, "attr_change", map[string]any{"path": relPath(f.fs.RootDir, p), "mode": uint32(req.Mode), "kind": "file"})
		}
	}
	// Times
	if req.Valid.Mtime() || req.Valid.Atime() {
		at := time.Now()
		mt := time.Now()
		if req.Valid.Atime() {
			at = req.Atime
		}
		if req.Valid.Mtime() {
			mt = req.Mtime
		}
		if err := os.Chtimes(p, at, mt); err != nil {
			return err
		}
		if f.fs.Hooks != nil {
			f.fs.Hooks.Fire(ctx, "attr_change", map[string]any{"path": relPath(f.fs.RootDir, p), "atime_ns": at.UnixNano(), "mtime_ns": mt.UnixNano(), "kind": "file"})
		}
	}
	// Reply with updated attrs
	if fi, err := os.Stat(p); err == nil {
		resp.Attr.Mode = fi.Mode()
		resp.Attr.Size = uint64(fi.Size())
		resp.Attr.Mtime = fi.ModTime()
	}
	return nil
}

func relPath(root, p string) string {
	r, err := filepath.Rel(root, p)
	if err != nil {
		return p
	}
	return r
}

func MountAndServe(ctx context.Context, mountpoint, root string, applier Apply, locker Locker, hooks interface {
	Fire(ctx context.Context, event string, payload map[string]any)
	Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string)
}) error {
	c, err := fuse.Mount(mountpoint, fuse.FSName("icecluster"), fuse.Subtype("ice"))
	if err != nil {
		return err
	}
	defer c.Close()

	fsys := &FS{RootDir: root, Apply: applier, Locker: locker, Hooks: hooks}
	return fs.Serve(c, fsys)
}

// FS implements removal via Dir.Remove
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	p := filepath.Join(d.dir, req.Name)
	if req.Dir {
		if d.fs.Hooks != nil {
			if allow, patch, reason := d.fs.Hooks.Decide(ctx, "dir_delete", map[string]any{"path": relPath(d.fs.RootDir, p)}); !allow {
				log.Printf("hook deny dir_delete %s: %s", p, reason)
				return fuse.EPERM
			} else if v, ok := patch["path"].(string); ok && v != "" {
				p = filepath.Join(d.fs.RootDir, v)
			}
		}
		// For safety, remove only if empty; return ENOTEMPTY otherwise
		if err := os.Remove(p); err != nil {
			// map directory not empty to proper fuse error
			if os.IsExist(err) || err == syscall.ENOTEMPTY { // platform differences
				return fuse.Errno(syscall.ENOTEMPTY)
			}
			return err
		}
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "dir_delete", map[string]any{"path": relPath(d.fs.RootDir, p)})
		}
		return nil
	}
	if d.fs.Hooks != nil {
		// allow scripts to block or rewrite file path before delete
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "file_delete", map[string]any{"path": relPath(d.fs.RootDir, p)}); !allow {
			log.Printf("hook deny file_delete %s: %s", p, reason)
			return fuse.EPERM
		} else if v, ok := patch["path"].(string); ok && v != "" {
			p = filepath.Join(d.fs.RootDir, v)
		}
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	// replicate delete
	return d.fs.Apply.ApplyDelete(relPath(d.fs.RootDir, p))
}

func isENOSYS(err error) bool {
	return err == syscall.ENOSYS
}

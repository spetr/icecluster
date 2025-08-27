//go:build linux
// +build linux

package fusefs

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/spetr/icecluster/internal/logsink"
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
	// track open write handles to handle unlink-while-open (e.g., vim swap files)
	openMu sync.Mutex
	open   map[string]*openState
	// If true, honoring delete while open means skipping finalize on close
}

type openState struct {
	ref     int
	deleted bool
	files   []*os.File
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{fs: f, dir: f.RootDir}, nil
}

// registerWrite increments refcount for a path being written
func (f *FS) registerWrite(path string, fl *os.File) {
	if f == nil {
		return
	}
	f.openMu.Lock()
	defer f.openMu.Unlock()
	if f.open == nil {
		f.open = make(map[string]*openState)
	}
	st, ok := f.open[path]
	if !ok {
		st = &openState{}
		f.open[path] = st
	}
	st.ref++
	if fl != nil {
		st.files = append(st.files, fl)
	}
}

// unregisterWrite decrements refcount and clears state if zero; returns whether path was marked deleted
func (f *FS) unregisterWrite(path string, fl *os.File) (wasDeleted bool) {
	if f == nil {
		return false
	}
	f.openMu.Lock()
	defer f.openMu.Unlock()
	if st, ok := f.open[path]; ok {
		wasDeleted = st.deleted
		// remove file handle
		if fl != nil && len(st.files) > 0 {
			for i, x := range st.files {
				if x == fl {
					st.files = append(st.files[:i], st.files[i+1:]...)
					break
				}
			}
		}
		st.ref--
		if st.ref <= 0 {
			delete(f.open, path)
		}
	}
	return
}

// markDeleted marks a path as deleted while it may still have open handles
func (f *FS) markDeleted(path string) {
	if f == nil {
		return
	}
	f.openMu.Lock()
	defer f.openMu.Unlock()
	if f.open == nil {
		return
	}
	if st, ok := f.open[path]; ok {
		st.deleted = true
	}
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
var _ fs.NodeCreater = (*Dir)(nil)

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
	// verbose: directory listing
	logsink.Vprintf("fuse readdir: path=%s entries=%d", relPath(d.fs.RootDir, d.dir), len(res))
	if d.fs.Hooks != nil {
		if allow, _, _ := d.fs.Hooks.Decide(ctx, "dir_list", map[string]any{"path": relPath(d.fs.RootDir, d.dir)}); !allow {
			return nil, syscall.Errno(syscall.EPERM)
		}
		d.fs.Hooks.Fire(ctx, "dir_list", map[string]any{"path": relPath(d.fs.RootDir, d.dir)})
	}
	return res, nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	p := filepath.Join(d.dir, name)
	fi, err := os.Stat(p)
	if err != nil {
		// verbose: not found
		logsink.Vprintf("fuse lookup: path=%s notfound", relPath(d.fs.RootDir, p))
		return nil, fuse.ENOENT
	}
	logsink.Vprintf("fuse lookup: path=%s", relPath(d.fs.RootDir, p))
	if fi.IsDir() {
		return &Dir{fs: d.fs, dir: p}, nil
	}
	return &File{fs: d.fs, path: p}, nil
}

// Create handles O_CREAT for files under this directory.
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	target := filepath.Join(d.dir, req.Name)
	// O_EXCL: if target exists, fail
	if int(req.Flags)&os.O_EXCL != 0 {
		if _, err := os.Stat(target); err == nil {
			return nil, nil, fuse.Errno(syscall.EEXIST)
		}
	}
	// Acquire cluster lock before writes, similar to File.Open
	if d.fs.Locker != nil {
		rp := relPath(d.fs.RootDir, target)
		t0 := time.Now()
		if err := d.fs.Locker.Lock(rp); err != nil {
			log.Printf("fuse lock(create): path=%s failed after %s: %v", rp, time.Since(t0), err)
			return nil, nil, err
		}
		log.Printf("fuse lock(create): path=%s acquired in %s", rp, time.Since(t0))
	}
	// Create target directly (pass-through)
	if err := os.MkdirAll(d.dir, 0755); err != nil {
		return nil, nil, err
	}
	flags := int(req.Flags)
	if flags&os.O_CREATE == 0 {
		flags |= os.O_CREATE
	}
	fl, err := os.OpenFile(target, flags, req.Mode)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("fuse create: path=%s mode=%#o flags=%d", relPath(d.fs.RootDir, target), uint32(req.Mode), int(req.Flags))
	fh := &FileHandle{f: &File{fs: d.fs, path: target}, fl: fl, writeMode: true}
	d.fs.registerWrite(target, fl)
	return fh.f, fh, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	p := filepath.Join(d.dir, req.Name)
	if d.fs.Hooks != nil {
		rel := relPath(d.fs.RootDir, p)
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "dir_create", map[string]any{"path": rel, "mode": uint32(req.Mode)}); !allow {
			log.Printf("hook deny dir_create %s: %s", rel, reason)
			return nil, syscall.Errno(syscall.EPERM)
		} else if v, ok := patch["path"].(string); ok && v != "" {
			p = filepath.Join(d.fs.RootDir, v)
		}
	}
	if err := os.MkdirAll(p, req.Mode); err != nil {
		return nil, err
	}
	log.Printf("fuse mkdir: path=%s mode=%#o", relPath(d.fs.RootDir, p), uint32(req.Mode))
	if d.fs.Hooks != nil {
		d.fs.Hooks.Fire(ctx, "dir_create", map[string]any{"path": relPath(d.fs.RootDir, p)})
	}
	return &Dir{fs: d.fs, dir: p}, nil
}

// Rename implements directory entry renaming for both files and directories.
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd, ok := newDir.(*Dir)
	if !ok {
		return syscall.Errno(syscall.EIO)
	}
	oldPath := filepath.Join(d.dir, req.OldName)
	newPath := filepath.Join(nd.dir, req.NewName)
	if d.fs.Hooks != nil {
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "file_rename", map[string]any{"old": relPath(d.fs.RootDir, oldPath), "new": relPath(d.fs.RootDir, newPath)}); !allow {
			log.Printf("hook deny rename %s -> %s: %s", oldPath, newPath, reason)
			return syscall.Errno(syscall.EPERM)
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
	log.Printf("fuse rename: old=%s new=%s", relPath(d.fs.RootDir, oldPath), relPath(d.fs.RootDir, newPath))
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
		logsink.Vprintf("fuse setattr(dir:owner): path=%s uid=%d gid=%d", relPath(d.fs.RootDir, p), uid, gid)
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "owner_change", map[string]any{"path": relPath(d.fs.RootDir, p), "uid": int(req.Uid), "gid": int(req.Gid), "kind": "dir"})
		}
	}
	// Mode
	if req.Valid.Mode() {
		if err := os.Chmod(p, req.Mode); err != nil {
			return err
		}
		logsink.Vprintf("fuse setattr(dir:mode): path=%s mode=%#o", relPath(d.fs.RootDir, p), uint32(req.Mode))
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
		logsink.Vprintf("fuse setattr(dir:times): path=%s atime=%d mtime=%d", relPath(d.fs.RootDir, p), at.UnixNano(), mt.UnixNano())
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
	// open underlying file directly (pass-through)
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
	// ensure directory exists for write opens
	if writeMode {
		if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
			return nil, err
		}
	}
	// open the actual path using requested flags; mode is ignored unless O_CREATE is used
	fl, err = os.OpenFile(f.path, flags, 0644)
	if err != nil {
		return nil, err
	}
	logsink.Vprintf("fuse open: path=%s flags=%d write=%t", relPath(f.fs.RootDir, f.path), flags, writeMode)
	if writeMode {
		f.fs.registerWrite(f.path, fl)
	}
	return &FileHandle{f: f, fl: fl, tmpPath: "", writeMode: writeMode}, nil
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
var _ fs.HandleFlusher = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*File)(nil)

func (h *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.fl.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buf[:n]
	if h.f.fs.Hooks != nil {
		if allow, _, _ := h.f.fs.Hooks.Decide(ctx, "file_read", map[string]any{"path": relPath(h.f.fs.RootDir, h.f.path), "offset": req.Offset, "size": n}); !allow {
			return syscall.Errno(syscall.EPERM)
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
	// verbose log each write chunk to avoid spamming default logs
	logsink.Vprintf("fuse write: path=%s off=%d size=%d", relPath(h.f.fs.RootDir, h.f.path), req.Offset, n)
	return nil
}

func (h *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// ensure content is flushed
	if err := h.fl.Sync(); err != nil {
		log.Printf("sync: %v", err)
	}
	// close handle
	if err := h.fl.Close(); err != nil {
		return err
	}
	// Capture write/deletion state, then release the cluster lock before any replication
	var (
		shouldReplicate bool
		targetPath      string
	)
	if h.writeMode {
		wasDeleted := h.f.fs.unregisterWrite(h.f.path, h.fl)
		if !wasDeleted {
			// allow hook to block or rewrite the path before replication
			target := h.f.path
			if h.f.fs.Hooks != nil {
				rp := relPath(h.f.fs.RootDir, target)
				if allow, patch, reason := h.f.fs.Hooks.Decide(ctx, "file_put", map[string]any{"path": rp}); !allow {
					log.Printf("hook deny file_put %s: %s", rp, reason)
					// do not replicate; proceed to unlock
				} else if v, ok := patch["path"].(string); ok && v != "" {
					target = filepath.Join(h.f.fs.RootDir, v)
				}
			}
			// if file exists, we'll replicate after unlocking
			if _, err := os.Stat(target); err == nil {
				shouldReplicate = true
				targetPath = target
			}
		}
	}
	// Always unlock before any replication to ensure we don't replicate while a lock exists
	if h.f.fs.Locker != nil {
		rp := relPath(h.f.fs.RootDir, h.f.path)
		t0 := time.Now()
		h.f.fs.Locker.Unlock(rp)
		log.Printf("fuse unlock: path=%s in %s", rp, time.Since(t0))
	}
	// Perform replication only after unlocking
	if shouldReplicate {
		if fi, err := os.Stat(targetPath); err == nil {
			log.Printf("fuse write commit: path=%s size=%d", relPath(h.f.fs.RootDir, targetPath), fi.Size())
			if f, err := os.Open(targetPath); err == nil {
				defer f.Close()
				if err := h.f.fs.Apply.ApplyPut(relPath(h.f.fs.RootDir, targetPath), f); err != nil {
					log.Printf("replicate put: %v", err)
				}
			}
		}
	}
	return nil
}

// Fsync ensures file contents are flushed to stable storage for this handle.
func (h *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// req.Flags.Datasync is ignored; we just Sync the file handle
	if h.fl != nil {
		if err := h.fl.Sync(); err != nil {
			return err
		}
		logsink.Vprintf("fuse fsync(handle): path=%s", relPath(h.f.fs.RootDir, h.f.path))
	}
	return nil
}

// Flush is called on close of a file descriptor; ensure data hits disk for write handles.
func (h *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if h.writeMode && h.fl != nil {
		if err := h.fl.Sync(); err != nil {
			return err
		}
		logsink.Vprintf("fuse flush: path=%s", relPath(h.f.fs.RootDir, h.f.path))
	}
	return nil
}

// Fsync is invoked to flush file data to disk while handle remains open.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// sync the on-disk file if it exists
	fl, err := os.Open(f.path)
	if err != nil {
		// if missing, nothing to sync
		return nil
	}
	defer fl.Close()
	if err := fl.Sync(); err != nil {
		return err
	}
	if f.fs.Hooks != nil {
		f.fs.Hooks.Fire(ctx, "file_sync", map[string]any{"path": relPath(f.fs.RootDir, f.path)})
	}
	logsink.Vprintf("fuse fsync(node): path=%s", relPath(f.fs.RootDir, f.path))
	return nil
}

// Setattr applies attribute/owner/size changes to a file and fires hooks.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	p := f.path
	// Size (truncate)
	if req.Valid.Size() {
		// Apply truncate directly to the actual path
		if err := os.Truncate(p, int64(req.Size)); err != nil {
			return err
		}
		logsink.Vprintf("fuse setattr(file:size): path=%s size=%d", relPath(f.fs.RootDir, p), int64(req.Size))
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
		logsink.Vprintf("fuse setattr(file:owner): path=%s uid=%d gid=%d", relPath(f.fs.RootDir, p), uid, gid)
		if f.fs.Hooks != nil {
			f.fs.Hooks.Fire(ctx, "owner_change", map[string]any{"path": relPath(f.fs.RootDir, p), "uid": int(req.Uid), "gid": int(req.Gid), "kind": "file"})
		}
	}
	// Mode
	if req.Valid.Mode() {
		if err := os.Chmod(p, req.Mode); err != nil {
			return err
		}
		logsink.Vprintf("fuse setattr(file:mode): path=%s mode=%#o", relPath(f.fs.RootDir, p), uint32(req.Mode))
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
		logsink.Vprintf("fuse setattr(file:times): path=%s atime=%d mtime=%d", relPath(f.fs.RootDir, p), at.UnixNano(), mt.UnixNano())
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

// keep a reference to the mounted FS to allow runtime option updates (e.g., on SIGHUP)
var mountedFSMu sync.Mutex
var mountedFS *FS

func MountAndServe(ctx context.Context, mountpoint, root string, applier Apply, locker Locker, hooks interface {
	Fire(ctx context.Context, event string, payload map[string]any)
	Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string)
}) error {
	// Attempt to mount; if it fails (possibly due to stale mount), unmount and retry once.
	c, err := fuse.Mount(mountpoint, fuse.FSName("icecluster"), fuse.Subtype("ice"))
	if err != nil {
		_ = fuse.Unmount(mountpoint)
		time.Sleep(200 * time.Millisecond)
		c, err = fuse.Mount(mountpoint, fuse.FSName("icecluster"), fuse.Subtype("ice"))
		if err != nil {
			return err
		}
	}
	// Ensure connection close and best-effort unmount on exit
	defer func() {
		_ = fuse.Unmount(mountpoint)
		c.Close()
	}()

	// Log successful FUSE mount
	log.Printf("fuse mounted: mountpoint=%s backing=%s", mountpoint, root)

	// On context cancellation, request unmount so the next start doesn't require manual fusermount3 -u
	go func() {
		<-ctx.Done()
		_ = fuse.Unmount(mountpoint)
	}()

	fsys := &FS{RootDir: root, Apply: applier, Locker: locker, Hooks: hooks}
	mountedFSMu.Lock()
	mountedFS = fsys
	mountedFSMu.Unlock()
	return fs.Serve(c, fsys)
}

// Unmount requests unmount of the given mountpoint (Linux build).
func Unmount(mountpoint string) error {
	return fuse.Unmount(mountpoint)
}

// FS implements removal via Dir.Remove
func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	p := filepath.Join(d.dir, req.Name)
	log.Printf("fuse delete: path=%s", relPath(d.fs.RootDir, req.Name))
	if req.Dir {
		if d.fs.Hooks != nil {
			if allow, patch, reason := d.fs.Hooks.Decide(ctx, "dir_delete", map[string]any{"path": relPath(d.fs.RootDir, p)}); !allow {
				log.Printf("hook deny dir_delete %s: %s", p, reason)
				return syscall.Errno(syscall.EPERM)
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
		log.Printf("fuse rmdir: path=%s", relPath(d.fs.RootDir, p))
		if d.fs.Hooks != nil {
			d.fs.Hooks.Fire(ctx, "dir_delete", map[string]any{"path": relPath(d.fs.RootDir, p)})
		}
		return nil
	}
	if d.fs.Hooks != nil {
		// allow scripts to block or rewrite file path before delete
		if allow, patch, reason := d.fs.Hooks.Decide(ctx, "file_delete", map[string]any{"path": relPath(d.fs.RootDir, p)}); !allow {
			log.Printf("hook deny file_delete %s: %s", p, reason)
			return syscall.Errno(syscall.EPERM)
		} else if v, ok := patch["path"].(string); ok && v != "" {
			p = filepath.Join(d.fs.RootDir, v)
		}
	}
	if err := os.Remove(p); err != nil {
		return err
	}
	// mark as deleted while potential writers may still have the file open
	d.fs.markDeleted(p)
	// replicate delete
	if err := d.fs.Apply.ApplyDelete(relPath(d.fs.RootDir, p)); err != nil {
		log.Printf("replicate delete failed: path=%s err=%v", relPath(d.fs.RootDir, p), err)
		return err
	}
	log.Printf("fuse delete replicated: path=%s", relPath(d.fs.RootDir, p))
	return nil
}

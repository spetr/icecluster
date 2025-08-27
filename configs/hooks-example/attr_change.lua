-- attr_change.lua
-- Payload: { path = "relative/path", kind = "file"|"dir", mode?=<uint>, size?=<int>, atime_ns?=<int>, mtime_ns?=<int> }

log("attr_change: path=" .. payload.path)

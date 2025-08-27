-- stat.lua
-- Payload: { path = "relative/path", kind = "file"|"dir", mode = <uint>, size = <int>, mtime_ns = <int> }
log(string.format("stat: %s kind=%s mode=%o size=%d mtime_ns=%d", payload.path, payload.kind, payload.mode, payload.size, payload.mtime_ns))

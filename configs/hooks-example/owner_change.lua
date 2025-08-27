-- owner_change.lua
-- Payload: { path = "relative/path", kind = "file"|"dir", uid?=<int>, gid?=<int> }

log(string.format("owner_change: %s uid=%s gid=%s", payload.path, tostring(payload.uid), tostring(payload.gid)))

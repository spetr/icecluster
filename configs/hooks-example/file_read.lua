-- file_read.lua
-- Payload: { path = "relative/path", offset = <number>, size = <number> }
log(string.format("file read: %s offset=%d size=%d", payload.path, payload.offset, payload.size))

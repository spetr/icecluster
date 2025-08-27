-- Called after a successful fsync of a file in the FUSE layer.
-- payload = { path = "relative/path" }

log("file_sync:", payload.path)

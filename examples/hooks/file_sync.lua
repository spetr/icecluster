-- Example hook for file_sync event
-- Called after a file has been successfully fsync'ed in the FUSE layer.
-- Available globals:
--   event   -> string, here equals "file_sync"
--   payload -> table, with keys:
--              path: relative path of the file in the volume
-- You can perform logging by calling `log(...)`.

function run()
  log("file_sync", payload.path)
end

run()

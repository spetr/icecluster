# Hooks examples

Place these files into your hooks directory (configured in your YAML), e.g. `hooks_dir: ./examples/hooks`.

## file_sync.lua
Fired after a successful fsync of a file from FUSE. Payload:

- path: relative path of the synced file

## all.lua
Logs every event name (and path if present) to demonstrate the hook interface.

Notes:
- Scripts run with a timeout configured in `hooks_timeout` (default ~2s).
- Use `log("..." )` to write to the server logs.

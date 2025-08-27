# Lua Hooks Examples

Place these files into your configured `hooks_dir`.

Files can be named by event (e.g., `peer_join.lua`) or put under a subdirectory
matching the event name (e.g., `peer_join/01-notify.lua`). A special `all.lua`
receives every event.

Available events wired right now:
- peer_join { peer }
- peer_leave { peer }
- peer_down { peer, error }
- peer_recover { peer }
- lock_grant { path, holder }
- lock_deny { path, holder, error }
- lock_release { path, holder }
- file_put { path }
- file_delete { path }
- dir_create { path }
- dir_delete { path }
- dir_rename { old, new }
- file_rename { old, new }
- file_read { path, offset, size }
- dir_list { path }
- stat { path, kind, mode, size, mtime_ns }
- attr_change { path, kind, mode?, size?, atime_ns?, mtime_ns? }
- owner_change { path, kind, uid?, gid? }
- file_sync { path }

Globals available in Lua:
- `event`: string event name
- `payload`: table with event fields
- `log(...)`: logs a line via server logger

Blocking or modifying operations (policy hooks):
- Scripts may optionally define a global function `decide(event, payload)`.
- Return a Lua table to allow/deny and optionally patch data:

	return {
		allow = true|false,        -- required; false blocks the operation
		reason = "text",           -- optional; shown when blocked
		patch  = { k = v, ... }    -- optional; shallow-merged into payload
	}

- If multiple scripts are present for an event:
	- They run in order; the first explicit `allow=false` denies the operation.
	- `patch` values are merged and passed to subsequent scripts and the handler.

Examples:
- Deny writes to a specific path (file_put):

	function decide(event, payload)
		if event == "file_put" and payload.path == "secret.conf" then
			return { allow=false, reason="writes to secret.conf are forbidden" }
		end
		return { allow=true }
	end

- Rewrite directory creation target (dir_create):

	function decide(event, payload)
		if event == "dir_create" and payload.path == "tmp" then
			return { allow=true, patch={ path = "var/tmp" } }
		end
		return { allow=true }
	end

- Block lock grant for a path owned by someone else (lock_grant):

	function decide(event, payload)
		if event == "lock_grant" and payload.path == "/config" and payload.holder ~= os.getenv("NODE") then
			return { allow=false, reason="only owner can lock /config" }
		end
		return { allow=true }
	end

Tips:
- Keep scripts fast; long-running work should be queued or forked
- Use the `hooks_timeout` to cap script runtime
- Compose multiple scripts per event by using the event subdir

-- Called when keepalive marks a peer as DOWN
local p = payload.peer or "?"
local err = payload.error or ""
log("Peer DOWN:", p, err)

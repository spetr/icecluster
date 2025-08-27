-- Called when a peer is probed back as healthy and re-added

local p = payload.peer or "?"
log("Peer recovered:", p)

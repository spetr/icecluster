-- Called when a peer unregisters from this node

local p = payload.peer or "?"
log("Peer left:", p)

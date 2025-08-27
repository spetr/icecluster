-- Called when a lock request is denied (409)
log("Lock deny:", payload.path, "holder=", payload.holder, "error=", payload.error)

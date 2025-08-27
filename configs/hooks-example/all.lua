-- Runs for every event

log("[all] event:", event)
for k,v in pairs(payload or {}) do
  log("  ", k, "=", tostring(v))
end

-- Global hook that logs all events to demonstrate the interface
-- It will be invoked for any event name if placed in the hooks directory as all.lua

function run()
  if payload and payload.path then
    log("event:", event, "path:", payload.path)
  else
    log("event:", event)
  end
end

run()

package hooks

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// Engine runs Lua scripts on lifecycle events.
type Engine struct {
	dir     string
	timeout time.Duration
	logf    func(format string, args ...any)
	loaded  []string
}

func New(dir string, timeout time.Duration, logf func(string, ...any)) *Engine {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	e := &Engine{dir: dir, timeout: timeout, logf: logf}
	e.Reload()
	return e
}

// Fire executes scripts for the named event with a payload.
// It looks for: dir/<event>.lua, dir/<event>/*.lua, and dir/all.lua
func (e *Engine) Fire(ctx context.Context, event string, payload map[string]any) {
	if e == nil || e.dir == "" {
		return
	}
	// Gather candidate scripts
	var candidates []string
	add := func(p string) {
		if p != "" {
			candidates = append(candidates, p)
		}
	}
	add(filepath.Join(e.dir, event+".lua"))
	// event directory
	if entries, err := os.ReadDir(filepath.Join(e.dir, event)); err == nil {
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			if strings.HasSuffix(ent.Name(), ".lua") {
				add(filepath.Join(e.dir, event, ent.Name()))
			}
		}
	}
	add(filepath.Join(e.dir, "all.lua"))

	for _, script := range candidates {
		if _, err := os.Stat(script); err != nil {
			continue
		}
		// Run each script with timeout
		c, cancel := context.WithTimeout(ctx, e.timeout)
		if err := e.run(c, script, event, payload); err != nil {
			e.logf("hooks: %s error: %v", script, err)
		}
		cancel()
	}
}

// Reload scans the hooks directory, syntax-checks Lua files, and logs them.
func (e *Engine) Reload() {
	if e == nil || e.dir == "" {
		return
	}
	var scripts []string
	// top-level *.lua
	entries, err := os.ReadDir(e.dir)
	if err == nil {
		for _, ent := range entries {
			if ent.IsDir() {
				// scan subdir for *.lua
				sub := filepath.Join(e.dir, ent.Name())
				files, _ := os.ReadDir(sub)
				for _, f := range files {
					if !f.IsDir() && strings.HasSuffix(f.Name(), ".lua") {
						scripts = append(scripts, filepath.Join(sub, f.Name()))
					}
				}
				continue
			}
			if strings.HasSuffix(ent.Name(), ".lua") {
				scripts = append(scripts, filepath.Join(e.dir, ent.Name()))
			}
		}
	}
	// de-dup and stable order
	seen := map[string]struct{}{}
	uniq := make([]string, 0, len(scripts))
	for _, s := range scripts {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		uniq = append(uniq, s)
	}
	// syntax check by loading each file in a Lua state
	okList := make([]string, 0, len(uniq))
	for _, s := range uniq {
		L := lua.NewState(lua.Options{SkipOpenLibs: false})
		code := fmt.Sprintf(`local f, err = loadfile(%q); if not f then error(err) end`, s)
		err := L.DoString(code)
		if err != nil {
			e.logf("hooks: syntax check failed: %s: %v", s, err)
			L.Close()
			continue
		}
		L.Close()
		e.logf("hooks: loaded %s", s)
		okList = append(okList, s)
	}
	e.loaded = okList
}

// SetOptions updates directory, timeout and logger, without restarting the engine.
func (e *Engine) SetOptions(dir string, timeout time.Duration, logf func(string, ...any)) {
	if e == nil {
		return
	}
	if dir != "" {
		e.dir = dir
	}
	if timeout > 0 {
		e.timeout = timeout
	}
	if logf != nil {
		e.logf = logf
	}
}

// Decide runs the same candidate scripts as Fire, but allows scripts to block or modify the payload.
// A script can export a function `decide(event, payload)` that returns a table:
//
//	{ allow = true|false, reason = "...", patch = { k = v, ... } }
//
// If `allow` is false, the decision is to deny and the first reason encountered is returned.
// If `patch` is set, keys are shallow-merged into the payload and propagated to subsequent scripts.
// If no script returns a decision, the operation is allowed with the original payload.
func (e *Engine) Decide(ctx context.Context, event string, payload map[string]any) (bool, map[string]any, string) {
	if e == nil || e.dir == "" {
		return true, payload, ""
	}
	// copy payload to avoid mutating caller map directly
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		out[k] = v
	}
	// Gather candidates like Fire
	var candidates []string
	add := func(p string) {
		if p != "" {
			candidates = append(candidates, p)
		}
	}
	add(filepath.Join(e.dir, event+".lua"))
	if entries, err := os.ReadDir(filepath.Join(e.dir, event)); err == nil {
		for _, ent := range entries {
			if ent.IsDir() {
				continue
			}
			if strings.HasSuffix(ent.Name(), ".lua") {
				add(filepath.Join(e.dir, event, ent.Name()))
			}
		}
	}
	add(filepath.Join(e.dir, "all.lua"))

	// run in order; first explicit deny wins; patches accumulate
	for _, script := range candidates {
		if _, err := os.Stat(script); err != nil {
			continue
		}
		// enforce per-script timeout like Fire()
		c, cancel := context.WithTimeout(ctx, e.timeout)
		allowed, patched, reason, err := e.runDecide(c, script, event, out)
		cancel()
		if err != nil {
			e.logf("hooks: %s decide error: %v", script, err)
			// errors in hooks don't deny by default
			continue
		}
		if patched != nil {
			maps.Copy(out, patched)
		}
		if !allowed {
			if reason == "" {
				reason = "denied by hook"
			}
			return false, out, reason
		}
	}
	return true, out, ""
}

func (e *Engine) runDecide(ctx context.Context, script, event string, payload map[string]any) (bool, map[string]any, string, error) {
	L := lua.NewState(lua.Options{SkipOpenLibs: false})
	defer L.Close()
	L.SetContext(ctx)
	// log helper
	L.SetGlobal("log", L.NewFunction(func(L *lua.LState) int {
		n := L.GetTop()
		args := make([]any, 0, n)
		for i := 1; i <= n; i++ {
			args = append(args, L.Get(i).String())
		}
		e.logf("lua: %s", strings.Join(func(ss []any) []string {
			r := make([]string, len(ss))
			for i, v := range ss {
				r[i] = fmt.Sprint(v)
			}
			return r
		}(args), " "))
		return 0
	}))
	L.SetGlobal("event", lua.LString(event))
	L.SetGlobal("payload", toLuaValue(L, payload))
	if err := L.DoFile(script); err != nil {
		return true, nil, "", err
	}
	// call decide if present
	if fn := L.GetGlobal("decide"); fn.Type() == lua.LTFunction {
		// push fn and args
		L.Push(fn)
		L.Push(lua.LString(event))
		L.Push(toLuaValue(L, payload))
		if err := L.PCall(2, 1, nil); err != nil {
			return true, nil, "", err
		}
		ret := L.Get(-1)
		L.Pop(1)
		if tbl, ok := ret.(*lua.LTable); ok {
			allow := true
			reason := ""
			var patch map[string]any
			tbl.ForEach(func(k, v lua.LValue) {
				switch k.String() {
				case "allow":
					if v.Type() == lua.LTBool {
						allow = bool(v.(lua.LBool))
					}
				case "reason":
					reason = v.String()
				case "patch":
					if v.Type() == lua.LTTable {
						patch = fromLuaTable(v.(*lua.LTable))
					}
				}
			})
			return allow, patch, reason, nil
		}
	}
	// no decision
	return true, nil, "", nil
}

func (e *Engine) run(ctx context.Context, script, event string, payload map[string]any) error {
	L := lua.NewState(lua.Options{SkipOpenLibs: false})
	defer L.Close()
	// Set context for cancellation
	L.SetContext(ctx)
	// Expose print -> logf
	L.SetGlobal("log", L.NewFunction(func(L *lua.LState) int {
		n := L.GetTop()
		args := make([]any, 0, n)
		for i := 1; i <= n; i++ {
			args = append(args, L.Get(i).String())
		}
		e.logf("lua: %s", strings.Join(func(ss []any) []string {
			r := make([]string, len(ss))
			for i, v := range ss {
				r[i] = fmt.Sprint(v)
			}
			return r
		}(args), " "))
		return 0
	}))
	// Provide event and payload
	L.SetGlobal("event", lua.LString(event))
	L.SetGlobal("payload", toLuaValue(L, payload))
	return L.DoFile(script)
}

func toLuaValue(L *lua.LState, v any) lua.LValue {
	switch t := v.(type) {
	case nil:
		return lua.LNil
	case string:
		return lua.LString(t)
	case bool:
		if t {
			return lua.LTrue
		}
		return lua.LFalse
	case int:
		return lua.LNumber(t)
	case int64:
		return lua.LNumber(t)
	case float64:
		return lua.LNumber(t)
	case map[string]any:
		tbl := L.NewTable()
		for k, vv := range t {
			tbl.RawSetString(k, toLuaValue(L, vv))
		}
		return tbl
	case []any:
		tbl := L.NewTable()
		for _, it := range t {
			tbl.Append(toLuaValue(L, it))
		}
		return tbl
	default:
		return lua.LString(fmt.Sprint(v))
	}
}

func fromLuaTable(t *lua.LTable) map[string]any {
	out := map[string]any{}
	t.ForEach(func(k, v lua.LValue) {
		key := k.String()
		switch v.Type() {
		case lua.LTString:
			out[key] = v.String()
		case lua.LTNumber:
			out[key] = float64(v.(lua.LNumber))
		case lua.LTBool:
			out[key] = bool(v.(lua.LBool))
		case lua.LTTable:
			out[key] = fromLuaTable(v.(*lua.LTable))
		default:
			out[key] = v.String()
		}
	})
	return out
}

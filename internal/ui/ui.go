package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed static/*
var content embed.FS

// Handler serves the SPA from /ui. It falls back to index.html for unknown subpaths.
func Handler() http.Handler {
	// Strip the /ui prefix and serve files from embedded FS
	sub, _ := fs.Sub(content, "static")
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/ui")
		if p == "" || p == "/" {
			// Serve index.html via file server by rewriting path
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/index.html"
			fileServer.ServeHTTP(w, r2)
			return
		}
		// Normalize and ensure no path escape
		clean := path.Clean(strings.TrimPrefix(p, "/"))
		// If requesting a known asset (js/css), let file server handle
		if strings.HasSuffix(clean, ".js") || strings.HasSuffix(clean, ".css") || strings.HasSuffix(clean, ".ico") {
			r3 := r.Clone(r.Context())
			r3.URL.Path = "/" + clean
			fileServer.ServeHTTP(w, r3)
			return
		}
		// For any other subpath under /ui, serve index to support SPA routing
		r4 := r.Clone(r.Context())
		r4.URL.Path = "/index.html"
		fileServer.ServeHTTP(w, r4)
	})
}

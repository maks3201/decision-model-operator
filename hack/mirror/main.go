//go:build ignore

// Command mirror is a minimal, read-only Ollaya/Docker-v2 registry used by the
// e2e "registry mirror" spec (test/e2e/mirror_test.go). It serves a
// single on-disk Ollaya model store baked into the mirror image so the operator's
// resolve AND the prefetch Job's `ollaya pull` can run fully offline inside kind.
//
// It is built with `//go:build ignore` so the main module's build/vet/lint never
// compile it; the e2e mirror image Dockerfile builds it as a throwaway module.
//
// Store layout (OLLAYA_MODELS, produced by `ollaya pull <model>`):
//
//	manifests/<registry-host>/<namespace>/<model>/<tag>   (the v2 manifest bytes)
//	blobs/sha256-<hex>                                     (content-addressed blobs)
//
// Registry wire paths served:
//
//	GET /v2/<namespace>/<model>/manifests/<tag>   -> the manifest, with every
//	    layer/config "urls" field stripped. Stripping is REQUIRED for an offline
//	    mirror: the upstream Ollaya manifest pins each blob's download to a baked-in
//	    ollaya.dev / huggingface.co URL, and `ollaya pull` follows those URLs
//	    verbatim unless they are absent — in which case it fetches each blob from
//	    THIS registry base (verified against the mirror e2e). Stripping changes the
//	    manifest bytes, so the digest the operator records (sha256 of the served
//	    bytes) is the sha256 of this stripped manifest; the prefetch Job verifies
//	    against the same bytes, so resolve and pull stay consistent.
//	GET /v2/<namespace>/<model>/blobs/sha256:<hex> -> the blob, with Range support
//	    (http.ServeContent): `ollaya pull` downloads large blobs with parallel
//	    byte-range requests and rejects a server that ignores Range ("corrupt data:
//	    server ignored the range request").
//	GET /v2/                                        -> 200 (registry ping).
//
// The server also records every path it served to STDOUT so the spec can assert
// the Job really went through the mirror.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	store := envOr("MIRROR_STORE", "/store/models")
	addr := envOr("MIRROR_ADDR", ":8080")
	// MIRROR_FAULT, when set to an HTTP status (e.g. "500" or "429"), makes every
	// /v2/ request return that status instead of serving. It is read once at
	// startup; the e2e chaos spec injects a fault by `kubectl set env` + a rollout,
	// so the fault persists for the whole time the env is set (longer than the
	// engine's in-request retries and a reconcile), which is what makes the
	// transient-registry behaviour observable. Unset (default) serves normally.
	fault := faultStatus(os.Getenv("MIRROR_FAULT"))

	// The store keeps manifests under a single <registry-host> directory (the host
	// the model was pulled from at bake time). Discover it so the served repo path
	// is independent of that bake-time host.
	hostDirs, _ := filepath.Glob(filepath.Join(store, "manifests", "*"))
	if len(hostDirs) == 0 {
		log.Fatalf("mirror: no manifests found under %s/manifests/*", store)
	}
	host := filepath.Base(hostDirs[0])
	log.Printf("mirror: store=%s manifestHost=%s addr=%s fault=%d", store, host, addr, fault)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// /healthz is never faulted so the readiness probe can target it while a
		// fault is injected on /v2/ (the chaos spec sets MIRROR_FAULT and still needs
		// the Pod to report Ready for `kubectl rollout status`).
		if p == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if fault != 0 && strings.HasPrefix(p, "/v2") {
			w.WriteHeader(fault)
			log.Printf("mirror: %d (injected fault) %s", fault, p)
			return
		}
		switch {
		case strings.Contains(p, "/manifests/"):
			serveManifest(w, r, store, host)
		case strings.Contains(p, "/blobs/"):
			serveBlob(w, r, store)
		case p == "/v2/" || p == "/v2":
			w.WriteHeader(http.StatusOK)
			log.Printf("mirror: 200 %s", p)
		default:
			http.NotFound(w, r)
			log.Printf("mirror: 404 %s", p)
		}
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// faultStatus parses MIRROR_FAULT into an HTTP status to force on /v2/ requests,
// or 0 when unset/invalid (serve normally). Only 5xx and 429 are accepted — the
// statuses the engine treats as retryable/transient.
func faultStatus(v string) int {
	switch v {
	case "429":
		return http.StatusTooManyRequests
	case "500":
		return http.StatusInternalServerError
	case "502":
		return http.StatusBadGateway
	case "503":
		return http.StatusServiceUnavailable
	default:
		return 0
	}
}

func serveManifest(w http.ResponseWriter, r *http.Request, store, host string) {
	p := r.URL.Path
	idx := strings.Index(p, "/manifests/")
	repo := strings.TrimPrefix(p[:idx], "/v2/")
	tag := p[idx+len("/manifests/"):]
	raw, err := os.ReadFile(filepath.Join(store, "manifests", host, repo, tag)) // #nosec G304 - fixed store root
	if err != nil {
		http.NotFound(w, r)
		log.Printf("mirror: 404 manifest %s/%s (%v)", repo, tag, err)
		return
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		http.Error(w, "bad manifest", http.StatusInternalServerError)
		return
	}
	// Strip "urls" so `ollaya pull` fetches blobs from this registry, not upstream.
	if c, ok := m["config"].(map[string]any); ok {
		delete(c, "urls")
	}
	if layers, ok := m["layers"].([]any); ok {
		for _, l := range layers {
			if lm, ok := l.(map[string]any); ok {
				delete(lm, "urls")
			}
		}
	}
	out, err := json.Marshal(m)
	if err != nil {
		http.Error(w, "re-marshal manifest", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	_, _ = w.Write(out)
	log.Printf("mirror: 200 manifest %s/%s (%d bytes, urls stripped)", repo, tag, len(out))
}

func serveBlob(w http.ResponseWriter, r *http.Request, store string) {
	p := r.URL.Path
	idx := strings.Index(p, "/blobs/")
	// Wire digests use "sha256:<hex>"; the on-disk store uses "sha256-<hex>".
	name := strings.ReplaceAll(p[idx+len("/blobs/"):], "sha256:", "sha256-")
	if strings.Contains(name, "/") || strings.Contains(name, "..") {
		http.Error(w, "bad blob name", http.StatusBadRequest)
		return
	}
	f, err := os.Open(filepath.Join(store, "blobs", name)) // #nosec G304 - sanitized, fixed store root
	if err != nil {
		http.NotFound(w, r)
		log.Printf("mirror: 404 blob %s (%v)", name, err)
		return
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		http.Error(w, "stat blob", http.StatusInternalServerError)
		return
	}
	// http.ServeContent handles Range requests (parallel chunked pull).
	http.ServeContent(w, r, name, time.Time{}, f)
	log.Printf("mirror: 200 blob %s (%d bytes, range=%q)", name, fi.Size(), r.Header.Get("Range"))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

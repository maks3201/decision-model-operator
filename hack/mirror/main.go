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
//	GET /v2/<namespace>/<model>/manifests/sha256:<hex> -> the manifest whose SERVED
//	    (urls-stripped) bytes hash to <hex>, independent of which tag it was baked
//	    under. Ollaya 0.13.0 pulls a pinned revision by digest (spike 011); the
//	    prefetch Job on 0.13.0 pulls <model>@sha256:<recorded-digest>, so the mirror
//	    must answer the digest route with the exact bytes the operator recorded.
//	    An unknown digest is a 404, as for an unknown tag.
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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// errBadManifest / errReMarshal are returned by strippedManifest for a file that
// is not a parseable schemaVersion-2 manifest, or that fails to re-marshal.
var (
	errBadManifest = errors.New("bad manifest")
	errReMarshal   = errors.New("re-marshal manifest")
)

func main() {
	store := envOr("MIRROR_STORE", "/store/models")
	addr := envOr("MIRROR_ADDR", ":8080")
	// MIRROR_FAULT selects a fault injected on every /v2/ request, read once at
	// startup; the e2e specs inject a fault by `kubectl set env` + a rollout, so the
	// fault persists for the whole time the env is set (longer than the engine's
	// in-request retries and a reconcile), which is what makes the transient-registry
	// behaviour observable. Values:
	//   "429"/"500"/"502"/"503" - force that HTTP status (retryable/transient).
	//   "reset"                  - accept the connection then abruptly close it so the
	//                              client sees a TCP reset / EOF (a transport error, not
	//                              an HTTP status): exercises the resolver/pull transport
	//                              path rather than a status classification.
	// Unset (default) serves normally. A DNS failure is NOT a mirror mode: it is
	// driven by pointing the operator's registry at a name that does not resolve.
	faultEnv := os.Getenv("MIRROR_FAULT")
	fault := faultStatus(faultEnv)
	faultReset := faultEnv == "reset"

	// MIRROR_TAG_MOVED, when "1"/"true", makes every tag request (not a digest
	// request) answer with a byte-different-but-valid manifest: the baked manifest
	// with an extra top-level field. It simulates an upstream tag that was
	// repackaged (a new revision) while the digest route still serves the original
	// bytes. The e2e flips it at test time with `kubectl set env` + a rollout, the
	// same mechanism as MIRROR_FAULT, so a store recovery after the flip must pull
	// the recorded revision by digest, not whatever the tag now points at.
	tagMoved := os.Getenv("MIRROR_TAG_MOVED") == "1" || os.Getenv("MIRROR_TAG_MOVED") == "true"

	// The store keeps manifests under a single <registry-host> directory (the host
	// the model was pulled from at bake time). Discover it so the served repo path
	// is independent of that bake-time host.
	hostDirs, _ := filepath.Glob(filepath.Join(store, "manifests", "*"))
	if len(hostDirs) == 0 {
		log.Fatalf("mirror: no manifests found under %s/manifests/*", store)
	}
	host := filepath.Base(hostDirs[0])
	// Index every baked manifest by the sha256 of the bytes we SERVE for it (urls
	// stripped), so a digest request (Ollaya 0.13.0 pulls a pinned revision by
	// digest) is answered with the exact bytes the operator recorded. Keyed by bare
	// hex, value is the on-disk manifest file path.
	digestIndex := buildDigestIndex(store)
	log.Printf("mirror: store=%s manifestHost=%s addr=%s fault=%d reset=%v tagMoved=%v digests=%d",
		store, host, addr, fault, faultReset, tagMoved, len(digestIndex))

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
		if faultReset && strings.HasPrefix(p, "/v2") {
			// Abruptly close the underlying connection so the client sees a TCP
			// reset / EOF (a transport error, not an HTTP response). http.Hijacker
			// gives us the raw conn; closing it without a response is the reset.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					log.Printf("mirror: reset (injected fault) %s", p)
					return
				}
			}
			// Fallback if hijack is unavailable: drop with a 502 so the client still
			// sees a transient failure rather than a served manifest.
			w.WriteHeader(http.StatusBadGateway)
			log.Printf("mirror: reset-fallback 502 %s", p)
			return
		}
		if fault != 0 && strings.HasPrefix(p, "/v2") {
			w.WriteHeader(fault)
			log.Printf("mirror: %d (injected fault) %s", fault, p)
			return
		}
		switch {
		case strings.Contains(p, "/manifests/sha256:"):
			serveManifestByDigest(w, r, digestIndex)
		case strings.Contains(p, "/manifests/"):
			serveManifest(w, r, store, host, tagMoved)
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

func serveManifest(w http.ResponseWriter, r *http.Request, store, host string, tagMoved bool) {
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
	out, err := strippedManifest(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// MIRROR_TAG_MOVED simulates an upstream repackage: the tag now serves a
	// byte-different (still valid) manifest, so a tag pull would get the new
	// revision. A digest pull is unaffected (serveManifestByDigest serves the
	// original bytes). The marker is an added top-level field, which changes the
	// bytes and therefore the sha256 the client would record for the tag.
	if tagMoved {
		var m map[string]any
		if err := json.Unmarshal(out, &m); err == nil {
			m["ollayaMirrorMoved"] = "true"
			if moved, err := json.Marshal(m); err == nil {
				out = moved
			}
		}
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		_, _ = w.Write(out)
		log.Printf("mirror: 200 manifest %s/%s (%d bytes, urls stripped, TAG MOVED)", repo, tag, len(out))
		return
	}
	w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	_, _ = w.Write(out)
	log.Printf("mirror: 200 manifest %s/%s (%d bytes, urls stripped)", repo, tag, len(out))
}

// serveManifestByDigest answers GET .../manifests/sha256:<hex> with the baked
// manifest whose SERVED (urls-stripped) bytes hash to <hex>. Ollaya 0.13.0 pulls a
// pinned revision by digest (spike 011), so the digest route must return the exact
// bytes the operator recorded, regardless of where the tag now points. An unknown
// digest is a 404, matching an unknown tag and the real registry's behaviour.
func serveManifestByDigest(w http.ResponseWriter, r *http.Request, index map[string][]byte) {
	p := r.URL.Path
	i := strings.Index(p, "sha256:")
	hexDigest := p[i+len("sha256:"):]
	out, ok := index[hexDigest]
	if !ok {
		http.NotFound(w, r)
		log.Printf("mirror: 404 manifest by digest sha256:%s (not baked)", hexDigest)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	_, _ = w.Write(out)
	log.Printf("mirror: 200 manifest by digest sha256:%s (%d bytes)", hexDigest, len(out))
}

// buildDigestIndex walks every baked manifest file and maps the sha256 of its
// SERVED bytes (urls stripped) to those bytes. The served bytes, not the on-disk
// bytes, are what the operator records as the digest, so the index must key on the
// same stripping serveManifest applies.
func buildDigestIndex(store string) map[string][]byte {
	index := make(map[string][]byte)
	root := filepath.Join(store, "manifests")
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path) // #nosec G304 - walking a fixed store root
		if err != nil {
			return nil
		}
		out, err := strippedManifest(raw)
		if err != nil {
			return nil // not a manifest (e.g. a stray file); skip
		}
		sum := sha256.Sum256(out)
		index[hex.EncodeToString(sum[:])] = out
		return nil
	})
	return index
}

// strippedManifest parses a baked manifest and removes every config/layer "urls"
// field so `ollaya pull` fetches blobs from this registry, not upstream. The
// returned bytes are what the mirror serves and what the operator records as the
// digest. It is the single source of the stripping both the tag and digest routes
// use, so their digests stay consistent.
func strippedManifest(raw []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, errBadManifest
	}
	if _, ok := m["schemaVersion"]; !ok {
		return nil, errBadManifest
	}
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
		return nil, errReMarshal
	}
	return out, nil
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

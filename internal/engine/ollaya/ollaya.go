/*
Copyright 2026 maks3201.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ollaya implements the engine.Engine contract for the Ollaya runtime.
package ollaya

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/maks3201/decision-model-operator/internal/engine"
)

const (
	// defaultRegistryURL is the base of the Ollaya model registry.
	defaultRegistryURL = "https://ollaya.dev"
	// servicePort is the port the Ollaya runtime listens on.
	servicePort int32 = 11435
	// engineName matches DecisionModel spec.engine.
	engineName = "ollaya"

	// Per-method default deadlines, applied only when the caller's context has
	// no deadline of its own. Warmup is generous because loading a large model
	// on CPU can take minutes (matches OLLAYA_LOAD_TIMEOUT's default).
	defaultResolveTimeout = 10 * time.Second
	defaultInspectTimeout = 10 * time.Second
	defaultWarmupTimeout  = 5 * time.Minute
	defaultDecideTimeout  = 30 * time.Second

	// Transport-level timeouts for the default client. These bound connection
	// setup and the wait for response headers, but not the total request: the
	// per-method deadline (or the caller's ctx) bounds the body/streaming read.
	dialTimeout           = 10 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 30 * time.Second
	expectContinueTimeout = 1 * time.Second
	idleConnTimeout       = 90 * time.Second
)

// methodTimeouts holds the per-method default deadlines. It is a field so tests
// can shorten the values without waiting real seconds.
type methodTimeouts struct {
	resolve time.Duration
	inspect time.Duration
	warmup  time.Duration
	decide  time.Duration
}

// Engine is the Ollaya implementation of engine.Engine.
type Engine struct {
	registryURL string
	// hfEndpoint, when non-empty, is the base URL Ollaya downloads model weights
	// from (Hugging Face or a mirror/enterprise endpoint). It is passed to the
	// prefetch Job as OLLAYA_HF_ENDPOINT; it is distinct from registryURL, which
	// serves manifests. Empty means the runtime default (huggingface.co).
	hfEndpoint string
	// httpClient talks to the model registry (Resolve). It honours the proxy
	// environment (HTTP_PROXY/HTTPS_PROXY/NO_PROXY) so pulls work in proxied
	// clusters.
	httpClient *http.Client
	// runtimeClient talks to serving Pods by Pod IP (Inspect, Warmup, Decide).
	// It is deliberately proxy-less (Proxy: nil): these are in-cluster calls that
	// carry the Authorization: Bearer <api key> header, and must never be routed
	// through a corporate proxy even if the Pod CIDR is missing from NO_PROXY.
	runtimeClient *http.Client
	timeouts      methodTimeouts
}

// Option configures an Engine.
type Option func(*Engine)

// WithRegistryURL overrides the registry base URL (default https://ollaya.dev).
func WithRegistryURL(url string) Option {
	return func(e *Engine) {
		e.registryURL = strings.TrimRight(url, "/")
	}
}

// WithHFEndpoint sets the base URL Ollaya downloads model weights from (a
// Hugging Face mirror or an enterprise endpoint); empty leaves the runtime
// default (huggingface.co). When set, PrefetchJobSpec adds
// OLLAYA_HF_ENDPOINT=<url> to the prefetch Job; serving Pods never get it (they
// do not download). Like WithRegistryURL this only stores the value (trimmed):
// scheme policy (https:// unless insecure registries are allowed) is enforced by
// the manager, consistent with how the registry URL is validated upstream, so
// the engine does not raise here.
func WithHFEndpoint(url string) Option {
	return func(e *Engine) {
		e.hfEndpoint = strings.TrimRight(url, "/")
	}
}

// WithHTTPClient overrides BOTH HTTP clients (registry and runtime) entirely
// (default: clients with no global Timeout, using a transport with sane
// dial/TLS/header timeouts; the registry client honours the proxy env, the
// runtime client does not). Injecting one client here opts out of the
// registry/runtime proxy split, so use it only in tests or when you have a
// single trusted destination.
func WithHTTPClient(c *http.Client) Option {
	return func(e *Engine) {
		if c != nil {
			e.httpClient = c
			e.runtimeClient = c
		}
	}
}

// WithRuntimeHTTPClient overrides only the runtime client (Inspect/Warmup/
// Decide, which call Pod IPs with the API key). The registry client is left
// untouched. Used by tests that need to assert the runtime client's proxy
// behaviour independently of the registry client.
func WithRuntimeHTTPClient(c *http.Client) Option {
	return func(e *Engine) {
		if c != nil {
			e.runtimeClient = c
		}
	}
}

// defaultHTTPClient returns the registry client: no global Timeout so long calls
// are bounded by the request context, and the proxy env is honoured so pulls
// work behind a corporate proxy. Connection setup and header wait are bounded. A
// CheckRedirect refuses a redirect that leaves the original scheme+host+port (or
// downgrades https->http), so a registry cannot bounce the client to a host the
// operator's allow-list never approved .
func defaultHTTPClient() *http.Client {
	return &http.Client{Transport: newTransport(true), CheckRedirect: refuseCrossOriginRedirect}
}

// refuseCrossOriginRedirect is an http.Client.CheckRedirect that allows a
// redirect only when it stays on the same scheme, host and port as the request
// that triggered it; any change of origin (including an https->http downgrade or
// a different port) is refused. via holds the chain so far, most recent last; we
// compare the new request against the immediately preceding one.
func refuseCrossOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	prev := via[len(via)-1].URL
	next := req.URL
	if !sameOrigin(prev, next) {
		return fmt.Errorf("ollaya: refusing cross-origin registry redirect from %s://%s to %s://%s",
			prev.Scheme, prev.Host, next.Scheme, next.Host)
	}
	// Defuse the standard library's own 10-redirect cap being hit silently.
	if len(via) >= 10 {
		return fmt.Errorf("ollaya: too many registry redirects")
	}
	return nil
}

// sameOrigin compares scheme + hostname + effective port of two URLs. The
// default port is filled from the scheme so "https://h" and "https://h:443" are
// the same origin, but "https://h" and "http://h" (or a different port) are not.
func sameOrigin(a, b *neturl.URL) bool {
	return a.Scheme == b.Scheme &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		effectivePort(a) == effectivePort(b)
}

// effectivePort returns the URL's explicit port or the scheme default.
func effectivePort(u *neturl.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch u.Scheme {
	case schemeHTTPS:
		return defaultPortTLS
	case schemeHTTP:
		return defaultPortHTTP
	default:
		return ""
	}
}

// defaultRuntimeClient returns the client used for in-cluster calls to serving
// Pods (Inspect/Warmup/Decide). It is identical to the registry client except
// that it has NO proxy (Proxy: nil): those calls carry the API key and target
// Pod IPs, so they must go direct and never traverse a proxy, even when the Pod
// CIDR is absent from NO_PROXY. It also refuses ALL redirects: a serving Pod has
// no legitimate reason to 3xx an in-cluster API call, and following one would let
// a compromised or buggy runtime point the operator (and the API key) at an
// arbitrary address (SSRF), or answer a forged /api/ps from elsewhere and flip
// the readiness gate.
func defaultRuntimeClient() *http.Client {
	return &http.Client{Transport: newTransport(false), CheckRedirect: refuseAllRedirects}
}

// refuseAllRedirects is an http.Client.CheckRedirect that refuses every redirect
// (used by the runtime client). The error names the status and the target host
// so an operator can see where the runtime tried to send us; it never includes
// the request headers (the API key must not leak into a log line).
func refuseAllRedirects(req *http.Request, via []*http.Request) error {
	status := 0
	// The stdlib sets req.Response to the response that triggered this redirect.
	if req.Response != nil {
		status = req.Response.StatusCode
	}
	return fmt.Errorf("ollaya: refusing runtime redirect (status %d) to host %s", status, req.URL.Host)
}

// newTransport builds the shared transport. When useProxy is true the proxy env
// is honoured (http.ProxyFromEnvironment); when false Proxy stays nil (direct).
// Dial/TLS/header timeouts are identical for both clients.
func newTransport(useProxy bool) *http.Transport {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
	}
	if useProxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	return tr
}

// New builds an Engine with the given options.
func New(opts ...Option) *Engine {
	e := &Engine{
		registryURL:   defaultRegistryURL,
		httpClient:    defaultHTTPClient(),
		runtimeClient: defaultRuntimeClient(),
		timeouts: methodTimeouts{
			resolve: defaultResolveTimeout,
			inspect: defaultInspectTimeout,
			warmup:  defaultWarmupTimeout,
			decide:  defaultDecideTimeout,
		},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// withTimeout applies d as a deadline only when ctx has none of its own. It
// returns a cancel func the caller must defer.
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// Compile-time check that Engine satisfies the contract.
var _ engine.Engine = (*Engine)(nil)

// Compile-time check that Engine implements the optional Decider capability.
var _ engine.Decider = (*Engine)(nil)

// Compile-time check that Engine implements the optional RegistryHoster capability.
var _ engine.RegistryHoster = (*Engine)(nil)

// Compile-time check that Engine implements the optional RuntimeVersioner capability.
var _ engine.RuntimeVersioner = (*Engine)(nil)

// Compile-time check that Engine implements the optional RuntimeImagePinner capability.
var _ engine.RuntimeImagePinner = (*Engine)(nil)

// Name returns the engine name.
func (e *Engine) Name() string { return engineName }

// ServicePort returns the port the runtime listens on.
func (e *Engine) ServicePort() int32 { return servicePort }

// CanonicalName delegates to the package-level CanonicalName so callers holding
// an *Engine (e.g. via an interface assertion) can canonicalise names without
// importing the package function directly. See CanonicalName for the rules.
func (e *Engine) CanonicalName(name string) (string, error) {
	return CanonicalName(name)
}

// namePartRE matches a namespace, model or tag part: 1-80 chars,
// [A-Za-z0-9_][A-Za-z0-9_.-]* (Ollaya docs/api.md §3).
var namePartRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,79}$`)

// hostLabelRE matches one DNS-ish label of a registry host: lowercase
// alphanumerics and hyphens, not empty, not starting/ending with a hyphen is
// not enforced (registries are lenient), but empty labels and ".." are rejected
// by validateHostSegment below.
var hostLabelRE = regexp.MustCompile(`^[a-z0-9-]+$`)

// portRE matches an optional numeric port (1-5 digits).
var portRE = regexp.MustCompile(`^[0-9]{1,5}$`)

// validateHostSegment checks a registry host (scheme already stripped): a
// hostname of dot-separated [a-z0-9-] labels with an optional :port, no empty
// labels and no "..". Returns an error otherwise.
func validateHostSegment(host string) error {
	if host == "" {
		return fmt.Errorf("empty host")
	}
	if strings.Contains(host, "..") {
		return fmt.Errorf("host %q contains an empty label", host)
	}
	hostname := host
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		hostname = host[:i]
		port := host[i+1:]
		if !portRE.MatchString(port) {
			return fmt.Errorf("host %q has an invalid port", host)
		}
	}
	if hostname == "" {
		return fmt.Errorf("host %q has no hostname", host)
	}
	for _, label := range strings.Split(hostname, ".") {
		if label == "" || !hostLabelRE.MatchString(label) {
			return fmt.Errorf("host %q has an invalid label %q", host, label)
		}
	}
	return nil
}

// parsedName is a fully parsed and normalized Ollaya model name.
type parsedName struct {
	// host is the registry host as written (may include scheme and port), or
	// "" when the name carried no host (use the engine default registry).
	host      string
	namespace string
	model     string
	tag       string
}

// canonical returns the shortest canonical name with the tag always shown,
// per §3 (host and default "library" namespace omitted when absent).
func (p parsedName) canonical() string {
	var b strings.Builder
	if p.host != "" {
		b.WriteString(p.host)
		b.WriteByte('/')
	}
	if p.namespace != "" && p.namespace != defaultNamespace {
		b.WriteString(p.namespace)
		b.WriteByte('/')
	} else if p.host != "" {
		// With an explicit host, keep the namespace so the name round-trips
		// (e.g. localhost:8080/library/laya:en stays as is).
		b.WriteString(p.namespace)
		b.WriteByte('/')
	}
	b.WriteString(p.model)
	b.WriteByte(':')
	b.WriteString(p.tag)
	return b.String()
}

// registryBase returns the registry base URL for this name: the name's host
// (defaulting the scheme to https) when present, else the engine default.
func (p parsedName) registryBase(def string) string {
	if p.host == "" {
		return def
	}
	h := p.host
	if !strings.HasPrefix(h, "http://") && !strings.HasPrefix(h, "https://") {
		h = "https://" + h
	}
	return strings.TrimRight(h, "/")
}

const defaultNamespace = "library"

const (
	schemeHTTP      = "http"
	schemeHTTPS     = "https"
	defaultPortHTTP = "80"
	defaultPortTLS  = "443"
)

// defaultManifestHost is the on-disk manifest host directory used when a name
// carries no explicit host (matches the default registry ollaya.dev).
const defaultManifestHost = "ollaya.dev"

// manifestDiskPath returns the path of the manifest file inside the model store
// relative to OLLAYA_MODELS: manifests/<host>/<namespace>/<model>/<tag>. The
// host directory is the registry authority with the scheme stripped and the
// port separator ':' replaced by '_', exactly how the ollaya CLI lays out the
// store (verified on 0.7.3 and 0.10.0: a pull with
// OLLAYA_REGISTRY=http://mirror:8080 writes manifests/mirror_8080/...). A
// host-less name uses defaultHost, which the engine derives from its configured
// registry so the Job's digest verification reads the file `ollaya pull`
// actually wrote under --ollaya-registry.
func (p parsedName) manifestDiskPath(defaultHost string) string {
	host := manifestHostDir(p.host)
	if host == "" {
		host = defaultHost
	}
	if host == "" {
		host = defaultManifestHost
	}
	ns := p.namespace
	if ns == "" {
		ns = defaultNamespace
	}
	return fmt.Sprintf("manifests/%s/%s/%s/%s", host, ns, p.model, p.tag)
}

// defaultManifestHostDir returns the on-disk store host directory for a
// host-less model name, derived from the engine's configured registry (the same
// authority `ollaya pull` writes under when OLLAYA_REGISTRY is set).
// For the default registry it is "ollaya.dev".
func (e *Engine) defaultManifestHostDir() string {
	h := manifestHostDir(e.registryURL)
	if h == "" {
		return defaultManifestHost
	}
	return h
}

// manifestHostDir turns a registry authority (optionally with an http(s)://
// scheme) into the on-disk store directory name the ollaya CLI uses: scheme
// stripped and the port separator ':' replaced by '_' (e.g.
// "http://mirror:8080" -> "mirror_8080", "ollaya.dev" -> "ollaya.dev"). An empty
// input returns "". Verified in the image.
func manifestHostDir(host string) string {
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimRight(host, "/")
	return strings.ReplaceAll(host, ":", "_")
}

// looksLikeHost reports whether the first path segment is a registry host
// rather than a namespace, following Ollama's heuristic: it contains a ".",
// a ":" (port), or is exactly "localhost" (optionally with a scheme).
func looksLikeHost(seg string) bool {
	s := strings.TrimPrefix(strings.TrimPrefix(seg, "https://"), "http://")
	if s == "localhost" || strings.HasPrefix(s, "localhost:") {
		return true
	}
	return strings.Contains(s, ".") || strings.Contains(s, ":")
}

// parseName parses [host/][namespace/]model[:tag] per docs/api.md §3, applying
// normalization (trim, lowercase). Invalid names return an error without any
// network call. The tag is split only from the last path segment.
func parseName(name string) (parsedName, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return parsedName{}, fmt.Errorf("ollaya: empty model name")
	}
	lower := strings.ToLower(trimmed)

	// A leading http:// or https:// scheme belongs to the host; strip it before
	// splitting on "/" and re-attach it to the host segment afterwards.
	scheme := ""
	switch {
	case strings.HasPrefix(lower, "https://"):
		scheme = "https://"
	case strings.HasPrefix(lower, "http://"):
		scheme = "http://" // NOSONAR: only with --allow-insecure-registries
	}
	rest := strings.TrimPrefix(lower, scheme)

	segs := strings.Split(rest, "/")

	var p parsedName
	// A leading host is only possible when there are 2+ path segments and the
	// first segment looks like a host (has ".", ":" or is localhost), or when an
	// explicit scheme was given.
	if len(segs) >= 2 && (scheme != "" || looksLikeHost(segs[0])) {
		p.host = scheme + segs[0]
		if err := validateHostSegment(segs[0]); err != nil {
			return parsedName{}, fmt.Errorf("ollaya: model name %q: %w", name, err)
		}
		segs = segs[1:]
	} else if scheme != "" {
		return parsedName{}, fmt.Errorf("ollaya: model name %q has a scheme but no host segment", name)
	}

	switch len(segs) {
	case 1:
		p.namespace = defaultNamespace
	case 2:
		p.namespace = segs[0]
		segs = segs[1:]
	default:
		return parsedName{}, fmt.Errorf("ollaya: model name %q has too many path segments", name)
	}

	// Split the tag from the final segment only.
	modelSeg := segs[0]
	p.tag = "latest"
	if i := strings.LastIndex(modelSeg, ":"); i >= 0 {
		p.model = modelSeg[:i]
		p.tag = modelSeg[i+1:]
	} else {
		p.model = modelSeg
	}

	if !namePartRE.MatchString(p.namespace) {
		return parsedName{}, fmt.Errorf("ollaya: invalid namespace in model name %q", name)
	}
	// With the default registry, a namespace that looks like a registry host
	// (contains ".", ":" or is "localhost") is rejected: CanonicalName drops the
	// default host, so the namespace would be read back as a host
	// (e.g. "ollaya.dev/0./0" -> "0./0:latest", where "0." then reads as a host).
	// A non-default host is kept in the canonical form, so it stays unambiguous.
	if hostIsDefault(p.host) && p.namespace != defaultNamespace && looksLikeHost(p.namespace) {
		return parsedName{}, fmt.Errorf("ollaya: namespace %q in model name %q must not look like a host", p.namespace, name)
	}
	if !namePartRE.MatchString(p.model) {
		return parsedName{}, fmt.Errorf("ollaya: invalid model in model name %q", name)
	}
	if !namePartRE.MatchString(p.tag) {
		return parsedName{}, fmt.Errorf("ollaya: invalid tag in model name %q", name)
	}
	return p, nil
}

// CanonicalName parses and normalizes an Ollaya model name and returns its
// canonical form per docs/api.md §3: trimmed, lowercased, with the tag always
// shown and the default "library" namespace / default host omitted. Examples:
//
//	"Laya:EN"                      -> "laya:en"
//	"laya"                         -> "laya:latest"
//	"ollaya.dev/library/laya:en"   -> "laya:en"
//	"acme/triage"                  -> "acme/triage:latest"
//
// The controller uses it to compare a user-written spec.model against the
// (already canonical) names Ollaya reports in /api/ps. Invalid names return an
// error. Note: an explicit non-default host is preserved (it changes identity),
// so "ollaya.dev/..." canonicalises to the short form only because ollaya.dev
// is the default registry host.
func CanonicalName(name string) (string, error) {
	p, err := parseName(name)
	if err != nil {
		return "", err
	}
	// A host equal to the default registry (ollaya.dev, any scheme/port-less
	// form) is dropped so it matches the short canonical names Ollaya reports.
	if hostIsDefault(p.host) {
		p.host = ""
	}
	return p.canonical(), nil
}

// hostIsDefault reports whether host (possibly with scheme) is the default
// registry host ollaya.dev, which the canonical short form omits. Only the exact
// default authority counts: the hostname must be ollaya.dev AND the port must be
// absent or the scheme's default (443 for https/none, 80 for http). A non-default
// port such as "ollaya.dev:5000" is a DIFFERENT registry and is NOT the default
// (stripping the port let it inherit the built-in resource
// defaults and the short canonical form).
func hostIsDefault(host string) bool {
	if host == "" {
		return true
	}
	host = strings.ToLower(strings.TrimSpace(host))
	scheme := schemeHTTPS
	switch {
	case strings.HasPrefix(host, "https://"):
		host = strings.TrimPrefix(host, "https://")
	case strings.HasPrefix(host, "http://"):
		scheme = schemeHTTP
		host = strings.TrimPrefix(host, "http://")
	}
	host = strings.TrimRight(host, "/")
	hostname := host
	port := ""
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		hostname = host[:i]
		port = host[i+1:]
	}
	if hostname != defaultManifestHost {
		return false
	}
	// Accept only an absent port or the scheme's default port.
	defPort := defaultPortTLS
	if scheme == schemeHTTP {
		defPort = defaultPortHTTP
	}
	return port == "" || port == defPort
}

// RegistryHost parses a model name and returns the registry host it targets
// (lowercase hostname with optional :port, scheme stripped) and whether plain
// http:// is used. It uses exactly the parser Resolve uses. For a name without
// a host it returns the host of the engine's configured registry
// (WithRegistryURL), not a hard-coded default, with insecure=true when that URL
// is http://. Invalid names return an error. Implements engine.RegistryHoster.
func (e *Engine) RegistryHost(name string) (host string, insecure bool, err error) {
	p, err := parseName(name)
	if err != nil {
		return "", false, err
	}
	if p.host == "" {
		// Fall back to the engine's configured registry, not a hard-coded host.
		h, ins := hostFromRegistryURL(e.registryURL)
		return h, ins, nil
	}
	h, ins := stripSchemeHost(p.host)
	return h, ins, nil
}

// stripSchemeHost removes an http(s):// prefix from a host segment and reports
// whether it was plain http://.
func stripSchemeHost(h string) (host string, insecure bool) {
	switch {
	case strings.HasPrefix(h, "http://"):
		return strings.TrimPrefix(h, "http://"), true
	case strings.HasPrefix(h, "https://"):
		return strings.TrimPrefix(h, "https://"), false
	default:
		return h, false
	}
}

// hostFromRegistryURL extracts the lowercase host[:port] and insecure flag from
// a registry base URL such as "https://ollaya.dev" or "http://localhost:5000".
func hostFromRegistryURL(registryURL string) (host string, insecure bool) {
	h, insecure := stripSchemeHost(strings.ToLower(strings.TrimSpace(registryURL)))
	h = strings.TrimRight(h, "/")
	// Drop any trailing path (defensive; registryURL is normally scheme+host).
	if i := strings.IndexByte(h, '/'); i >= 0 {
		h = h[:i]
	}
	if h == "" {
		return defaultManifestHost, false
	}
	return h, insecure
}

// registryIsDefault reports whether registryURL is the default Ollaya registry
// over the default (https) transport, in which case the prefetch and serving
// Pods need no OLLAYA_REGISTRY env: `ollaya` already defaults there. The
// hostname must be ollaya.dev on https with no port or :443; an http:// base
// (plaintext) or a non-default port is NOT the default and must be forwarded.
func registryIsDefault(registryURL string) bool {
	registryURL = strings.TrimSpace(registryURL)
	if registryURL == "" {
		return true
	}
	// An explicit http:// base changes the transport, so it is never "default".
	if strings.HasPrefix(strings.ToLower(registryURL), "http://") {
		return false
	}
	return hostIsDefault(registryURL)
}

// ollayaManifest is the subset of the registry manifest we validate.
type ollayaManifest struct {
	SchemaVersion int `json:"schemaVersion"`
}

// maxBodyBytes caps how much of any response body we read (manifest, /api/ps,
// /api/decide). A larger body is treated as an error rather than read into
// memory unbounded.
const maxBodyBytes = 1 << 20 // 1 MiB

// retry policy: up to maxAttempts tries with these backoffs between them.
const maxAttempts = 3

// retryBackoffs is the wait before attempt i+1 (len == maxAttempts-1).
var retryBackoffs = []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}

// readCappedBody reads at most maxBodyBytes of resp.Body and errors if the body
// is larger than the cap.
func readCappedBody(resp *http.Response) ([]byte, error) {
	limited := io.LimitReader(resp.Body, maxBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBodyBytes {
		return nil, fmt.Errorf("ollaya: response body exceeds %d bytes", maxBodyBytes)
	}
	return body, nil
}

// retryableStatus reports whether an HTTP status warrants a retry: 429 and 5xx
// are transient; 404 and other 4xx are not.
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code >= 500
}

// retryAfter parses a Retry-After header (delta-seconds form only) into a wait
// duration, capped so a hostile header can't stall us. Returns 0 if absent/bad.
func retryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs < 0 {
		return 0
	}
	d := time.Duration(secs) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// sleepCtx waits for d or until ctx is done, whichever comes first. Returns
// ctx.Err() if the context finished first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// doRetry executes newReq (a fresh request per attempt) with the retry policy.
// It returns the last response (already status-checked by shouldRetry) or the
// last error. Retries happen on transport errors and retryableStatus responses;
// 404/4xx return immediately. The context is honoured between attempts and a
// Retry-After header is respected.
func (e *Engine) doRetry(ctx context.Context, newReq func() (*http.Request, error)) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := retryBackoffs[attempt-1]
			if err := sleepCtx(ctx, wait); err != nil {
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, err
			}
		}

		req, err := newReq()
		if err != nil {
			return nil, err
		}

		resp, err := e.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue // transport error: retry
		}

		if attempt < maxAttempts-1 && retryableStatus(resp.StatusCode) {
			// Honour Retry-After if present by folding it into the next backoff.
			if ra := retryAfter(resp); ra > 0 {
				_ = resp.Body.Close()
				if err := sleepCtx(ctx, ra); err != nil {
					return nil, err
				}
				// The fixed backoff still applies on the next loop; that's fine.
				lastErr = fmt.Errorf("ollaya: status %d", resp.StatusCode)
				continue
			}
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("ollaya: status %d", resp.StatusCode)
			continue
		}
		return resp, nil
	}
	return nil, lastErr
}

// Resolve turns a model name into a pinned ModelRef by fetching its manifest.
func (e *Engine) Resolve(ctx context.Context, name string) (engine.ModelRef, error) {
	p, err := parseName(name)
	if err != nil {
		return engine.ModelRef{}, err
	}
	ctx, cancel := withTimeout(ctx, e.timeouts.resolve)
	defer cancel()
	base := p.registryBase(e.registryURL)
	url := fmt.Sprintf("%s/v2/%s/%s/manifests/%s", base, p.namespace, p.model, p.tag)

	resp, err := e.doRetry(ctx, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	})
	if err != nil {
		return engine.ModelRef{}, fmt.Errorf("ollaya: resolve %q: %w", name, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Classify the status BEFORE reading the body: a 404 with a huge CDN/proxy
	// HTML error page must be ErrNotFound (permanent), not a "body too large"
	// transient error. Drain a bounded amount so the connection can be reused.
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		return engine.ModelRef{}, fmt.Errorf("ollaya: resolve %q: %w", name, engine.ErrNotFound)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		return engine.ModelRef{}, fmt.Errorf("ollaya: resolve %q: unexpected status %d", name, resp.StatusCode)
	}

	body, err := readCappedBody(resp)
	if err != nil {
		return engine.ModelRef{}, fmt.Errorf("ollaya: read manifest for %q: %w", name, err)
	}

	var m ollayaManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return engine.ModelRef{}, fmt.Errorf("ollaya: manifest for %q is not valid JSON: %w", name, err)
	}
	if m.SchemaVersion != 2 {
		return engine.ModelRef{}, fmt.Errorf("ollaya: manifest for %q has unsupported schemaVersion %d", name, m.SchemaVersion)
	}

	sum := sha256.Sum256(body)
	return engine.ModelRef{
		Name:     p.canonical(),
		Digest:   hex.EncodeToString(sum[:]),
		Manifest: body,
	}, nil
}

// psResponse is the /api/ps response shape.
type psResponse struct {
	Models []psModel `json:"models"`
}

type psModel struct {
	Name      string     `json:"name"`
	Digest    string     `json:"digest"`
	Device    string     `json:"device"`
	ExpiresAt *time.Time `json:"expires_at"`
	Details   struct {
		QuantizationLevel string `json:"quantization_level"`
	} `json:"details"`
}

// Inspect lists models loaded in the runtime at baseURL.
func (e *Engine) Inspect(ctx context.Context, baseURL, apiKey string) ([]engine.Loaded, error) {
	ctx, cancel := withTimeout(ctx, e.timeouts.inspect)
	defer cancel()
	url := strings.TrimRight(baseURL, "/") + "/api/ps"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("ollaya: build inspect request: %w", err)
	}
	setAuth(req, apiKey)

	resp, err := e.runtimeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollaya: inspect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readCappedBody(resp)
	if err != nil {
		return nil, fmt.Errorf("ollaya: read inspect body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ollaya: inspect: unexpected status %d", resp.StatusCode)
	}

	var ps psResponse
	if err := json.Unmarshal(body, &ps); err != nil {
		return nil, fmt.Errorf("ollaya: inspect: invalid JSON: %w", err)
	}

	loaded := make([]engine.Loaded, 0, len(ps.Models))
	for _, m := range ps.Models {
		loaded = append(loaded, engine.Loaded{
			Name:      m.Name,
			Digest:    strings.TrimPrefix(m.Digest, "sha256:"),
			Device:    normalizeDevice(m.Device),
			Precision: m.Details.QuantizationLevel,
			Pinned:    m.ExpiresAt == nil,
		})
	}
	return loaded, nil
}

// decideRequest is the /api/decide warmup payload.
type decideRequest struct {
	Model     string `json:"model"`
	KeepAlive int    `json:"keep_alive"`
}

// decideResponse is the subset of the /api/decide response we read.
type decideResponse struct {
	DoneReason string `json:"done_reason"`
	Error      string `json:"error"`
	Code       string `json:"code"`
}

// Warmup loads a model into memory and pins it (keep_alive=-1).
func (e *Engine) Warmup(ctx context.Context, baseURL, apiKey, model string) error {
	ctx, cancel := withTimeout(ctx, e.timeouts.warmup)
	defer cancel()
	url := strings.TrimRight(baseURL, "/") + "/api/decide"
	payload, err := json.Marshal(decideRequest{Model: model, KeepAlive: -1})
	if err != nil {
		return fmt.Errorf("ollaya: marshal warmup request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("ollaya: build warmup request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setAuth(req, apiKey)

	resp, err := e.runtimeClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollaya: warmup %q: %w", model, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readCappedBody(resp)
	if err != nil {
		return fmt.Errorf("ollaya: read warmup body: %w", err)
	}

	var dr decideResponse
	// Best-effort decode; body may be empty on some transports.
	_ = json.Unmarshal(body, &dr)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if dr.Code != "" {
			return fmt.Errorf("ollaya: warmup %q failed (status %d, code %s): %s", model, resp.StatusCode, dr.Code, dr.Error)
		}
		return fmt.Errorf("ollaya: warmup %q: unexpected status %d", model, resp.StatusCode)
	}

	if dr.DoneReason != "load" {
		return fmt.Errorf("ollaya: warmup %q: unexpected done_reason %q", model, dr.DoneReason)
	}
	return nil
}

// setAuth adds a bearer token header when apiKey is non-empty.
func setAuth(req *http.Request, apiKey string) {
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
}

// Runtime version defaults. The ghcr tags are plain MAJOR.MINOR.PATCH (no "v").
// DefaultImageCPU/CUDA are derived from DefaultRuntimeVersion so a version bump
// touches one place. 0.12.0 is drop-in compatible with the 0.10.0 facts in spike
// 006 (re-verified in spike 008: serve, RO store, pull, sha256(manifest)==/api/ps
// digest, warmup, /v1/systemone shape, prefetch classification, upgrade path and
// the device gate are all unchanged; the only additive /api/ps field,
// context_length, is not read by Inspect).
const (
	// DefaultRuntimeVersion is the Ollaya release this operator build defaults to
	// when a DecisionModel pins no version and sets no explicit image.
	DefaultRuntimeVersion = "0.12.0"
	// MinRuntimeVersion is the oldest release we have verified; older versions are
	// rejected by ValidateRuntimeVersion.
	MinRuntimeVersion = "0.7.3"

	imageRepo = "ghcr.io/ollaya-dev/ollaya"
)

// runtimeImageDigest holds the multi-arch OCI image index digests for a runtime
// version's CPU and CUDA images. The index digest (not a per-arch manifest) is
// used so amd64 and arm64 nodes both resolve the same reference. A tag plus this
// digest (repo:tag@sha256:...) pins the exact bytes: if upstream re-pushes the
// tag, the digest no longer matches and the pull fails loudly instead of running
// different bytes under one revision hash.
type runtimeImageDigest struct {
	// cpu is the sha256 of the CPU image's OCI index (empty if unknown).
	cpu string
	// cuda is the sha256 of the -cuda image's OCI index (empty if unknown).
	cuda string
}

// runtimeImageDigests pins the engine's known runtime versions to their image
// index digests. Only versions listed here render by digest; a version absent
// from the map (e.g. a user runtimeVersion newer than this build's table) renders
// by tag, and RuntimeImagePinned reports false for it so the controller can refuse
// it unless unpinned runtime images are explicitly allowed.
//
// Digests captured with `docker buildx imagetools inspect ghcr.io/ollaya-dev/ollaya:<tag>`
// (the top-level "Digest:" of the OCI image index), 2026-10-07:
//
//	0.7.3        index sha256:3e3ad48f93baa7d98d43a9655646b6264bd8322493393a6eb7a899b5d231b4a9  (linux/amd64 + linux/arm64)
//	0.7.3-cuda   index sha256:b35eedf0c0464455240820a32f755f58d53e68e99c0007c75c8c60fb2309c8d6  (linux/amd64 only)
//	0.10.0       index sha256:13fd0aad32f60cc2e5e7bb8007eed51194052dd75a066bf87f5dfbece4edbaa8  (linux/amd64 + linux/arm64)
//	0.10.0-cuda  index sha256:1e4d7708405b1c28e46bb7c2d79bebb250c05131bc899fc0bff54008d2f22938  (linux/amd64 only)
//	0.11.0       index sha256:5f96bc111bf6ca2af9691c0a33a6d04c61e16c4918fb002850f212cbc998de60  (linux/amd64 + linux/arm64)
//	0.11.0-cuda  index sha256:239045853b01fc919a33f5206b51c0e3a4d8a744bed3f94998a650589561baec  (linux/amd64 only)
//	0.12.0       index sha256:f79e865fda7af45aa66617b85fe16b08688d27f83a3137a21c75d3b137d8cf39  (linux/amd64 + linux/arm64)
//	0.12.0-cuda  index sha256:ee3c316db37b1dfc828bd8bf5db1d269c98e49ad4918cf2e18748292e1f178e7  (linux/amd64 only)
var runtimeImageDigests = map[string]runtimeImageDigest{
	"0.7.3": {
		cpu:  "sha256:3e3ad48f93baa7d98d43a9655646b6264bd8322493393a6eb7a899b5d231b4a9",
		cuda: "sha256:b35eedf0c0464455240820a32f755f58d53e68e99c0007c75c8c60fb2309c8d6",
	},
	"0.10.0": {
		cpu:  "sha256:13fd0aad32f60cc2e5e7bb8007eed51194052dd75a066bf87f5dfbece4edbaa8",
		cuda: "sha256:1e4d7708405b1c28e46bb7c2d79bebb250c05131bc899fc0bff54008d2f22938",
	},
	"0.11.0": {
		cpu:  "sha256:5f96bc111bf6ca2af9691c0a33a6d04c61e16c4918fb002850f212cbc998de60",
		cuda: "sha256:239045853b01fc919a33f5206b51c0e3a4d8a744bed3f94998a650589561baec",
	},
	"0.12.0": {
		cpu:  "sha256:f79e865fda7af45aa66617b85fe16b08688d27f83a3137a21c75d3b137d8cf39",
		cuda: "sha256:ee3c316db37b1dfc828bd8bf5db1d269c98e49ad4918cf2e18748292e1f178e7",
	},
}

// digestForVersion returns the known index digest for a version and device, or
// "" when the version is not in runtimeImageDigests (or the device's digest is
// unset).
func digestForVersion(version, device string) string {
	d, ok := runtimeImageDigests[version]
	if !ok {
		return ""
	}
	if device == engine.DeviceCUDA {
		return d.cuda
	}
	return d.cpu
}

// Exported runtime image defaults, derived from DefaultRuntimeVersion and pinned
// by the index digest from runtimeImageDigests (so the default always resolves to
// fixed bytes). If a future DefaultRuntimeVersion is set without a digest entry,
// these fall back to the bare tag and TestDefaultImagesArePinned fails to catch it.
var (
	// DefaultImageCPU is the CPU serving/prefetch image (digest-pinned).
	DefaultImageCPU = imageForVersion(DefaultRuntimeVersion, engine.DeviceCPU)
	// DefaultImageCUDA is the CUDA serving image (digest-pinned; still amd64-only).
	DefaultImageCUDA = imageForVersion(DefaultRuntimeVersion, engine.DeviceCUDA)
)

// runtimeVersionRE matches a plain MAJOR.MINOR.PATCH version (no "v" prefix, no
// pre-release/build suffix), matching the ghcr tag form.
var runtimeVersionRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

// InvalidRuntimeVersionError is returned by ValidateRuntimeVersion for a version
// the engine will not build an image from. It is a typed error so the controller
// can recognise it and surface a clear condition.
type InvalidRuntimeVersionError struct {
	Version string
	Reason  string
}

func (e *InvalidRuntimeVersionError) Error() string {
	return fmt.Sprintf("ollaya: invalid runtime version %q: %s", e.Version, e.Reason)
}

// ValidateRuntimeVersion checks that version is a plain MAJOR.MINOR.PATCH string
// (no "v", no suffix) and not older than MinRuntimeVersion. An empty string is
// valid and means "the engine default" (DefaultRuntimeVersion). Returns an
// *InvalidRuntimeVersionError otherwise.
func ValidateRuntimeVersion(version string) error {
	if version == "" {
		return nil // empty = default, resolved later
	}
	if !runtimeVersionRE.MatchString(version) {
		return &InvalidRuntimeVersionError{
			Version: version,
			Reason:  "must be MAJOR.MINOR.PATCH with no 'v' prefix or suffix",
		}
	}
	if compareVersions(version, MinRuntimeVersion) < 0 {
		return &InvalidRuntimeVersionError{
			Version: version,
			Reason:  "below the minimum supported version " + MinRuntimeVersion,
		}
	}
	return nil
}

// compareVersions compares two validated MAJOR.MINOR.PATCH strings numerically.
// Returns -1, 0 or 1. It assumes both match runtimeVersionRE (callers validate
// the input; MinRuntimeVersion is a constant that does).
func compareVersions(a, b string) int {
	ap := strings.SplitN(a, ".", 3)
	bp := strings.SplitN(b, ".", 3)
	for i := 0; i < 3; i++ {
		x, _ := strconv.Atoi(ap[i])
		y, _ := strconv.Atoi(bp[i])
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// resolvedRuntimeVersion returns the version an image will be built from: the
// engine default when p.RuntimeVersion is empty, else p.RuntimeVersion.
func resolvedRuntimeVersion(p engine.Params) string {
	if p.RuntimeVersion == "" {
		return DefaultRuntimeVersion
	}
	return p.RuntimeVersion
}

// ResolvedRuntimeVersion reports the runtime version the engine will serve for
// these params so the controller can record it in status. It is "" when the
// image is user-set (spec.image), because the version is then unknown.
func ResolvedRuntimeVersion(p engine.Params) string {
	if p.Image != "" {
		return ""
	}
	return resolvedRuntimeVersion(p)
}

// imageForVersion builds the ghcr image reference for a version and device. An
// invalid version falls back to DefaultRuntimeVersion (defence in depth: the
// controller validates with ValidateRuntimeVersion before building specs, but a
// spec-building method cannot return an error, so it must never emit a malformed
// tag). When the (resolved) version has a known index digest it is appended as
// repo:tag@sha256:... so the reference is immutable; otherwise the bare tag is
// returned and RuntimeImagePinned reports false for it.
func imageForVersion(version, device string) string {
	if ValidateRuntimeVersion(version) != nil {
		version = DefaultRuntimeVersion
	} else if version == "" {
		version = DefaultRuntimeVersion
	}
	tag := version
	if device == engine.DeviceCUDA {
		tag += "-cuda"
	}
	ref := imageRepo + ":" + tag
	if digest := digestForVersion(version, device); digest != "" {
		ref += "@" + digest
	}
	return ref
}

// DefaultRuntimeVersion implements engine.RuntimeVersioner: the release used when
// Params.RuntimeVersion is empty.
func (e *Engine) DefaultRuntimeVersion() string { return DefaultRuntimeVersion }

// ValidateRuntimeVersion implements engine.RuntimeVersioner, delegating to the
// package-level validator (empty is valid; below MinRuntimeVersion or malformed
// is an *InvalidRuntimeVersionError).
func (e *Engine) ValidateRuntimeVersion(version string) error {
	return ValidateRuntimeVersion(version)
}

// CompareRuntimeVersions implements engine.RuntimeVersioner: it orders two
// MAJOR.MINOR.PATCH versions (-1, 0, 1). A version that is not valid (malformed,
// or below the minimum) is treated as "unknown" and compares equal to anything,
// so it never claims an update is available — matching the controller's prior
// behaviour.
func (e *Engine) CompareRuntimeVersions(a, b string) int {
	if !comparableVersion(a) || !comparableVersion(b) {
		return 0
	}
	return compareVersions(a, b)
}

// RuntimeImagePinned implements engine.RuntimeImagePinner: it reports whether the
// image this engine renders for a version ("" = the engine default) and device is
// pinned by an immutable digest. True only when the resolved version has a known
// index digest in runtimeImageDigests for the device, i.e. exactly when
// imageForVersion appends @sha256:... . A user spec.image is never rendered by
// this engine (the controller handles --allow-image-override separately), so this
// method only speaks to engine-rendered images.
func (e *Engine) RuntimeImagePinned(version, device string) bool {
	v := version
	if v == "" || ValidateRuntimeVersion(v) != nil {
		// Empty or invalid renders as the default (defence in depth); report the
		// default's pinned-ness so the two agree.
		v = DefaultRuntimeVersion
	}
	return digestForVersion(v, device) != ""
}

// comparableVersion reports whether v is a concrete, orderable runtime version:
// a well-formed MAJOR.MINOR.PATCH string that is not below the minimum. The empty
// "use the default" sentinel and any malformed or below-minimum string are not
// comparable (treated as "unknown").
func comparableVersion(v string) bool {
	return runtimeVersionRE.MatchString(v) && ValidateRuntimeVersion(v) == nil
}

// RuntimeVersionFromImage implements engine.RuntimeVersioner: it extracts the
// MAJOR.MINOR.PATCH release from one of the engine's default image references
// (ghcr.io/ollaya-dev/ollaya:<v>, :<v>-cuda, optionally pinned with
// @sha256:<digest>). Anything else — a different repository, a digest-only
// reference, or a mirror of the image under another registry — returns "" (the
// version is then unknown).
func (e *Engine) RuntimeVersionFromImage(image string) string {
	prefix := imageRepo + ":"
	if !strings.HasPrefix(image, prefix) {
		return ""
	}
	tag := strings.TrimPrefix(image, prefix)
	// Drop a pinned digest (repo:tag@sha256:...) before parsing the tag.
	if i := strings.IndexByte(tag, '@'); i >= 0 {
		tag = tag[:i]
	}
	tag = strings.TrimSuffix(tag, "-cuda")
	if ValidateRuntimeVersion(tag) != nil {
		return ""
	}
	return tag
}

const (
	containerName   = "ollaya"
	prefetchName    = "prefetch"
	modelsMount     = "/models"
	stateMountPath  = "/home/ollaya/.ollaya"
	modelsVolume    = "models"
	stateVolume     = "ollaya-state"
	runtimeUID      = int64(1000)
	runtimeGID      = int64(1000)
	terminationSecs = int64(30)
	prefetchDeadl   = int64(1800)
	prefetchBackoff = int32(4)
	gpuResourceName = corev1.ResourceName("nvidia.com/gpu")

	// Prefetch container resource defaults. `ollaya pull` is I/O-bound: it
	// downloads layers and writes them to the store, it does not load the model
	// into memory. Measured peak RSS pulling laya:en (853 MB) in the CPU image
	// under OrbStack was ~36-39 MiB (sampling `docker stats`); the store on disk
	// was ~814 MiB. Without any request the Job is BestEffort and the first to
	// be evicted / OOM-killed on a busy node, so we set a small guaranteed
	// request with generous headroom and a memory limit that still fits a larger
	// multi-GB GGUF pull. CPU is left unlimited (download throughput), only
	// requested, so the Job is Burstable rather than throttled.
	prefetchCPUReq = "100m"
	prefetchMemReq = "256Mi"
	prefetchMemLim = "1Gi"
)

// imageFor picks the serving image: p.Image override, else the image derived
// from the resolved runtime version and device.
func imageFor(p engine.Params) string {
	if p.Image != "" {
		return p.Image
	}
	return imageForVersion(p.RuntimeVersion, p.Device)
}

// ptr returns a pointer to v (Kubernetes API fields want pointers).
func ptr[T any](v T) *T { return &v }

// podSecurityContext is shared by the serving Pod and prefetch Job.
func podSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: ptr(true),
		RunAsUser:    ptr(runtimeUID),
		RunAsGroup:   ptr(runtimeGID),
		FSGroup:      ptr(runtimeGID),
		// Skip the recursive chown of the store on every mount when the root
		// already has the right group: stores hold multi-GB weights (GGUF), and
		// a full walk on each Pod start delays readiness.
		FSGroupChangePolicy: ptr(corev1.FSGroupChangeOnRootMismatch),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

// apiKeyEnv returns the OLLAYA_API_KEY env var sourced from the secret, or nil.
func apiKeyEnv(p engine.Params) *corev1.EnvVar {
	if p.APIKey == nil {
		return nil
	}
	return &corev1.EnvVar{
		Name:      "OLLAYA_API_KEY",
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: p.APIKey},
	}
}

// downloadTokenEnv returns the OLLAYA_HF_TOKEN env var sourced from a Secret, or
// nil. Used by the prefetch Job only; serving Pods never download weights.
func downloadTokenEnv(p engine.Params) *corev1.EnvVar {
	if p.DownloadToken == nil {
		return nil
	}
	ref := p.DownloadToken.DeepCopy()
	ref.Optional = ptr(false)
	return &corev1.EnvVar{
		Name:      "OLLAYA_HF_TOKEN",
		ValueFrom: &corev1.EnvVarSource{SecretKeyRef: ref},
	}
}

// threadsSupportVersion is the first Ollaya release that reads OLLAYA_THREADS
// (spike 008 §2: shipped in 0.11.0). Older runtimes ignore the env, so setting
// it would only churn their Pod template on an operator upgrade for no effect.
const threadsSupportVersion = "0.11.0"

// supportsThreadsEnv reports whether the runtime these Params will run supports
// OLLAYA_THREADS. The version must be known AND at least threadsSupportVersion:
//   - a user-set spec.image is an unknown version -> false (never set the env,
//     the image may be any release or a fork);
//   - an empty RuntimeVersion resolves to the engine default;
//   - otherwise the recorded/explicit RuntimeVersion is compared.
//
// Gating on the version keeps a recorded 0.10.0 stable rendering byte-identical
// to the pre-OLLAYA_THREADS output, so an operator upgrade does not restart it.
func supportsThreadsEnv(p engine.Params) bool {
	if p.Image != "" {
		return false
	}
	v := resolvedRuntimeVersion(p)
	if !comparableVersion(v) {
		return false
	}
	return compareVersions(v, threadsSupportVersion) >= 0
}

// ollayaThreadsEnv returns an OLLAYA_THREADS env var set to floor(cpu limit) when
// the user set a CPU limit on the serving container AND the runtime supports the
// env (>= 0.11.0, see supportsThreadsEnv), or nil otherwise.
// Pinning the runtime's worker threads to the container's CPU limit measurably
// lowers and tightens inference latency on the ONNX models (spike 008 §2: ~20%
// lower median on laya:en, flat memory) and is expected to help the GGUF/llama.cpp
// runner more. It is a serving-only env (the prefetch Job does not run inference),
// derived from resources.limits.cpu — already a revision-hash input — so it adds
// no new hash dimension: a Pod template with OLLAYA_THREADS appears only for a DM
// that set a CPU limit, which the user already controls. No limit, an older
// runtime, or a user-set image -> unset -> the runtime's own default.
func ollayaThreadsEnv(p engine.Params) *corev1.EnvVar {
	if !supportsThreadsEnv(p) {
		return nil
	}
	lim, ok := p.Resources.Limits[corev1.ResourceCPU]
	if !ok {
		return nil
	}
	// floor of the CPU quantity in whole cores; at least 1. MilliValue()/1000
	// floors (e.g. "1500m" -> 1, "2" -> 2, "500m" -> 0 -> clamped to 1).
	cores := lim.MilliValue() / 1000
	if cores < 1 {
		cores = 1
	}
	return &corev1.EnvVar{Name: "OLLAYA_THREADS", Value: strconv.FormatInt(cores, 10)}
}

// ServingPodSpec returns the PodSpec for serving Pods. The model store is
// mounted read-only at /models; a writable emptyDir backs /home/ollaya/.ollaya.
func (e *Engine) ServingPodSpec(p engine.Params) corev1.PodSpec {
	env := []corev1.EnvVar{
		{Name: "OLLAYA_HOST", Value: "0.0.0.0:11435"},
		{Name: "OLLAYA_MODELS", Value: modelsMount},
		{Name: "OLLAYA_DEVICE", Value: p.Device},
		// §6: OLLAYA_KEEP_ALIVE accepts a negative value ("-1") to keep a model
		// loaded until the server stops; Warmup also pins via the API.
		{Name: "OLLAYA_KEEP_ALIVE", Value: "-1"},
	}
	if te := ollayaThreadsEnv(p); te != nil {
		env = append(env, *te)
	}
	if ake := apiKeyEnv(p); ake != nil {
		env = append(env, *ake)
	}
	// When the engine registry is non-default, set OLLAYA_REGISTRY on serving Pods
	// too: `ollaya serve` resolves a host-less name (e.g. "laya:en") against the
	// store under manifests/<OLLAYA_REGISTRY host>/..., which is where the prefetch
	// Job wrote it. Without it, serve would look under
	// manifests/ollaya.dev/... and /api/ps would never show the model, so the
	// readiness gate could not pass on a mirror-pulled store. Proven in the image
	// (serve with OLLAYA_REGISTRY=http://mirror:8080 loads a mirror-pulled
	// laya:en, /api/ps shows it). Default registry renders byte-identically.
	if !registryIsDefault(e.registryURL) {
		env = append(env, corev1.EnvVar{Name: "OLLAYA_REGISTRY", Value: e.registryURL})
	}

	// Liveness/startup on GET / (§7.1: always 200, even when an API key is set,
	// unlike /api/tags which would 401).
	rootProbe := func() *corev1.Probe {
		return &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/",
					Port: intstr.FromString("http"),
				},
			},
		}
	}
	startup := rootProbe()
	startup.PeriodSeconds = 2
	startup.FailureThreshold = 30
	liveness := rootProbe()
	liveness.PeriodSeconds = 10

	return corev1.PodSpec{
		SecurityContext:               podSecurityContext(),
		AutomountServiceAccountToken:  ptr(false), // Ollaya needs no API access (review E5)
		TerminationGracePeriodSeconds: ptr(terminationSecs),
		Containers: []corev1.Container{
			{
				Name:  containerName,
				Image: imageFor(p),
				Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: servicePort},
				},
				Env:       env,
				Resources: servingResources(p),
				SecurityContext: &corev1.SecurityContext{
					ReadOnlyRootFilesystem:   ptr(true),
					AllowPrivilegeEscalation: ptr(false),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: modelsVolume, MountPath: modelsMount, SubPath: p.StoreSubPath, ReadOnly: true},
					{Name: stateVolume, MountPath: stateMountPath},
				},
				StartupProbe:   startup,
				LivenessProbe:  liveness,
				ReadinessProbe: nil, // model readiness is driven by the readiness gate
			},
		},
		Volumes: []corev1.Volume{
			{
				Name: modelsVolume,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: p.CacheClaimName,
						ReadOnly:  true,
					},
				},
			},
			{
				Name:         stateVolume,
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			},
		},
	}
}

// servingResources returns the serving container resources: it first fills
// memory/cpu requests from the per-model default table for any key the user
// left unset (applyModelDefaults), then adds nvidia.com/gpu:1 to limits for
// CUDA when the user has not already set it. User-provided values always win,
// per resource key, and no CPU limit is ever added.
func servingResources(p engine.Params) corev1.ResourceRequirements {
	res := applyModelDefaults(p.Model.Name, p.Device, p.Resources)
	if p.Device != engine.DeviceCUDA {
		return res
	}
	if _, ok := res.Limits[gpuResourceName]; ok {
		return res // keep the user's GPU value
	}
	if res.Limits == nil {
		res.Limits = corev1.ResourceList{}
	}
	res.Limits[gpuResourceName] = resource.MustParse("1")
	return res
}

// prefetchResources returns the resource requests/limits for the prefetch
// container. See the prefetch* constants for the measured rationale: a small
// guaranteed memory/cpu request so the Job is not BestEffort (first to be
// evicted/OOM-killed on a busy node), and a memory limit with headroom for
// larger pulls. CPU is requested but not limited.
func prefetchResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(prefetchCPUReq),
			corev1.ResourceMemory: resource.MustParse(prefetchMemReq),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse(prefetchMemLim),
		},
	}
}

// Prefetch exit codes. A permanent code tells the controller (via the Job's
// podFailurePolicy) to fail immediately without burning retries; a transient
// failure (exit 1) is retried by the Job up to backoffLimit. These values are
// also what classifyPrefetchFailure maps back to a reason.
const (
	prefetchExitTransient      = 1 // network / 5xx / timeout: retry
	prefetchExitModelNotFound  = 3 // manifest 404 / tag not found: permanent
	prefetchExitDigestMismatch = 4 // pulled digest != expected: permanent
	prefetchExitTagMoved       = 5 // pulled a valid but different manifest: tag moved upstream, permanent

	// String forms for embedding in the shell script (const concatenation).
	prefetchExitTransientStr      = "1"
	prefetchExitModelNotFoundStr  = "3"
	prefetchExitDigestMismatchStr = "4"
	prefetchExitTagMovedStr       = "5"
)

// Prefetch failure reasons written to the termination message (and the last log
// line as a fallback) so the controller can surface them. Exported so the
// controller maps a failed Job to the same strings without duplicating them.
const (
	PrefetchReasonModelNotFound    = "ModelNotFound"
	PrefetchReasonDigestMismatch   = "DigestMismatch"
	PrefetchReasonUpstreamTagMoved = "UpstreamTagMoved"
	PrefetchReasonTransient        = "Transient"
)

// prefetchScript pulls the model and verifies its digest, then optionally prunes
// sibling revision sub-paths from the store root. It classifies a failure as
// permanent (a 404/tag-not-found or a digest mismatch) or transient (network,
// 5xx, timeout) and exits with a distinct code so the Job's podFailurePolicy can
// fail fast on a permanent error instead of burning all retries. The reason is
// written to the container termination message and as the final log line
// (TerminationMessagePolicy: FallbackToLogsOnError). The manifest path is
// computed in Go (MANIFEST_PATH); the model name uses the "--" separator so a
// value starting with "-" cannot be read as a CLI flag (review E4).
//
// ollaya pull returns exit 1 for BOTH a missing tag and a network error (verified
// in the image), so a missing tag is told apart by the message
// ("not found in registry"); anything else that fails the pull is treated as
// transient and retried. A successful pull whose manifest digest differs from the
// recorded one is split further: a well-formed manifest means the tag moved
// upstream (UpstreamTagMoved, exit 5), an ill-formed one means corruption
// (DigestMismatch, exit 4); both are permanent.
var prefetchScript = `set -eu
fail() {
  # $1 = reason, $2 = exit code, $3 = optional one-line detail (short digests).
  # The first line is always "reason: <X>" (the classifier matches that prefix);
  # a non-empty detail is written as a second "detail: <...>" line so the user
  # sees the two short digests in the condition/Event. Kept well under the
  # 4096-byte termination-message limit (two 12-hex digests plus labels).
  {
    printf 'reason: %s\n' "$1"
    [ -n "${3:-}" ] && printf 'detail: %s\n' "$3"
  } > /dev/termination-log 2>/dev/null || true
  echo "prefetch failed: $1" >&2
  printf 'reason: %s\n' "$1"
  [ -n "${3:-}" ] && printf 'detail: %s\n' "$3"
  exit "$2"
}
# Seed: if the controller passed the recorded manifest bytes (base64 in
# MANIFEST_SEED_B64), verify them against EXPECT_DIGEST and write them to the
# on-disk tag path BEFORE pulling. ollaya pull then trusts the on-disk manifest
# (spike 009) and fetches exactly the referenced blobs, so the store is rebuilt
# to the recorded digest even if the tag moved upstream. A seed whose sha256 does
# not match EXPECT_DIGEST is a permanent DigestMismatch BEFORE anything is written.
if [ -n "${MANIFEST_SEED_B64:-}" ]; then
  if [ -z "${EXPECT_DIGEST:-}" ] || [ -z "${MANIFEST_PATH:-}" ]; then
    fail ` + PrefetchReasonDigestMismatch + ` ` + prefetchExitDigestMismatchStr + `
  fi
  seed_tmp="$(mktemp)"
  printf '%s' "$MANIFEST_SEED_B64" | base64 -d > "$seed_tmp" || fail ` + PrefetchReasonDigestMismatch + ` ` + prefetchExitDigestMismatchStr + `
  seed_digest="$(sha256sum "$seed_tmp" | cut -d' ' -f1)"
  if [ "$seed_digest" != "$EXPECT_DIGEST" ]; then
    echo "seed manifest digest $seed_digest != expected $EXPECT_DIGEST; refusing to write" >&2
    rm -f "$seed_tmp"
    seed_short="$(printf '%s' "$seed_digest" | cut -c1-12)"
    want_short="$(printf '%s' "$EXPECT_DIGEST" | cut -c1-12)"
    fail ` + PrefetchReasonDigestMismatch + ` ` + prefetchExitDigestMismatchStr + ` "recorded $want_short, seed $seed_short"
  fi
  mkdir -p "$(dirname "$OLLAYA_MODELS/$MANIFEST_PATH")"
  mv "$seed_tmp" "$OLLAYA_MODELS/$MANIFEST_PATH"
  echo "seeded manifest for $seed_digest at $MANIFEST_PATH"
fi
pull_err="$(ollaya pull -- "$MODEL" 2>&1 1>/dev/null)" || {
  echo "$pull_err" >&2
  case "$pull_err" in
    *"not found in registry"*|*"not found"*)
      fail ` + PrefetchReasonModelNotFound + ` ` + prefetchExitModelNotFoundStr + ` ;;
    *)
      fail ` + PrefetchReasonTransient + ` ` + prefetchExitTransientStr + ` ;;
  esac
}
if [ -z "${EXPECT_DIGEST:-}" ]; then
  echo "no expected digest; skipping verification"
else
  got="$(sha256sum "$OLLAYA_MODELS/$MANIFEST_PATH" | cut -d' ' -f1)"
  echo "pulled digest: $got"
  if [ "$got" != "$EXPECT_DIGEST" ]; then
    echo "digest mismatch: got $got want $EXPECT_DIGEST" >&2
    # Distinguish a moved tag from a corrupt download: if the pulled manifest is
    # a well-formed Ollaya manifest (schemaVersion 2) it is a real but different
    # revision (the tag moved upstream), which recovery of a pinned revision
    # cannot fix by re-pulling the tag; a manifest that is not well-formed is
    # treated as corruption. Both are permanent. The message names both short
    # digests (first 12 hex) so the user sees what moved.
    want_short="$(printf '%s' "$EXPECT_DIGEST" | cut -c1-12)"
    got_short="$(printf '%s' "$got" | cut -c1-12)"
    if grep -q '"schemaVersion"[[:space:]]*:[[:space:]]*2' "$OLLAYA_MODELS/$MANIFEST_PATH" 2>/dev/null; then
      echo "tag moved upstream: recorded $want_short, registry now serves $got_short" >&2
      fail ` + PrefetchReasonUpstreamTagMoved + ` ` + prefetchExitTagMovedStr + ` "recorded $want_short, registry now serves $got_short"
    fi
    echo "corrupt or unexpected manifest: recorded $want_short, got $got_short" >&2
    fail ` + PrefetchReasonDigestMismatch + ` ` + prefetchExitDigestMismatchStr + ` "recorded $want_short, got $got_short"
  fi
  echo "digest ok: $got"
fi
# Prune other revisions' stores. Runs only when the operator provided a valid,
# non-empty keep list (KEEP_SUBPATHS) and STORE_ROOT is mounted. Only prune
# entries that are directories whose name is a revision hash — exactly 10 or 16
# lowercase hex chars (defence in depth via the case below); anything else at the
# root (legacy manifests/, blobs/, lost+found, files) is left untouched.
if [ -n "${KEEP_SUBPATHS:-}" ] && [ -n "${STORE_ROOT:-}" ]; then
  for entry in "$STORE_ROOT"/*; do
    [ -d "$entry" ] || continue
    base="$(basename "$entry")"
    case "$base" in
      [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
      [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
      *) continue ;;  # not a revision hash (10 or 16 hex): never prune
    esac
    keep=no
    for k in $KEEP_SUBPATHS; do
      if [ "$base" = "$k" ]; then keep=yes; break; fi
    done
    if [ "$keep" = "no" ]; then
      echo "pruning store sub-path: $base"
      rm -rf -- "$entry"
    fi
  done
fi
`

// unparsableNameScript is used when the model name does not parse: it never
// runs ollaya pull, prints the reason and exits 1 (review E4).
const unparsableNameScript = `echo "ollaya: refusing to prefetch: model name did not parse in the operator" >&2
exit 1
`

// invalidSubPathScript is used when a non-empty StoreSubPath is not a valid
// revision hash: the Job refuses rather than pulling into an unexpected path.
const invalidSubPathScript = `echo "ollaya: refusing to prefetch: store sub-path is not a valid revision hash" >&2
exit 1
`

// storeRootMount is where the Job mounts the PVC root (no subPath) so it can
// prune sibling revision sub-paths.
const storeRootMount = "/store-root"

// terminationMessagePath is where the prefetch container writes its one-line
// "reason: <X>" classification; the kubelet mounts it writable even under
// readOnlyRootFilesystem. TerminationMessageFallbackToLogsOnError makes the
// controller fall back to the last log line (also "reason: <X>") if the file is
// empty.
const terminationMessagePath = "/dev/termination-log"

// prefetchPodFailurePolicy fails the Job immediately on a permanent prefetch exit
// code (a missing tag or a digest mismatch) and counts anything else toward the
// normal retry budget. Requires RestartPolicy: Never.
func prefetchPodFailurePolicy() *batchv1.PodFailurePolicy {
	return &batchv1.PodFailurePolicy{
		Rules: []batchv1.PodFailurePolicyRule{
			{
				Action: batchv1.PodFailurePolicyActionFailJob,
				OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
					Operator: batchv1.PodFailurePolicyOnExitCodesOpIn,
					Values:   []int32{prefetchExitModelNotFound, prefetchExitDigestMismatch, prefetchExitTagMoved},
				},
			},
		},
	}
}

// classifyPrefetchFailure maps a failed prefetch pod's termination message and
// exit code to a stable reason and whether the failure is permanent (never worth
// retrying). The controller calls it to surface the reason in the PrefetchFailed
// condition/Event. The termination message is matched first ("reason: <X>",
// written by the script); the exit code is the fallback when the message is
// absent (e.g. the container was killed before writing it). An unrecognised
// failure is treated as transient so the operator does not permanently fail a
// revision on an unknown error.
func classifyPrefetchFailure(terminationMessage string, exitCode int32) (reason string, permanent bool) {
	switch {
	case strings.Contains(terminationMessage, "reason: "+PrefetchReasonUpstreamTagMoved):
		return PrefetchReasonUpstreamTagMoved, true
	case strings.Contains(terminationMessage, "reason: "+PrefetchReasonDigestMismatch):
		return PrefetchReasonDigestMismatch, true
	case strings.Contains(terminationMessage, "reason: "+PrefetchReasonModelNotFound):
		return PrefetchReasonModelNotFound, true
	case strings.Contains(terminationMessage, "reason: "+PrefetchReasonTransient):
		return PrefetchReasonTransient, false
	}
	switch exitCode {
	case prefetchExitTagMoved:
		return PrefetchReasonUpstreamTagMoved, true
	case prefetchExitDigestMismatch:
		return PrefetchReasonDigestMismatch, true
	case prefetchExitModelNotFound:
		return PrefetchReasonModelNotFound, true
	default:
		return PrefetchReasonTransient, false
	}
}

// maxSeedManifestBytes caps the raw manifest the Job will seed. Real manifests
// are a few KB (laya:en is ~3 KB); the cap is generous for a multi-layer manifest
// but keeps a hostile/oversized value out of the Pod spec env (the kernel bounds
// the whole arg/env block). A larger manifest simply skips seeding and falls back
// to pull-by-tag.
const maxSeedManifestBytes = 256 << 10 // 256 KiB

// seedManifestUsable reports whether a ModelRef carries manifest bytes we can
// seed: present, within the size cap, with a digest to verify against, and
// sha256(Manifest) actually equals Digest (defence in depth — the Job re-checks,
// but a mismatched pair here is a controller bug we must not ship into the store).
func seedManifestUsable(m engine.ModelRef) bool {
	if len(m.Manifest) == 0 || len(m.Manifest) > maxSeedManifestBytes || m.Digest == "" {
		return false
	}
	sum := sha256.Sum256(m.Manifest)
	return hex.EncodeToString(sum[:]) == m.Digest
}

// revHashRE validates a store sub-path / keep-list entry: the controller's
// revision hash is 10 lowercase hex chars (legacy) or 16 (the 64-bit width).
// Only entries matching this may ever be pruned; legacy store dirs (manifests/,
// blobs/, lost+found) never match, so they are always left untouched. Both exact
// widths are accepted and nothing in between, so a stray path can never match.
var revHashRE = regexp.MustCompile(`^([a-f0-9]{10}|[a-f0-9]{16})$`)

// PrefetchJobSpec returns a Job that downloads p.Model into the store (RW
// mount, at StoreSubPath). Idempotent (ollaya pull is a fast no-op when
// present) and fails if the downloaded digest != p.Model.Digest. After a
// successful pull it prunes sibling revision sub-paths not in
// KeepStoreSubPaths (never when that list is empty).
func (e *Engine) PrefetchJobSpec(p engine.Params) batchv1.JobSpec {
	// Pulling needs no GPU: always use the CPU image of the same runtime version
	// unless p.Image overrides.
	image := imageForVersion(p.RuntimeVersion, engine.DeviceCPU)
	if p.Image != "" {
		image = p.Image
	}

	// Parse the name to compute the on-disk manifest path. An unparsable name
	// must never reach `ollaya pull`: use a script that refuses and exits 1
	// (review E4). The controller also validates names, this is defence in depth.
	parsed, perr := parseName(p.Model.Name)
	script := prefetchScript
	manifestPath := ""
	switch {
	case perr != nil:
		script = unparsableNameScript
	case p.StoreSubPath != "" && !revHashRE.MatchString(p.StoreSubPath):
		// A non-empty sub-path that is not a revision hash: refuse.
		script = invalidSubPathScript
	default:
		manifestPath = parsed.manifestDiskPath(e.defaultManifestHostDir())
	}

	env := []corev1.EnvVar{
		{Name: "MODEL", Value: p.Model.Name},
		{Name: "EXPECT_DIGEST", Value: p.Model.Digest},
		{Name: "OLLAYA_MODELS", Value: modelsMount},
		{Name: "MANIFEST_PATH", Value: manifestPath},
	}

	// Seed the recorded manifest bytes so the Job can rebuild exactly this digest
	// even after the tag moved upstream (spike 009: ollaya pull trusts an on-disk
	// manifest). Only when: the controller supplied the bytes, this is the normal
	// pull script (not a refuse script), the on-disk path is known, and the digest
	// matches the bytes. The bytes go in a base64 env var — manifests are a few KB;
	// a cap keeps a hostile/oversized value out of the Pod spec (the whole env
	// block is bounded by the kernel's arg/env limit). The script re-verifies the
	// seed against EXPECT_DIGEST before writing, so a wrong value fails permanently
	// without touching the store.
	if script == prefetchScript && manifestPath != "" && seedManifestUsable(p.Model) {
		env = append(env, corev1.EnvVar{
			Name:  "MANIFEST_SEED_B64",
			Value: base64.StdEncoding.EncodeToString(p.Model.Manifest),
		})
	}

	// Point `ollaya pull` at the same registry the resolver used. The Ollaya
	// 0.7.3 and 0.10.0 CLI selects its registry for a bare (host-less) name from the
	// OLLAYA_REGISTRY env var, falling back to ollaya.dev; an explicit host in
	// the name always overrides it (verified against the image; evidence in the
	// the spike). The
	// resolver uses the same precedence (parsedName.registryBase). Without this,
	// --ollaya-registry / OLLAYA_REGISTRY reached only the resolver, so the Job
	// still pulled from ollaya.dev — a digest mismatch, and air-gapped mode did
	// not work. We set it only when the engine
	// registry is non-default so the default case renders byte-identically.
	if !registryIsDefault(e.registryURL) {
		env = append(env, corev1.EnvVar{Name: "OLLAYA_REGISTRY", Value: e.registryURL})
	}

	// Hugging Face / weight mirror: the registry (OLLAYA_REGISTRY) serves manifests,
	// but the weight blobs are fetched from a separate endpoint (Hugging Face, an
	// HF mirror, or an enterprise endpoint). When configured, tell the CLI.
	if e.hfEndpoint != "" {
		env = append(env, corev1.EnvVar{Name: "OLLAYA_HF_ENDPOINT", Value: e.hfEndpoint})
	}
	// Download token: a credential for private/gated model weights on HF or a
	// mirror. Injected from a Secret ref; never into serving Pods (they never
	// download). The token is sent to the HF endpoint only, never to the model
	// registry.
	if dte := downloadTokenEnv(p); dte != nil {
		env = append(env, *dte)
	}

	// Store-root pruning is fail-safe: enabled only when the caller gave a
	// non-empty KeepStoreSubPaths in which EVERY entry is a valid revision hash,
	// and this revision has a valid sub-path. Any invalid entry disables pruning
	// entirely (the script logs why). The current sub-path is added to the keep
	// set but does not by itself enable pruning (review fixes 1 & 2).
	volumeMounts := []corev1.VolumeMount{
		{Name: modelsVolume, MountPath: modelsMount, SubPath: p.StoreSubPath}, // RW: the Job writes the store
		{Name: stateVolume, MountPath: stateMountPath},
	}
	keep, pruneEnabled := pruneKeepList(p)
	if pruneEnabled && script == prefetchScript {
		env = append(env,
			corev1.EnvVar{Name: "STORE_ROOT", Value: storeRootMount},
			corev1.EnvVar{Name: "KEEP_SUBPATHS", Value: strings.Join(keep, " ")},
		)
		// Second mount of the PVC root (no subPath) so the script can prune siblings.
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: modelsVolume, MountPath: storeRootMount})
	}

	return batchv1.JobSpec{
		BackoffLimit:          ptr(prefetchBackoff),
		ActiveDeadlineSeconds: ptr(prefetchDeadl),
		// Fail fast on a permanent error (a missing tag or a digest mismatch) so a
		// corrupt/moved tag does not burn all backoffLimit retries; keep counting
		// transient failures (network/5xx/timeout) toward the normal retry budget.
		// podFailurePolicy requires RestartPolicy: Never (the kubelet rejects it
		// with OnFailure), so the pod is replaced per failure rather than its
		// container restarted in place — same retry budget via BackoffLimit.
		PodFailurePolicy: prefetchPodFailurePolicy(),
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy:                corev1.RestartPolicyNever,
				SecurityContext:              podSecurityContext(),
				AutomountServiceAccountToken: ptr(false), // review E5
				Containers: []corev1.Container{
					{
						Name:                     prefetchName,
						Image:                    image,
						Command:                  []string{"/bin/sh", "-c"},
						Args:                     []string{script},
						Env:                      env,
						Resources:                prefetchResources(),
						TerminationMessagePath:   terminationMessagePath,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
						SecurityContext: &corev1.SecurityContext{
							ReadOnlyRootFilesystem:   ptr(true), // review E5: writes only to mounts
							AllowPrivilegeEscalation: ptr(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: volumeMounts,
					},
				},
				Volumes: []corev1.Volume{
					{
						Name: modelsVolume,
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: p.CacheClaimName,
							},
						},
					},
					{
						Name:         stateVolume,
						VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
					},
				},
			},
		},
	}
}

// pruneKeepList decides whether store pruning is safe and returns the keep set.
// Pruning is enabled only when the caller-provided KeepStoreSubPaths is
// non-empty AND every entry is a valid revision hash AND this revision has a
// valid sub-path. If any keep entry is invalid, pruning is DISABLED (returning
// enabled=false) rather than silently dropping it — dropping would widen
// deletion (review fix 2). The current sub-path is added to the keep set
// (so we never delete what we just pulled) but does not by itself enable
// pruning (fix 1).
func pruneKeepList(p engine.Params) (keep []string, enabled bool) {
	if len(p.KeepStoreSubPaths) == 0 {
		return nil, false
	}
	if p.StoreSubPath == "" || !revHashRE.MatchString(p.StoreSubPath) {
		return nil, false
	}
	seen := map[string]bool{}
	for _, k := range p.KeepStoreSubPaths {
		if !revHashRE.MatchString(k) {
			// Any invalid entry disables pruning entirely.
			return nil, false
		}
		if !seen[k] {
			keep = append(keep, k)
			seen[k] = true
		}
	}
	if !seen[p.StoreSubPath] {
		keep = append(keep, p.StoreSubPath)
	}
	return keep, true
}

// normalizeDevice maps a runtime device string to an engine.Device* class.
// Ollaya reports the concrete device ("cuda:0", "cuda:1") on GPU and "cpu" on
// CPU; the readiness gate compares against the requested class ("cuda").
//
// Only the exact form "cuda:<ordinal>" is collapsed to "cuda". Anything else
// (e.g. a mixed/partial-offload value like "cuda:0+cpu" or "cuda:0,cpu") is
// returned unchanged so it fails the gate: loosening this check would hide the
// silent CPU fallback the gate exists to catch.
func normalizeDevice(d string) string {
	d = strings.ToLower(strings.TrimSpace(d))
	base, ord, ok := strings.Cut(d, ":")
	if !ok || base != engine.DeviceCUDA || ord == "" {
		return d
	}
	for _, c := range ord {
		if c < '0' || c > '9' {
			return d
		}
	}
	return base
}

// Compile-time check that Engine implements the optional PrefetchFailureClassifier capability.
var _ engine.PrefetchFailureClassifier = (*Engine)(nil)

// ClassifyPrefetchFailure implements engine.PrefetchFailureClassifier.
func (e *Engine) ClassifyPrefetchFailure(terminationMessage string, exitCode int32) (string, bool) {
	return classifyPrefetchFailure(terminationMessage, exitCode)
}

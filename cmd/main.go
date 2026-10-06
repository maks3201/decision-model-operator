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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/controller"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// parseWatchNamespaces parses the --watch-namespaces flag value into a
// validated, de-duplicated, order-preserving list of namespace names.
//
// Rules:
//   - empty/whitespace-only input -> nil (watch all namespaces, cluster-wide);
//   - each item is trimmed of surrounding whitespace;
//   - an empty item between commas (e.g. "a,,b" or a trailing comma) is rejected;
//   - each name must be a valid DNS-1123 label (Kubernetes namespace names are
//     DNS-1123 labels: lowercase alphanumeric or '-', start/end alphanumeric,
//     max 63 chars);
//   - duplicates are dropped, keeping first occurrence.
func parseWatchNamespaces(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	seen := make(map[string]struct{})
	var out []string
	for _, part := range strings.Split(raw, ",") {
		ns := strings.TrimSpace(part)
		if ns == "" {
			return nil, fmt.Errorf("empty namespace in --watch-namespaces=%q", raw)
		}
		if errs := validation.IsDNS1123Label(ns); len(errs) > 0 {
			return nil, fmt.Errorf("invalid namespace %q: %s", ns, strings.Join(errs, "; "))
		}
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		out = append(out, ns)
	}
	return out, nil
}

// warnSkippedProxyEnv logs ONE warning naming the proxy variables that are not
// passed to prefetch Jobs. info is the logger's Info method. It receives key names
// only: never a value (it holds credentials) and never a parse error (net/url
// errors embed the input string).
func warnSkippedProxyEnv(info func(msg string, keysAndValues ...any), skipped []string) {
	if len(skipped) == 0 {
		return
	}
	info("proxy variables that carry credentials or cannot be parsed are used by the operator "+
		"but are NOT passed to prefetch Jobs; prefetch pulls will not use this proxy",
		"variables", skipped)
}

// validateHFEndpoint checks that the Hugging Face endpoint URL is well-formed,
// absolute with a host, has no userinfo/query/fragment, and is https (or http
// when allowInsecure is true). Called at startup so a bad flag fails early.
func validateHFEndpoint(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("cannot parse URL: %w", err)
	}
	if !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("must be an absolute URL with a host (got %q)", raw)
	}
	if u.User != nil {
		return fmt.Errorf("must not contain credentials (userinfo); use the download-token Secret")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must not contain a query or fragment")
	}
	switch u.Scheme {
	case "https":
		// ok
	case "http":
		if !allowInsecure {
			return fmt.Errorf("http:// requires --allow-insecure-registries")
		}
	default:
		return fmt.Errorf("unsupported scheme %q (only https or http)", u.Scheme)
	}
	return nil
}

// watchNamespacesCacheDefaults builds the cache.Options.DefaultNamespaces map
// for the given watched namespaces. It returns nil when the set is empty, which
// leaves the manager cache cluster-wide (the default). Extracted for testing.
func watchNamespacesCacheDefaults(watchNamespaces []string) map[string]cache.Config {
	if len(watchNamespaces) == 0 {
		return nil
	}
	defaults := make(map[string]cache.Config, len(watchNamespaces))
	for _, ns := range watchNamespaces {
		defaults[ns] = cache.Config{}
	}
	return defaults
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(decisionmodelv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	var ollayaRegistry string
	flag.StringVar(&ollayaRegistry, "ollaya-registry", "",
		"Base URL of the Ollaya model registry. If unset, OLLAYA_REGISTRY is used, "+
			"otherwise https://ollaya.dev.")
	var ollayaHFEndpoint string
	flag.StringVar(&ollayaHFEndpoint, "ollaya-hf-endpoint", "",
		"Base URL Ollaya downloads model weights from (Hugging Face mirror). If unset, "+
			"OLLAYA_HF_ENDPOINT is used.")
	var allowedRegistries string
	flag.StringVar(&allowedRegistries, "allowed-registries", "ollaya.dev",
		"Comma-separated registry hosts a model may resolve from.")
	var allowInsecureRegistries bool
	flag.BoolVar(&allowInsecureRegistries, "allow-insecure-registries", false,
		"Permit http:// model registries.")
	var allowImageOverride bool
	flag.BoolVar(&allowImageOverride, "allow-image-override", false,
		"Permit spec.image to override the engine default image.")
	var maxConcurrentReconciles int
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 4,
		"Maximum number of DecisionModels reconciled concurrently.")
	var watchNamespacesRaw string
	flag.StringVar(&watchNamespacesRaw, "watch-namespaces", "",
		"Comma-separated namespaces to watch. Empty (default) watches all namespaces. "+
			"When set, the operator only sees and touches these namespaces and can run with "+
			"namespaced RBAC.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Namespace-scoped mode: parse, validate (DNS-1123), and dedupe the
	// watched namespaces. Empty means all namespaces (cluster-wide, default).
	watchNamespaces, err := parseWatchNamespaces(watchNamespacesRaw)
	if err != nil {
		setupLog.Error(err, "invalid --watch-namespaces")
		os.Exit(1)
	}
	if len(watchNamespaces) > 0 {
		setupLog.Info("namespace-scoped mode", "watch-namespaces", watchNamespaces)
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// To use cert-manager instead, uncomment:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	// Scope the cache to objects this operator owns: a label selector on
	// decisionmodel.io/name (present on every owned Deployment/Job/Service/PVC and
	// on serving Pod templates), plus stripping managed fields to cut memory.
	// This avoids caching every Pod/Deployment/etc. in the cluster.
	ownedSelector := labels.SelectorFromSet(nil)
	if req, rerr := labels.NewRequirement(decisionmodelv1alpha1.LabelName, selection.Exists, nil); rerr == nil {
		ownedSelector = labels.NewSelector().Add(*req)
	}
	byLabel := cache.ByObject{Label: ownedSelector}
	cacheOptions := cache.Options{
		DefaultTransform: cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}:                   byLabel,
			&appsv1.Deployment{}:            byLabel,
			&appsv1.ReplicaSet{}:            byLabel,
			&batchv1.Job{}:                  byLabel,
			&corev1.Service{}:               byLabel,
			&corev1.PersistentVolumeClaim{}: byLabel,
			&policyv1.PodDisruptionBudget{}: byLabel,
		},
	}
	// Namespace-scoped mode: restrict the cache to the watched namespaces.
	// DefaultNamespaces applies to every object kind (including the label-selected
	// owned objects above, which keep their selectors), so the operator only lists
	// and watches within these namespaces and needs no cluster-scoped list/watch.
	// The leader-election Lease lives in the operator's own namespace and is not
	// affected. Empty leaves the cache cluster-wide (default).
	if defaults := watchNamespacesCacheDefaults(watchNamespaces); defaults != nil {
		cacheOptions.DefaultNamespaces = defaults
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		Cache:                  cacheOptions,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "4aba9cc6.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if ollayaRegistry == "" {
		ollayaRegistry = os.Getenv("OLLAYA_REGISTRY")
	}
	if ollayaHFEndpoint == "" {
		ollayaHFEndpoint = os.Getenv("OLLAYA_HF_ENDPOINT")
	}
	var ollayaOpts []ollaya.Option
	if ollayaRegistry != "" {
		ollayaOpts = append(ollayaOpts, ollaya.WithRegistryURL(ollayaRegistry))
	}
	if ollayaHFEndpoint != "" {
		if err := validateHFEndpoint(ollayaHFEndpoint, allowInsecureRegistries); err != nil {
			setupLog.Error(err, "invalid --ollaya-hf-endpoint")
			os.Exit(1)
		}
		ollayaOpts = append(ollayaOpts, ollaya.WithHFEndpoint(ollayaHFEndpoint))
	}

	signalCtx := ctrl.SetupSignalHandler()

	var allowed []string
	for _, h := range strings.Split(allowedRegistries, ",") {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			allowed = append(allowed, h)
		}
	}

	// Proxy settings for prefetch Jobs come from the operator's own environment.
	// Credentialed proxy URLs are used by the operator but not copied into tenant
	// Jobs (readable by anyone who can get Jobs in that namespace).
	prefetchProxyEnv, skippedProxyEnv := controller.ProxyEnvFromEnviron(os.Getenv)
	warnSkippedProxyEnv(setupLog.Info, skippedProxyEnv)

	if err := (&controller.DecisionModelReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		APIReader: mgr.GetAPIReader(),
		Recorder:  mgr.GetEventRecorder("decisionmodel-controller"),
		Engines: map[string]engine.Engine{
			"ollaya": ollaya.New(ollayaOpts...),
		},
		AllowedRegistries:       allowed,
		AllowInsecureRegistries: allowInsecureRegistries,
		AllowImageOverride:      allowImageOverride,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		WatchNamespaces:         watchNamespaces,
		PrefetchProxyEnv:        prefetchProxyEnv,
		BaseContext:             signalCtx,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DecisionModel")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(req *http.Request) error {
		// Report ready only once the informer caches have synced, so we
		// don't serve/act on an empty cache right after startup.
		ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
		defer cancel()
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return fmt.Errorf("informer caches not synced")
		}
		return nil
	}); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(signalCtx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

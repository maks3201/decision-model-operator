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

package controller

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"k8s.io/apimachinery/pkg/types"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	// +kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

// zeroStabilizationRollout disables the post-promotion stabilization window
// (spec.rollout.stabilization: 0), restoring the pre-window behaviour where the
// previous revision is collected after the short endpoint-gap grace. GC and
// promotion tests that assert collection shortly after promotion use it so the
// 5m default window does not keep the previous revision alive through the test.
func zeroStabilizationRollout() *decisionmodelv1alpha1.RolloutSpec {
	return &decisionmodelv1alpha1.RolloutSpec{Stabilization: &metav1.Duration{Duration: 0}}
}

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())
	DeferCleanup(cancel)

	var err error
	err = decisionmodelv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// +kubebuilder:scaffold:scheme

	// envtest has no kubelet. Real Kubernetes sets Pod Ready=True once
	// ContainersReady and every readiness gate are True; model that, so specs that
	// let the operator patch the gate see the Pod become Ready like in a cluster.
	// Specs that need "gate True but not Ready" call withoutKubelet.
	afterGatePatch = kubeletSetsReady

	// Legacy reconcile fixtures create bare labelled Pods with no ReplicaSet /
	// Deployment owner chain. Relax the controller-chain ownership check for them by default;
	// the ownership specs re-enable it with withPodOwnership.
	enforcePodOwnership.Store(false)

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	// Retrieve the first found binary directory to allow running tests from IDEs
	if getFirstFoundEnvTestBinaryDir() != "" {
		testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	}

	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel() // stop managers before the API server goes away
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

// getFirstFoundEnvTestBinaryDir locates the first binary in the specified path.
// ENVTEST-based tests depend on specific binaries, usually located in paths set by
// controller-runtime. When running tests directly (e.g., via an IDE) without using
// Makefile targets, the 'BinaryAssetsDirectory' must be explicitly configured.
//
// This function streamlines the process by finding the required binaries, similar to
// setting the 'KUBEBUILDER_ASSETS' environment variable. To ensure the binaries are
// properly set up, run 'make setup-envtest' beforehand.
func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		logf.Log.Error(err, "Failed to read directory", "path", basePath)
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}

// updateDM applies mutate to a DecisionModel under retry.RetryOnConflict: it
// re-Gets the object on each attempt so an optimistic-lock status patch from the
// reconciler that bumped resourceVersion between a test's Get and Update
// cannot fail the test with a spurious 409 Conflict. Use this for every
// test-side read-modify-write on a DecisionModel instead of a bare Get+Update.
func updateDM(
	ctx context.Context,
	namespace, name string,
	mutate func(*decisionmodelv1alpha1.DecisionModel),
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm); err != nil {
			return err
		}
		mutate(dm)
		return k8sClient.Update(ctx, dm)
	})
}

// updateDMStatus is updateDM for the status subresource.
func updateDMStatus(
	ctx context.Context,
	namespace, name string,
	mutate func(*decisionmodelv1alpha1.DecisionModel),
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm); err != nil {
			return err
		}
		mutate(dm)
		return k8sClient.Status().Update(ctx, dm)
	})
}

// kubeletEnabled turns the simulated kubelet off for specs that exercise the rule
// "a Pod counts only when PodReady is True".
var kubeletEnabled atomic.Bool

func init() { kubeletEnabled.Store(true) }

// kubeletSetsReady is the simulated kubelet: it sets Ready=True on a Pod whose
// containers are ready and whose model-ready gate was just set True. Failures are
// ignored on purpose (a conflicting write just means the Pod is not Ready yet).
func kubeletSetsReady(ctx context.Context, c client.Client, pod *corev1.Pod) {
	if !kubeletEnabled.Load() {
		return
	}
	cur := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), cur); err != nil {
		return
	}
	if cur.DeletionTimestamp != nil || podReady(cur) || !containersReady(cur) || !gateTrue(cur) {
		return
	}
	cur.Status.Conditions = append(cur.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()})
	_ = c.Status().Update(ctx, cur)
}

// withoutKubelet disables the simulated kubelet for the rest of the current spec.
func withoutKubelet() {
	kubeletEnabled.Store(false)
	DeferCleanup(func() { kubeletEnabled.Store(true) })
}

// safeClock is a concurrency-safe injectable clock for tests whose reconcile
// starts a background goroutine (the async evaluator) that reads r.Now while the
// test advances the clock on the main goroutine. A plain time.Time var races
// under -race; safeClock guards it with a mutex.
type safeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newSafeClock() *safeClock { return &safeClock{t: time.Now()} }

func (c *safeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *safeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// withPodOwnership enables the controller-chain ownership check for the
// rest of the current spec (the suite default relaxes it for legacy fixtures).
func withPodOwnership() {
	enforcePodOwnership.Store(true)
	DeferCleanup(func() { enforcePodOwnership.Store(false) })
}

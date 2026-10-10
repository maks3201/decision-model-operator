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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
)

// This suite is the envtest regression for a P0 reproduced on kind: with a
// non-default --ollaya-registry, a candidate-only spec.model change whose resolve
// fails ROLLED the serving stable Deployment. The root cause is that the serving Pod template carries an
// OLLAYA_REGISTRY env derived from the manager flag (e.registryURL), not from the
// recorded revision; a flag change (or restart against a different/broken
// registry) re-rendered the stable template, which freezeFromLive did not catch,
// so the stable Deployment generation bumped and the single replica rolled onto a
// Pod that could not serve the recorded model.
//
// Unlike the fakeEngine suite (stable_regate_identity_test.go), these specs drive
// the REAL ollaya engine so OLLAYA_REGISTRY is actually rendered, and change the
// engine's registry between the stable being promoted and the failing candidate
// reconcile. The invariant: a running stable's Pod template is byte-identical and
// its Deployment is not rolled when only the registry flag and/or the candidate
// changes.
var _ = Describe("the serving stable template survives a registry flag change", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
		srv       *httptest.Server
		regHost   string
	)
	manifestBody := []byte(`{"schemaVersion":2}`)
	wantDigest := func() string {
		sum := sha256.Sum256(manifestBody)
		return hex.EncodeToString(sum[:])
	}()

	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	markJobComplete := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	mkGatedPod := func(name, rev, pod string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = "10.0.0.161"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	podGateTrue := func(pod string) bool {
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pod}, p)).To(Succeed())
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}
	// newReconcilerFor builds a reconciler whose real ollaya engine is pointed at
	// registryURL (a non-default http:// host, so OLLAYA_REGISTRY is rendered).
	newReconcilerFor := func(registryURL, allowHost string) *DecisionModelReconciler {
		eng := ollaya.New(ollaya.WithRegistryURL(registryURL))
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                 map[string]engine.Engine{"ollaya": eng},
			Prober:                  &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			AllowedRegistries:       []string{allowHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(128),
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("stable-registry-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		u, perr := neturl.Parse(srv.URL)
		Expect(perr).NotTo(HaveOccurred())
		regHost = strings.ToLower(u.Host)
	})
	AfterEach(func() {
		if srv != nil {
			srv.Close()
		}
	})

	// driveReady brings a single-replica DM to Ready through the real engine at
	// srv, returning the reconciler and the stable revision hash.
	driveReady := func(name string) (*DecisionModelReconciler, string) {
		r := newReconcilerFor(srv.URL, regHost)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		}
		rec()
		rev := getDM(name).Status.CandidateRevision.Hash
		markJobComplete(name, rev)
		rec()
		mkGatedPod(name, rev, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev))
		return r, rev
	}

	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}

	It("does not roll the stable when the registry flag changes and the candidate resolve fails", func() {
		r1, rev := driveReady("flap")

		By("the stable serving template carries OLLAYA_REGISTRY for the non-default registry")
		depBefore := stableDep("flap", rev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()
		Expect(envValue(depBefore.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).
			To(Equal(srv.URL), "precondition: the stable template points at the original registry")

		By("the manager restarts pointed at a different, now-broken registry")
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		u2, perr := neturl.Parse(srv2.URL)
		Expect(perr).NotTo(HaveOccurred())
		host2 := strings.ToLower(u2.Host)
		srv2.Close() // closed: resolve now fails with connection refused (registry outage)
		// A new reconciler with the new registry flag; both hosts allow-listed.
		r2 := newReconcilerFor(srv2.URL, host2)
		r2.AllowedRegistries = []string{regHost, host2}

		By("a candidate-only spec.model change whose resolve fails under the new registry")
		Expect(updateDM(ctx, namespace, "flap", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:multilingual"
		})).To(Succeed())
		_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "flap"}})

		By("the stable Deployment is not rolled and its template is byte-identical")
		depAfter := stableDep("flap", rev)
		Expect(depAfter.Generation).To(Equal(genBefore),
			"the stable Deployment must not be rolled by a registry flag change on a failing candidate")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore),
			"the stable Pod template must be byte-identical (OLLAYA_REGISTRY stays the recorded value)")
		Expect(envValue(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).
			To(Equal(srv.URL), "the stable keeps the registry its store was prefetched against")

		By("the serving stable Pod stays model-ready")
		Expect(podGateTrue("flap-a")).To(BeTrue())
		_ = r1 // r1 is only used to build the initial stable
	})

	It("does not roll the stable on a steady-state reconcile after the registry flag changed", func() {
		// Even without a candidate change: a plain reconcile under a different
		// registry flag (operator upgrade / restart) must not roll a running stable.
		_, rev := driveReady("steady")
		depBefore := stableDep("steady", rev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		defer srv2.Close()
		u2, perr := neturl.Parse(srv2.URL)
		Expect(perr).NotTo(HaveOccurred())
		host2 := strings.ToLower(u2.Host)
		r2 := newReconcilerFor(srv2.URL, host2)
		r2.AllowedRegistries = []string{regHost, host2}

		_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "steady"}})

		depAfter := stableDep("steady", rev)
		Expect(depAfter.Generation).To(Equal(genBefore), "a registry flag change must not roll a healthy stable")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable template is frozen from the recorded identity")
		Expect(envValue(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL))
	})
})

// envValue returns the value of the named env var, or "" if absent.
func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

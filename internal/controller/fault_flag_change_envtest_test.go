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

// A manager flag change (here the registry the serving template renders into its
// OLLAYA_REGISTRY env) must not re-render a revision whose identity is already
// recorded: not the stable being promoted, and not the target a Recreate rollback
// is restoring. These specs drive the REAL ollaya engine so OLLAYA_REGISTRY is
// actually rendered, then swap the registry flag at the transition seam and assert
// the recorded revision's Pod template is byte-identical (no roll). They
// complement stable_template_registry_test.go, which covers a steady-state /
// failing-candidate stable; these cover the PROMOTION and RECREATE-RESTORE seams.
var _ = Describe("fault seams: a flag change does not re-render a recorded revision", func() {
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
		p.Status.PodIP = "10.0.0.171"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	newReconcilerFor := func(registryURL, allowHost string) *DecisionModelReconciler {
		eng := ollaya.New(ollaya.WithRegistryURL(registryURL))
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                 map[string]engine.Engine{"ollaya": eng},
			Prober:                  &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			AllowedRegistries:       []string{allowHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(256),
		}
	}
	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	envValue := func(env []corev1.EnvVar, key string) string {
		for _, e := range env {
			if e.Name == key {
				return e.Value
			}
		}
		return ""
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("faultflag-%d", counter)
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

	// Gap: a flag change (the registry the manager renders) between the candidate
	// becoming model-ready and the Service switch must not change the template the
	// candidate is promoted with: the promoted stable's Pod template must equal the
	// evaluated candidate's (OLLAYA_REGISTRY stays the value the candidate resolved
	// and prefetched against), and the Deployment must not be rolled afterwards.
	It("promotes the candidate with its evaluated template and a later flag change does not roll it", func() {
		r := newReconcilerFor(srv.URL, regHost)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g5"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g5"}})
		}
		rec()
		rev := getDM("g5").Status.CandidateRevision.Hash
		markJobComplete("g5", rev)
		rec()
		mkGatedPod("g5", rev, "g5-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM("g5").Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		By("the promoted stable template carries the registry the candidate was evaluated/prefetched against")
		dep := stableDep("g5", rev)
		genBefore := dep.Generation
		tmplBefore := dep.Spec.Template.DeepCopy()
		Expect(envValue(dep.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL),
			"the promoted stable renders OLLAYA_REGISTRY as its evaluated candidate did")

		By("the manager restarts pointed at a different registry and reconciles the Ready stable")
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		DeferCleanup(func() { srv2.Close() })
		u2, perr := neturl.Parse(srv2.URL)
		Expect(perr).NotTo(HaveOccurred())
		host2 := strings.ToLower(u2.Host)
		r2 := newReconcilerFor(srv2.URL, host2)
		r2.AllowedRegistries = []string{regHost, host2}
		_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g5"}})

		By("the stable Deployment is not rolled and its template is byte-identical")
		depAfter := stableDep("g5", rev)
		Expect(depAfter.Generation).To(Equal(genBefore),
			"a registry flag change after promotion must not roll the stable Deployment")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore),
			"the promoted stable Pod template must stay byte-identical across a flag change")
		Expect(envValue(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL),
			"the stable keeps the registry it was promoted with")
	})

	// Gap: a Recreate stop scales the stable to 0 while the candidate starts. A
	// manager flag change (registry) during that stop must not change the template
	// the stable is restored with when the candidate fails: the restore renders the
	// target from its recorded identity, so its Pod template is byte-identical.
	It("restores a Recreate stable from its recorded template when the candidate fails after a flag change", func() {
		r := newReconcilerFor(srv.URL, regHost)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g7"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g7"}})
		}
		rec()
		rev1 := getDM("g7").Status.CandidateRevision.Hash
		markJobComplete("g7", rev1)
		rec()
		mkGatedPod("g7", rev1, "g7-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM("g7").Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		stableDepBefore := stableDep("g7", rev1)
		stableTmplBefore := stableDepBefore.Spec.Template.DeepCopy()

		By("rolling out a candidate (new model); the Recreate stop scales the stable to 0")
		Expect(updateDM(ctx, namespace, "g7", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:multilingual"
		})).To(Succeed())
		var rev2 string
		Eventually(func() string {
			rec()
			if c := getDM("g7").Status.CandidateRevision; c != nil {
				rev2 = c.Hash
			}
			return getDM("g7").Status.StableStoppedForRevision
		}, "5s", "20ms").Should(Equal(rev2), "the stable is recorded stopped for the candidate")

		By("the manager restarts pointed at a different registry mid-stop")
		srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		DeferCleanup(func() { srv2.Close() })
		u2, perr := neturl.Parse(srv2.URL)
		Expect(perr).NotTo(HaveOccurred())
		host2 := strings.ToLower(u2.Host)
		r2 := newReconcilerFor(srv2.URL, host2)
		r2.AllowedRegistries = []string{regHost, host2}
		rec2 := func() {
			_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g7"}})
		}

		By("the candidate fails to start (prefetch Job fails), triggering the restore")
		cjob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("g7", rev2)}, cjob)).To(Succeed())
		now := metav1.Now()
		cjob.Status.StartTime = &now
		cjob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
		}
		Expect(k8sClient.Status().Update(ctx, cjob)).To(Succeed())

		By("the stable is restored and its Pod template is byte-identical to before the stop")
		Eventually(func() string {
			rec2()
			return getDM("g7").Status.StableStoppedForRevision
		}, "10s", "50ms").Should(BeEmpty(), "the stop marker is cleared as the stable is restored")
		depAfter := stableDep("g7", rev1)
		Expect(depAfter.Spec.Template).To(Equal(*stableTmplBefore),
			"the restored stable renders from its recorded identity, unchanged by the flag change")
		Expect(envValue(depAfter.Spec.Template.Spec.Containers[0].Env, "OLLAYA_REGISTRY")).To(Equal(srv.URL),
			"the restored stable keeps the registry it was originally promoted with")
	})
})

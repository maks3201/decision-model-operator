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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// A bad candidate spec must never stop the operator from serving and repairing
// the running stable. These specs drive a stable to Ready, then break the live
// spec (missing model, disallowed registry, invalid runtimeVersion, transient
// resolve error) and assert the stable keeps being maintained.
var _ = Describe("a bad candidate never stops stable maintenance", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	markJobComplete := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
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
		p.Status.PodIP = "10.0.0.141"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	// newR builds a reconciler; allowedRegistries scopes the registry guard (nil
	// = allow all, so a disallowed-registry case sets it explicitly).
	newR := func(fake *fakeEngine, pr *revProber, allowed []string) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": fake}, Prober: pr,
			Recorder:          events.NewFakeRecorder(64),
			AllowedRegistries: allowed,
		}
	}

	// stable drives a single revision to Ready and returns (reconciler, engine,
	// prober, revA). No candidate is pending.
	stable := func(name string, allowed []string) (*DecisionModelReconciler, *fakeEngine, *revProber, string) {
		fake := newFakeEngine()
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, allowed)
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
		revA := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		pr.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJobComplete(name, revA)
		rec()
		mkGatedPod(name, revA, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return r, fake, pr, revA
	}

	reconcile1 := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	cond := func(name, t string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, t)
	}

	// assertStableMaintained: delete the stable Pod, reconcile, and assert the
	// operator recreates it and gates it True (serving/repair continues), the
	// phase stays Ready and Ready=True, while the given candidate-failure
	// condition is surfaced.
	assertStableMaintained := func(r *DecisionModelReconciler, name, revA, failCondType, failReason string) {
		// The stable Pod is deleted (e.g. node drain). Maintenance must recreate it.
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name + "-a"}})).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-a"}, &corev1.Pod{}))
		}, "5s", "50ms").Should(BeTrue())

		reconcile1(r, name)
		// The stable Deployment is still present and selects revA.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + revA}, dep)).
			To(Succeed(), "stable Deployment maintained despite the bad candidate")
		Expect(dep.Spec.Template.Labels[decisionmodelv1alpha1.LabelRevision]).To(Equal(revA))

		// Recreate the (kubelet-less) stable Pod and let maintenance gate it True.
		mkGatedPod(name, revA, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { reconcile1(r, name); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady), "stable stays Ready")

		ready := cond(name, decisionmodelv1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionTrue), "Ready=True while the stable is model-ready")
		// The candidate failure is surfaced (never fails the object).
		fc := cond(name, failCondType)
		Expect(fc).NotTo(BeNil())
		if failCondType == decisionmodelv1alpha1.ConditionResolved {
			Expect(fc.Status).To(Equal(metav1.ConditionFalse))
		}
		Expect(cond(name, decisionmodelv1alpha1.ConditionDegraded).Reason).To(Equal(failReason))
		Expect(getDM(name).Status.FailedRevision).To(BeNil(), "nothing is marked failed while a stable serves")
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("preflight-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("keeps serving the stable when the new model tag does not exist, then rolls out once fixed", func() {
		r, fake, pr, revA := stable("missing", nil)
		// Point the model at a missing tag: resolve returns ErrNotFound.
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "missing", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())

		assertStableMaintained(r, "missing", revA, decisionmodelv1alpha1.ConditionResolved, reasonModelNotFound)
		Expect(cond("missing", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonModelNotFound))

		// Fix the spec: a valid model resolves and a normal candidate starts.
		fake.mu.Lock()
		fake.resolveErr = nil
		fake.digest = "cafe290000000000000000000000000000000000000000000000000000000000"
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "missing", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcile1(r, "missing")
		Expect(getDM("missing").Status.CandidateRevision).NotTo(BeNil(), "a valid spec resumes the rollout")
		pr.set(getDM("missing").Status.CandidateRevision.Hash, engine.Loaded{})
	})

	It("keeps serving the stable when the new model's registry is not allowed", func() {
		r, _, _, revA := stable("registry", []string{"ollaya.dev"})
		Expect(updateDM(ctx, namespace, "registry", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "evil.example.com/laya:en"
		})).To(Succeed())
		assertStableMaintained(r, "registry", revA, decisionmodelv1alpha1.ConditionReady, reasonRegistryNotAllowed)
	})

	It("keeps serving the stable when spec.runtimeVersion is invalid", func() {
		r, _, _, revA := stable("rtver", nil)
		Expect(updateDM(ctx, namespace, "rtver", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1" // below the engine minimum -> invalid
		})).To(Succeed())
		assertStableMaintained(r, "rtver", revA, decisionmodelv1alpha1.ConditionReady, reasonInvalidRuntimeVersion)
	})

	It("still fails when the model tag is missing and there is NO stable", func() {
		fake := newFakeEngine()
		fake.resolveErr = engine.ErrNotFound
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, nil)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "nostable"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:missing", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		reconcile1(r, "nostable")
		dm := getDM("nostable")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed), "no stable: a missing tag fails as before")
		Expect(cond("nostable", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonModelNotFound))
	})

	It("maintains the stable and marks nothing failed on a transient resolve error", func() {
		r, fake, _, revA := stable("transient", nil)
		// A registry 5xx (non-NotFound) during resolve.
		fake.mu.Lock()
		fake.resolveErr = fmt.Errorf("registry 503 service unavailable")
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "transient", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())

		reconcile1(r, "transient")
		// Nothing failed; the stable Deployment is intact and Resolved=False/ResolveFailed.
		Expect(getDM("transient").Status.FailedRevision).To(BeNil())
		Expect(getDM("transient").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "transient-" + revA}, &appsv1.Deployment{})).To(Succeed())
		rc := cond("transient", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonResolveFailed))
	})
})

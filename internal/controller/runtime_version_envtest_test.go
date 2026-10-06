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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("runtime version and rollout budget", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	newR := func(policy string, maxRollouts int) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:               map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder:              events.NewFakeRecorder(256),
			RuntimeVersionPolicy:  policy,
			MaxConcurrentRollouts: maxRollouts,
			// Scope the fleet-budget count to this test's namespace so DecisionModels
			// left behind by other specs in the shared envtest cluster are not counted.
			WatchNamespaces: []string{namespace},
		}
	}

	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}

	markJob := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	gatedPod := func(name, rev string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.70"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModelSpec)) {
		spec := decisionmodelv1alpha1.DecisionModelSpec{
			Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
		}
		if mutate != nil {
			mutate(&spec)
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: spec,
		})).To(Succeed())
	}

	// driveReady brings a DM to Ready through its full first rollout.
	driveReady := func(r *DecisionModelReconciler, name, rev string) {
		rec(r, name)
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("rtver-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("rejects runtimeVersion together with image (CEL)", func() {
		err := k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "both"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Image: "ghcr.io/x/y:1", RuntimeVersion: "0.10.0",
			},
		})
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want Invalid, got %v", err)
	})

	It("records an explicit runtimeVersion and uses its image in the revision", func() {
		r := newR(RuntimeVersionPinned, 0)
		createDM("pinned", func(s *decisionmodelv1alpha1.DecisionModelSpec) { s.RuntimeVersion = "0.9.0" })
		rev := RevisionHash(getDM("pinned").Spec, defaultDigest, "ghcr.io/ollaya-dev/ollaya:0.9.0")
		driveReady(r, "pinned", rev)

		dm := getDM("pinned")
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev))
		Expect(dm.Status.StableRevision.RuntimeVersion).To(Equal("0.9.0"))
		Expect(dm.Status.StableRevision.Image).To(Equal("ghcr.io/ollaya-dev/ollaya:0.9.0"))
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pinned-" + rev}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal("ghcr.io/ollaya-dev/ollaya:0.9.0"))
	})

	It("rejects a too-old runtimeVersion as Degraded/InvalidRuntimeVersion", func() {
		r := newR(RuntimeVersionPinned, 0)
		createDM("tooold", func(s *decisionmodelv1alpha1.DecisionModelSpec) { s.RuntimeVersion = "0.1.0" })
		rec(r, "tooold")
		dm := getDM("tooold")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		ready := meta_Find(dm, decisionmodelv1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(reasonInvalidRuntimeVersion))
		// No candidate recorded and no serving Deployment created.
		Expect(dm.Status.CandidateRevision).To(BeNil())
		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(ctx, deps, client.InNamespace(namespace),
			client.MatchingLabels{decisionmodelv1alpha1.LabelName: "tooold"})).To(Succeed())
		Expect(deps.Items).To(BeEmpty())
	})

	It("surfaces RuntimeUpdateAvailable when pinned below the default", func() {
		r := newR(RuntimeVersionPinned, 0)
		createDM("behind", func(s *decisionmodelv1alpha1.DecisionModelSpec) { s.RuntimeVersion = "0.9.0" })
		rev := RevisionHash(getDM("behind").Spec, defaultDigest, "ghcr.io/ollaya-dev/ollaya:0.9.0")
		driveReady(r, "behind", rev)
		cond := meta_Find(getDM("behind"), decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Message).To(ContainSubstring(fakeDefaultRuntimeVersion))
	})

	// Pinned policy: a DM with no spec.runtimeVersion records the engine default on
	// its stable; a later model change reuses that recorded version rather than
	// re-resolving against a (possibly newer) operator default.
	It("reuses the stable runtime version for a later change under Pinned", func() {
		r := newR(RuntimeVersionPinned, 0)
		createDM("reuse", nil) // no spec.runtimeVersion -> follows policy
		rev := RevisionHash(getDM("reuse").Spec, defaultDigest, fakeImage)
		driveReady(r, "reuse", rev)
		Expect(getDM("reuse").Status.StableRevision.RuntimeVersion).To(Equal(fakeDefaultRuntimeVersion))

		// Change the model: under Pinned the candidate reuses the stable's recorded
		// version (the engine default at promotion time), not a re-resolved default.
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = "d9d9990000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "reuse", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		rec(r, "reuse")
		cand := getDM("reuse").Status.CandidateRevision
		Expect(cand).NotTo(BeNil())
		Expect(cand.RuntimeVersion).To(Equal(fakeDefaultRuntimeVersion))
	})

	// Fleet budget: with max 1 concurrent rollout, a second fresh candidate waits
	// in Pending/RolloutQueued; it is admitted after the first promotes.
	It("queues rollouts beyond the budget and releases them FIFO", func() {
		r := newR(RuntimeVersionPinned, 1)
		createDM("a", nil)
		createDM("b", nil)
		revA := RevisionHash(getDM("a").Spec, defaultDigest, fakeImage)

		// Start A (admitted, now has a candidate). B wants to start but is queued.
		rec(r, "a")
		Expect(getDM("a").Status.CandidateRevision).NotTo(BeNil())
		rec(r, "b")
		bdm := getDM("b")
		Expect(bdm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending))
		Expect(meta_Find(bdm, decisionmodelv1alpha1.ConditionReady).Reason).To(Equal(reasonRolloutQueued))
		Expect(bdm.Status.CandidateRevision).To(BeNil(), "queued DM has no candidate yet")

		// Finish A's rollout -> it promotes and frees the slot.
		driveReady(r, "a", revA)
		Expect(getDM("a").Status.CandidateRevision).To(BeNil())

		// B is now admitted on the next reconcile.
		revB := RevisionHash(getDM("b").Spec, defaultDigest, fakeImage)
		driveReady(r, "b", revB)
		Expect(getDM("b").Status.StableRevision.Hash).To(Equal(revB))
	})

	It("does not queue when the budget is unlimited (0)", func() {
		r := newR(RuntimeVersionPinned, 0)
		createDM("u1", nil)
		createDM("u2", nil)
		rec(r, "u1")
		rec(r, "u2")
		Expect(getDM("u1").Status.CandidateRevision).NotTo(BeNil())
		Expect(getDM("u2").Status.CandidateRevision).NotTo(BeNil())
	})

	// newRv builds a reconciler whose engine advertises a specific default runtime
	// version (empty = the baseline default), to simulate an operator upgrade that
	// ships a newer default on a fresh build/restart.
	newRv := func(policy, defaultVersion string) *DecisionModelReconciler {
		r := newR(policy, 0)
		r.Engines["ollaya"] = &fakeEngine{digest: defaultDigest, defaultVersion: defaultVersion}
		return r
	}

	// The core promise: an operator upgrade that bumps the default runtime
	// image does NOT start a rollout under Pinned; it only surfaces the update.
	It("Pinned: an engine default bump starts no rollout and surfaces RuntimeUpdateAvailable", func() {
		r := newRv(RuntimeVersionPinned, "") // baseline default 0.10.0
		createDM("up-pin", nil)
		rev := RevisionHash(getDM("up-pin").Spec, defaultDigest, fakeImage)
		driveReady(r, "up-pin", rev)
		Expect(getDM("up-pin").Status.StableRevision.RuntimeVersion).To(Equal(fakeDefaultRuntimeVersion))

		// Operator upgrade: a fresh reconciler whose engine default is newer.
		r2 := newRv(RuntimeVersionPinned, "0.11.0")
		for i := 0; i < 3; i++ {
			Expect(rec(r2, "up-pin")).To(Equal(decisionmodelv1alpha1.PhaseReady))
		}
		dm := getDM("up-pin")
		Expect(dm.Status.CandidateRevision).To(BeNil(), "no rollout on an operator default bump under Pinned")
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev), "revision hash unchanged")
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "up-pin-" + rev}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal(fakeImage), "Deployment template unchanged")
		cond := meta_Find(dm, decisionmodelv1alpha1.ConditionRuntimeUpdateAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Message).To(ContainSubstring("0.11.0"))
	})

	// Under FollowOperator the same bump rolls the DecisionModel exactly once.
	It("FollowOperator: an engine default bump starts exactly one candidate", func() {
		r := newRv(RuntimeVersionFollowOperator, "")
		createDM("up-follow", nil)
		rev := RevisionHash(getDM("up-follow").Spec, defaultDigest, fakeImage)
		driveReady(r, "up-follow", rev)

		r2 := newRv(RuntimeVersionFollowOperator, "0.11.0")
		rec(r2, "up-follow")
		cand := getDM("up-follow").Status.CandidateRevision
		Expect(cand).NotTo(BeNil(), "FollowOperator rolls onto the new default")
		newRev := RevisionHash(getDM("up-follow").Spec, defaultDigest, "ghcr.io/ollaya-dev/ollaya:0.11.0")
		Expect(cand.Hash).To(Equal(newRev))
		Expect(cand.RuntimeVersion).To(Equal("0.11.0"))
		// Steady state: no second candidate churned.
		stable := getDM("up-follow").Status.StableRevision.Hash
		for i := 0; i < 3; i++ {
			rec(r2, "up-follow")
		}
		Expect(getDM("up-follow").Status.CandidateRevision.Hash).To(Equal(newRev), "exactly one candidate")
		Expect(getDM("up-follow").Status.StableRevision.Hash).To(Equal(stable))
	})

	// A pre-change stable (no runtimeVersion in status, a default image recorded)
	// must not roll under Pinned when the engine default bumps.
	It("Pinned: a pre-existing stable without a recorded runtimeVersion does not roll", func() {
		r := newRv(RuntimeVersionPinned, "")
		createDM("up-legacy", nil)
		rev := RevisionHash(getDM("up-legacy").Spec, defaultDigest, fakeImage)
		driveReady(r, "up-legacy", rev)
		// Simulate a stable recorded by an older operator: clear the version field,
		// leaving only the default image recorded.
		Expect(updateDMStatus(ctx, namespace, "up-legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision.RuntimeVersion = ""
		})).To(Succeed())

		r2 := newRv(RuntimeVersionPinned, "0.11.0")
		for i := 0; i < 3; i++ {
			Expect(rec(r2, "up-legacy")).To(Equal(decisionmodelv1alpha1.PhaseReady))
		}
		Expect(getDM("up-legacy").Status.CandidateRevision).To(BeNil(),
			"a legacy stable's runtime version is derived from its image and pinned")
		Expect(getDM("up-legacy").Status.StableRevision.Hash).To(Equal(rev))
	})

	// Without the RuntimeVersioner capability a set spec.runtimeVersion is Degraded.
	It("engine without the capability: runtimeVersion is RuntimeVersionUnsupported", func() {
		r := newR(RuntimeVersionPinned, 0)
		r.Engines["ollaya"] = newNoRuntimeVersionEngine()
		createDM("no-cap", func(s *decisionmodelv1alpha1.DecisionModelSpec) { s.RuntimeVersion = "0.10.0" })
		rec(r, "no-cap")
		dm := getDM("no-cap")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(meta_Find(dm, decisionmodelv1alpha1.ConditionReady).Reason).To(Equal(reasonRuntimeVersionUnsupported))
	})
})

// noRuntimeVersionEngine wraps a fakeEngine but hides the RuntimeVersioner
// capability. It embeds the engine.Engine *interface* (so RuntimeVersioner's
// concrete methods are not promoted) and re-adds RegistryHost + CanonicalName so
// the controller still passes the registry/name guards and reaches the
// runtime-version validation with an engine that cannot pin versions.
type noRuntimeVersionEngine struct {
	engine.Engine
	f *fakeEngine
}

func newNoRuntimeVersionEngine() noRuntimeVersionEngine {
	f := newFakeEngine()
	return noRuntimeVersionEngine{Engine: f, f: f}
}
func (e noRuntimeVersionEngine) RegistryHost(name string) (string, bool, error) {
	return e.f.RegistryHost(name)
}
func (e noRuntimeVersionEngine) CanonicalName(name string) (string, error) {
	return e.f.CanonicalName(name)
}

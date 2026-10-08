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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// The fleet rollout budget counts a DecisionModel as an active rollout while it
// still keeps a previous revision inside its stabilization window (the second
// GPU/disk the budget bounds), and a queued candidate keeps its current window's
// rollback protection until it is actually admitted.
var _ = Describe("rollout budget and the stabilization window", func() {
	const digest2 = "b7b7120000000000000000000000000000000000000000000000000000000000"
	var (
		ctx       context.Context
		namespace string
		counter   int
		clock     time.Time
	)
	int32Ptr := func(v int32) *int32 { return &v }

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	newRec := func(prober *revProber, maxRollouts int) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:               map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                prober,
			Recorder:              events.NewFakeRecorder(256),
			MaxConcurrentRollouts: maxRollouts,
			WatchNamespaces:       []string{namespace},
			Now:                   func() time.Time { return clock },
		}
	}
	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}
	markJob := func(name, rev string) {
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
	gatedPod := func(name, rev, ip string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = ip
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// drive1 brings a brand-new DM to Ready (first revision, no previous).
	drive1 := func(r *DecisionModelReconciler, prober *revProber, name, ip string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		prober.fallback = engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}
		prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		rec(r, name)
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev, ip)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	// stabilize promotes a second revision so name ends in its post-promotion
	// stabilization window (previousRevision == rev1). Returns (rev1, rev2).
	stabilize := func(r *DecisionModelReconciler, prober *revProber, name, ip string) (string, string) {
		rev1 := drive1(r, prober, name, ip)
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, digest2, fakeImage)
		prober.fallback = engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"}
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})
		rec(r, name)
		markJob(name, rev2)
		rec(r, name)
		gatedPod(name, rev2, ip)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev2))
		Expect(getDM(name).Status.PreviousRevision).NotTo(BeNil())
		Expect(getDM(name).Status.PreviousRevision.Hash).To(Equal(rev1))
		return rev1, rev2
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("budgetstab-%d", counter)
		clock = time.Now()
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Bug 1: a stabilizing DM counts as an active rollout, so a different DM's new
	// candidate is queued until the window ends.
	It("counts a stabilizing revision against the budget; a different DM queues until the window ends", func() {
		prober := &revProber{}
		r := newRec(prober, 1)
		stabilize(r, prober, "bg-a", "10.0.0.60") // A is now stabilizing (window active)
		// A's stable is rendered from its recorded identity, so B can resolve
		// laya:en to the baseline digest without disturbing A.
		r.Engines["ollaya"].(*fakeEngine).digest = defaultDigest

		// B wants to start a first rollout: A's window occupies the only slot.
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "bg-b"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		Expect(rec(r, "bg-b")).To(Equal(decisionmodelv1alpha1.PhasePending))
		Expect(meta_Find(getDM("bg-b"), decisionmodelv1alpha1.ConditionReady).Reason).
			To(Equal(reasonRolloutQueued))
		Expect(getDM("bg-b").Status.CandidateRevision).To(BeNil())

		// A's window ends (healthy): it frees the slot.
		clock = clock.Add(10 * time.Minute)
		rec(r, "bg-a")
		Expect(getDM("bg-a").Status.PreviousRevision).To(BeNil(), "A's window ended")

		// B is now admitted.
		Eventually(func() *decisionmodelv1alpha1.RevisionStatus {
			rec(r, "bg-b")
			return getDM("bg-b").Status.CandidateRevision
		}, "5s", "50ms").ShouldNot(BeNil())
	})

	// Requirement 3: a stabilizing DM that gets a new spec reuses its own slot (it
	// already holds one via its window) and is admitted, not queued behind itself.
	// The window/previousRevision is cleared only AFTER admission.
	It("admits a stabilizing DM's own new candidate into its slot and clears the window only after admission", func() {
		prober := &revProber{}
		r := newRec(prober, 1)
		rev1, rev2 := stabilize(r, prober, "bg-self", "10.0.0.61")
		_ = rev1

		// Give the same DM a third revision while still inside rev2's window.
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = "c8c8340000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "bg-self", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "jevk5:en"
		})).To(Succeed())
		rev3 := RevisionHash(getDM("bg-self").Spec, "c8c8340000000000000000000000000000000000000000000000000000000000", fakeImage)
		Expect(rev3).NotTo(Equal(rev2))

		// Reconcile: admitted into its own slot (not Pending), window ended.
		ph := rec(r, "bg-self")
		Expect(ph).NotTo(Equal(decisionmodelv1alpha1.PhasePending), "must not wait behind itself")
		dm := getDM("bg-self")
		Expect(dm.Status.CandidateRevision).NotTo(BeNil())
		Expect(dm.Status.CandidateRevision.Hash).To(Equal(rev3))
		Expect(dm.Status.PreviousRevision).To(BeNil(), "the old window is ended once the new candidate is admitted")
		Expect(meta_Find(dm, decisionmodelv1alpha1.ConditionStabilizing)).To(BeNil())
	})

	// Bug 2 / the budget never blocks a rollback: the stable path (which owns the
	// stabilization rollback) is not gated by the budget. A stabilizing DM whose
	// new stable turns unhealthy rolls back even while another DM holds the only
	// rollout slot.
	It("rolls back an unhealthy stabilizing stable even when the fleet budget is full", func() {
		prober := &revProber{}
		r := newRec(prober, 1)

		// A is stabilizing first, so its window holds the only slot.
		rev1, rev2 := stabilize(r, prober, "bg-roll", "10.0.0.62")
		r.Engines["ollaya"].(*fakeEngine).digest = defaultDigest

		// B then tries a first rollout and is queued behind A's window.
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "bg-busy"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		Expect(rec(r, "bg-busy")).To(Equal(decisionmodelv1alpha1.PhasePending), "B queued behind A's window")

		// A's new stable then loses the model. A takes the STABLE path (spec
		// unchanged), never the budget gate, so it rolls back to its previous
		// revision even though a queued DM is waiting.
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})
		clock = clock.Add(2 * time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "bg-roll") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		dm := getDM("bg-roll")
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev1), "rolled back to previous despite a full budget")
		Expect(dm.Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(dm.Status.PreviousRevision).To(BeNil())
	})
})

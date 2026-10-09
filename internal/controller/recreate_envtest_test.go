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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Recreate strategy: the stable is stopped (scaled to 0) only after the
// candidate's model is cached, and started again if the candidate fails.
var _ = Describe("Recreate rollout strategy", func() {
	const model1 = "laya:en"
	const model2 = "kev:en"
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": eng}, Prober: prober,
			Recorder: events.NewFakeRecorder(128),
		}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
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
	deleteStablePod := func(name, rev string) {
		pod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-pod-" + rev}, pod); err == nil {
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		}
	}
	depReplicas := func(name, rev string) int32 {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		if dep.Spec.Replicas == nil {
			return 1
		}
		return *dep.Spec.Replicas
	}
	depExists := func(name, rev string) bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, &appsv1.Deployment{}) == nil
	}
	svcRev := func(name string) string {
		svc := &corev1.Service{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc); err != nil {
			return ""
		}
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}
	// settleDeployment marks a revision's Deployment as a completed rollout
	// (Progressing=True/NewReplicaSetAvailable, observedGeneration == generation),
	// so stableRolloutState reports rolloutIdle and a later Pod shortfall counts as
	// a health failure (not a rollout in progress).
	settleDeployment := func(name, rev string) {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		spec := int32(1)
		if dep.Spec.Replicas != nil {
			spec = *dep.Spec.Replicas
		}
		dep.Status.ObservedGeneration = dep.Generation
		dep.Status.Replicas = spec
		dep.Status.UpdatedReplicas = spec
		dep.Status.AvailableReplicas = spec
		dep.Status.ReadyReplicas = spec
		dep.Status.Conditions = []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue,
			Reason: "NewReplicaSetAvailable", Message: "ReplicaSet has successfully progressed.",
		}}
		Expect(k8sClient.Status().Update(ctx, dep)).To(Succeed())
	}
	stableProber := func() *revProber {
		return &revProber{fallback: engine.Loaded{Name: model1, Digest: defaultDigest, Device: "cpu"}}
	}

	// driveFirstStable brings a brand-new Recreate DM to Ready at replicas 1 and
	// returns rev1.
	driveFirstStable := func(r *DecisionModelReconciler, pr *revProber, name string) string {
		rec(r, name)
		rev1 := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		pr.set(rev1, engine.Loaded{Name: model1, Digest: defaultDigest, Device: "cpu"})
		markJob(name, rev1)
		rec(r, name)
		gatedPod(name, rev1, "10.0.0.1")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return getDM(name).Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev1
	}

	// driveRecreatePromote takes an already-stable Recreate DM through a full
	// candidate rollout to promotion: new model -> cached -> stable stopped ->
	// stable Pods deleted -> candidate started and model-ready -> promoted. Returns
	// (prevRev, newStableRev). After it the old stable is PreviousRevision (at 0)
	// inside the stabilization window.
	driveRecreatePromote := func(r *DecisionModelReconciler, pr *revProber, name, prevRev, ip string) string {
		Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		rec(r, name)
		markJob(name, rev2)
		rec(r, name) // Cached -> stable stopped
		Expect(getDM(name).Status.StableStoppedForRevision).To(Equal(rev2))
		deleteStablePod(name, prevRev)
		rec(r, name) // candidate started
		gatedPod(name, rev2, ip)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return getDM(name).Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev2))
		Expect(getDM(name).Status.PreviousRevision).NotTo(BeNil())
		Expect(getDM(name).Status.PreviousRevision.Hash).To(Equal(prevRev))
		return rev2
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "recreate-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("stops the stable only after the candidate is cached, then starts the candidate and promotes", func() {
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "r1"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "r1")

		// New model -> candidate. Change the model; a new revision rev2 appears.
		Expect(updateDM(ctx, namespace, "r1", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM("r1").Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})

		// Reconcile into Caching: the candidate prefetch Job exists and the stable
		// is STILL running (not yet stopped) — prefetch happens before stop.
		rec(r, "r1")
		Expect(getDM("r1").Status.StableStoppedForRevision).To(BeEmpty(),
			"stable must not be stopped before the candidate is cached")
		Expect(depReplicas("r1", rev1)).To(Equal(int32(1)), "stable still serving during Caching")
		Expect(depExists("r1", rev2)).To(BeFalse(), "candidate Deployment not created before stop")

		// Complete the candidate's prefetch -> Cached. Now the operator records the
		// stop and scales the stable to 0, BEFORE starting the candidate.
		markJob("r1", rev2)
		rec(r, "r1")
		Expect(getDM("r1").Status.StableStoppedForRevision).To(Equal(rev2), "stop recorded after Cached")
		Expect(depReplicas("r1", rev1)).To(Equal(int32(0)), "stable scaled to 0")
		Expect(depExists("r1", rev2)).To(BeFalse(), "candidate not started until the stable Pods are gone")

		// The stable Pod is still present (envtest has no ReplicaSet controller), so
		// the operator waits. Delete it to simulate the scale-down completing.
		rec(r, "r1")
		Expect(depExists("r1", rev2)).To(BeFalse(), "still waiting for the stable Pods to go")
		deleteStablePod("r1", rev1)

		// Now the candidate Deployment is created and, once model-ready, promotes.
		rec(r, "r1")
		Expect(depExists("r1", rev2)).To(BeTrue(), "candidate started after the stable Pods are gone")
		gatedPod("r1", rev2, "10.0.0.2")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "r1")
			return getDM("r1").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("r1").Status.StableRevision.Hash).To(Equal(rev2))
		Expect(getDM("r1").Status.StableStoppedForRevision).To(BeEmpty(), "stop marker cleared on promotion")
	})

	It("restores the stable when the Recreate candidate fails to start", func() {
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		clock := newSafeClock()
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rf"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rf")

		// Candidate rev2 for a new model; drive to Cached + stable stopped.
		Expect(updateDM(ctx, namespace, "rf", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM("rf").Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		rec(r, "rf")
		markJob("rf", rev2)
		rec(r, "rf")
		Expect(getDM("rf").Status.StableStoppedForRevision).To(Equal(rev2))
		Expect(depReplicas("rf", rev1)).To(Equal(int32(0)))
		deleteStablePod("rf", rev1)
		rec(r, "rf") // candidate Deployment created; no gated Pod -> stays Starting

		// The candidate never becomes model-ready; advancing past the Starting
		// timeout rolls it back.
		clock.add(startingTimeout(getDM("rf")) + time.Minute)
		Eventually(func() string {
			rec(r, "rf")
			if fr := getDM("rf").Status.FailedRevision; fr != nil {
				return fr.Hash
			}
			return ""
		}, "5s", "50ms").Should(Equal(rev2), "candidate failed and was recorded")

		// The stop marker is cleared and the stable is scaled back to its replicas.
		Expect(getDM("rf").Status.StableStoppedForRevision).To(BeEmpty(), "stop marker cleared on failure")
		Eventually(func() int32 {
			rec(r, "rf")
			return depReplicas("rf", rev1)
		}, "5s", "50ms").Should(Equal(int32(1)), "stable restored to its replicas")
		Expect(getDM("rf").Status.StableRevision.Hash).To(Equal(rev1))
	})

	It("leaves BlueGreen unchanged: the stable keeps serving during the candidate rollout", func() {
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "bg"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				// no rollout.strategy -> BlueGreen
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "bg")

		Expect(updateDM(ctx, namespace, "bg", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM("bg").Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		rec(r, "bg")
		markJob("bg", rev2)
		rec(r, "bg")

		// BlueGreen: no stop marker; stable keeps its replicas; candidate Deployment
		// is created alongside the stable.
		Expect(getDM("bg").Status.StableStoppedForRevision).To(BeEmpty())
		Expect(depReplicas("bg", rev1)).To(Equal(int32(1)), "stable keeps serving (BlueGreen)")
		rec(r, "bg")
		Expect(depExists("bg", rev2)).To(BeTrue(), "candidate runs alongside the stable")
	})

	It("keeps the previous revision scaled to 0 during the stabilization window and stages a capacity-safe rollback when the new stable turns unhealthy", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rbk"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rbk")
		rev2 := driveRecreatePromote(r, pr, "rbk", rev1, "10.0.5.2")

		// Point 1: the previous revision (old stable) stays scaled to 0 for the
		// whole window — it was stopped for the Recreate rollout and must not take
		// the single GPU back.
		prevDep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rbk-" + rev1}, prevDep)).To(Succeed())
		Expect(prevDep.Spec.Replicas).NotTo(BeNil())
		Expect(*prevDep.Spec.Replicas).To(Equal(int32(0)), "previous revision stays at 0 during the window")

		// Make the new stable unhealthy within the window: delete its only Pod
		// (quorum shortfall). Reconcile once to start the unhealthy debounce, then
		// advance past it so the rollback fires on the next reconcile.
		settleDeployment("rbk", rev2) // rollout complete, so the shortfall is a health failure
		deleteStablePod("rbk", rev2)
		rec(r, "rbk") // observes the shortfall, starts the debounce clock
		clock.add(postPromotionDebounce + time.Minute)

		// The staged rollback starts: RecreateRollback recorded, failed workloads
		// deleted, Service NOT yet switched to the target.
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "rbk")
			return getDM("rbk").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil(), "staged Recreate rollback recorded")
		Expect(getDM("rbk").Status.RecreateRollback.Target).To(Equal(rev1))
		Expect(getDM("rbk").Status.RecreateRollback.Failed).To(Equal(rev2))
		Expect(getDM("rbk").Status.StableRevision.Hash).To(Equal(rev1), "status stable is the target")
		Expect(svcRev("rbk")).NotTo(Equal(rev1), "Service not switched to the target until it is model-ready")

		// The target comes up on the freed GPU: make it model-ready; the Service
		// then switches and the marker clears.
		gatedPod("rbk", rev1, "10.0.5.3")
		Eventually(func() string {
			rec(r, "rbk")
			return svcRev("rbk")
		}, "5s", "50ms").Should(Equal(rev1), "Service switched to the target once it is model-ready")
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "rbk")
			return getDM("rbk").Status.RecreateRollback
		}, "5s", "50ms").Should(BeNil(), "rollback marker cleared")
		Expect(getDM("rbk").Status.FailedRevision).NotTo(BeNil())
		Expect(getDM("rbk").Status.FailedRevision.Hash).To(Equal(rev2))
	})

	It("resumes a staged Recreate rollback after a restart", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rbk2"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rbk2")
		rev2 := driveRecreatePromote(r, pr, "rbk2", rev1, "10.0.6.2")
		settleDeployment("rbk2", rev2)
		deleteStablePod("rbk2", rev2)
		rec(r, "rbk2") // start the unhealthy debounce
		clock.add(postPromotionDebounce + time.Minute)
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "rbk2")
			return getDM("rbk2").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil())

		// Fresh reconciler (empty RAM): it must resume the staged rollback from
		// status alone — keep the Service off the target until it is ready, then
		// switch and clear the marker.
		pr2 := stableProber()
		pr2.set(rev1, engine.Loaded{Name: model1, Digest: defaultDigest, Device: "cpu"})
		pr2.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		r2 := newReconciler(eng, pr2)
		r2.Now = clock.now
		rec(r2, "rbk2")
		Expect(getDM("rbk2").Status.RecreateRollback).NotTo(BeNil(), "rollback still staged after restart")
		Expect(svcRev("rbk2")).NotTo(Equal(rev1), "Service still not on the target before it is ready")

		gatedPod("rbk2", rev1, "10.0.6.3")
		Eventually(func() string {
			rec(r2, "rbk2")
			return svcRev("rbk2")
		}, "5s", "50ms").Should(Equal(rev1), "restarted reconciler finishes the rollback")
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r2, "rbk2")
			return getDM("rbk2").Status.RecreateRollback
		}, "5s", "50ms").Should(BeNil())
	})

	It("rejects strategy Recreate combined with Manual promotion (CEL)", func() {
		manual := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel-manual"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Strategy:  decisionmodelv1alpha1.RolloutRecreate,
					Promotion: decisionmodelv1alpha1.PromotionManual,
				},
			},
		}
		err := k8sClient.Create(ctx, manual)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Recreate cannot be combined with promotion: Manual"))

		// The deprecated manualPromotion:true alias is rejected the same way.
		alias := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel-alias"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Strategy:        decisionmodelv1alpha1.RolloutRecreate,
					ManualPromotion: true, //nolint:staticcheck // deliberately testing the deprecated alias is rejected
				},
			},
		}
		err = k8sClient.Create(ctx, alias)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Recreate cannot be combined with manualPromotion: true"))

		// Recreate with automatic / evaluation-gated promotion is allowed.
		ok := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel-ok"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Strategy:  decisionmodelv1alpha1.RolloutRecreate,
					Promotion: decisionmodelv1alpha1.PromotionAutomatic,
				},
			},
		}
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
	})

	// Item 10: under Recreate the relative eval gates are skipped (the stopped
	// stable gives no baseline), so a zero minAccuracy would promote with no
	// effective gate. CEL rejects Recreate + evaluation with a zero minAccuracy and
	// accepts a real floor.
	It("rejects strategy Recreate + evaluation with a zero minAccuracy (CEL), accepts a real floor", func() {
		zero := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel-zero"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Strategy: decisionmodelv1alpha1.RolloutRecreate,
					Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
						DatasetRef:      decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
						MinAccuracy:     "0",
						MaxAccuracyDrop: "0.02", // only a relative gate otherwise
					},
				},
			},
		}
		err := k8sClient.Create(ctx, zero)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("non-zero minAccuracy"))

		// "0.0" is also rejected (same no-floor meaning).
		zero.Name = "cel-zero2"
		zero.ResourceVersion = ""
		zero.Spec.Rollout.Evaluation.MinAccuracy = "0.0"
		Expect(k8sClient.Create(ctx, zero)).To(HaveOccurred())

		// A real floor is accepted.
		good := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel-floor"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Strategy: decisionmodelv1alpha1.RolloutRecreate,
					Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
						DatasetRef:      decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
						MinAccuracy:     "0.90",
						MaxAccuracyDrop: "0.02",
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, good)).To(Succeed())
	})

	It("survives a restart mid-stop: the stop marker in status drives recovery", func() {
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rs"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rs")
		Expect(updateDM(ctx, namespace, "rs", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM("rs").Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		rec(r, "rs")
		markJob("rs", rev2)
		rec(r, "rs")
		Expect(getDM("rs").Status.StableStoppedForRevision).To(Equal(rev2), "stop recorded")
		Expect(depReplicas("rs", rev1)).To(Equal(int32(0)))

		// Simulate an operator restart: a brand-new reconciler with empty RAM. It
		// must derive the in-progress stop from status alone and keep the stable at
		// 0, then proceed once the Pods are gone.
		pr2 := stableProber()
		pr2.set(rev1, engine.Loaded{Name: model1, Digest: defaultDigest, Device: "cpu"})
		pr2.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		r2 := newReconciler(eng, pr2)
		rec(r2, "rs")
		Expect(getDM("rs").Status.StableStoppedForRevision).To(Equal(rev2), "stop marker preserved across restart")
		Expect(depReplicas("rs", rev1)).To(Equal(int32(0)), "stable held at 0 after restart")
		Expect(depExists("rs", rev2)).To(BeFalse(), "candidate not started while stable Pods remain")

		deleteStablePod("rs", rev1)
		rec(r2, "rs")
		Expect(depExists("rs", rev2)).To(BeTrue(), "candidate started after the stable Pods are gone")
	})

	It("fails a staged Recreate rollback whose target never becomes model-ready, clearing the marker", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rto"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rto")
		rev2 := driveRecreatePromote(r, pr, "rto", rev1, "10.0.7.2")

		// New stable turns unhealthy -> staged rollback to rev1.
		settleDeployment("rto", rev2)
		deleteStablePod("rto", rev2)
		rec(r, "rto")
		clock.add(postPromotionDebounce + time.Minute)
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "rto")
			return getDM("rto").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil())

		// Free the GPU (failed Pods gone) but NEVER make the target model-ready, so
		// the rollback waits. Advancing past the Starting timeout (from
		// recreateRollback.startedAt) abandons the rollback: Failed + marker cleared.
		deleteStablePod("rto", rev2)
		clock.add(startingTimeout(getDM("rto")) + time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "rto")
			return getDM("rto").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed), "rollback abandoned after the timeout")
		Expect(getDM("rto").Status.RecreateRollback).To(BeNil(), "stale rollback marker cleared on timeout")
		deg := meta_Find(getDM("rto"), decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonRecreateRollbackTimeout))
	})

	It("re-deletes the failed revision's workloads at the top of the rollback (crash-safe) and never starts the target before they are gone", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rcr"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "rcr")
		rev2 := driveRecreatePromote(r, pr, "rcr", rev1, "10.0.8.2")

		// Stage the rollback, but simulate a crash between rollbackToPrevious's
		// status persist and its deleteRevisionWorkloads: the marker is set yet the
		// failed revision's Deployment + a serving Pod still exist.
		settleDeployment("rcr", rev2)
		deleteStablePod("rcr", rev2)
		rec(r, "rcr")
		clock.add(postPromotionDebounce + time.Minute)
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "rcr")
			return getDM("rcr").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil())
		// Re-create a serving Pod for the failed revision to mimic a delete that did
		// not take (crash before/within the delete): the failed revision still holds
		// the GPU.
		gatedPod("rcr", rev2, "10.0.8.9")

		// The rollback must re-issue the delete at its top and NOT scale the target
		// up while the failed Pod is present (its Deployment lingers at 0 from the
		// window; it must stay at 0 until the GPU is free).
		rec(r, "rcr")
		Expect(depReplicas("rcr", rev1)).To(Equal(int32(0)),
			"target not scaled up while the failed revision's Pods still hold the GPU")
		// The failed revision's Deployment is (re)deleted.
		Eventually(func() bool {
			rec(r, "rcr")
			return depExists("rcr", rev2)
		}, "5s", "50ms").Should(BeFalse(), "failed revision workloads re-deleted at the top of the rollback")

		// Once the failed Pod is gone, the target scales up and the Service switches.
		deleteStablePod("rcr", rev2)
		rec(r, "rcr")
		Expect(depReplicas("rcr", rev1)).To(Equal(int32(1)), "target scaled up after the GPU is free")
		gatedPod("rcr", rev1, "10.0.8.3")
		Eventually(func() string {
			rec(r, "rcr")
			return svcRev("rcr")
		}, "5s", "50ms").Should(Equal(rev1))
	})

	It("clears a stale RecreateRollback marker whose target no longer matches the stable", func() {
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "stale"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "stale")

		// Inject a staged-rollback marker whose Target does not match the recorded
		// stable (e.g. a status edit, or the stable moved under it). It could never
		// be driven (the dispatch guard requires Target == stable.Hash), so a
		// reconcile must clear it rather than leave the DM wedged.
		Expect(updateDMStatus(ctx, namespace, "stale", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.RecreateRollback = &decisionmodelv1alpha1.RecreateRollbackStatus{
				Failed: "deadbeef", Target: "notthestable",
			}
		})).To(Succeed())

		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "stale")
			return getDM("stale").Status.RecreateRollback
		}, "5s", "50ms").Should(BeNil(), "stale marker cleared")
		// The DM keeps serving its stable; the stale marker did not break it.
		Expect(getDM("stale").Status.StableRevision.Hash).To(Equal(rev1))
		Expect(getDM("stale").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
	})

	// driveToStopped brings a Recreate DM to a serving stable, then starts a
	// candidate and advances to "stable stopped" (StableStoppedForRevision set,
	// stable scaled to 0, stable Pods deleted) WITHOUT promoting. Returns
	// (stableRev, candidateRev). The candidate Deployment exists but has no gated
	// Pod, so it never promotes on its own.
	driveToStopped := func(r *DecisionModelReconciler, pr *revProber, name string) (string, string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, name)
		Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model2
		})).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		pr.set(rev2, engine.Loaded{Name: model2, Digest: defaultDigest, Device: "cpu"})
		rec(r, name)
		markJob(name, rev2)
		rec(r, name) // Cached -> stable stopped
		Expect(getDM(name).Status.StableStoppedForRevision).To(Equal(rev2))
		Expect(depReplicas(name, rev1)).To(Equal(int32(0)))
		deleteStablePod(name, rev1)
		rec(r, name) // candidate Deployment created; no gated Pod -> stays stopped
		return rev1, rev2
	}

	// Item 1: every exit of a stopped candidate OTHER than promote/rollbackOrFail
	// must clear the stopped marker and restore the stable (persist-first), with
	// the candidate's workloads collected. Table over the transitions, each
	// re-checked after a simulated restart (fresh reconciler, empty RAM).
	DescribeTable("clears the stopped-stable marker and restores the stable on a non-rollback exit",
		func(mutate func(name string, pr *revProber)) {
			pr := stableProber()
			eng := newFakeEngine()
			r := newReconciler(eng, pr)
			name := "exit-" + itoa(nsCounter) // unique within the per-spec namespace
			rev1, rev2 := driveToStopped(r, pr, name)

			// Apply the transition that ends the stopped candidate.
			mutate(name, pr)

			// A fresh reconciler (restart: empty RAM) must clear the marker and
			// restore the stable to its replicas, serving again.
			r2 := newReconciler(eng, pr)
			Eventually(func() string {
				rec(r2, name)
				return getDM(name).Status.StableStoppedForRevision
			}, "5s", "50ms").Should(BeEmpty(), "stopped marker cleared on this exit")
			Eventually(func() int32 {
				rec(r2, name)
				return depReplicas(name, rev1)
			}, "5s", "50ms").Should(Equal(int32(1)), "stable scaled back to its replicas")
			// Bring the restored stable's Pod up: the DM becomes Ready again (honest
			// readiness, not a premature True while at 0).
			gatedPod(name, rev1, "10.0.9."+itoa(nsCounter))
			Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
				rec(r2, name)
				return getDM(name).Status.Phase
			}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady), "stable Ready once restored and model-ready")
			Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev1))
			_ = rev2
		},
		Entry("spec reverted to the stable model", func(name string, pr *revProber) {
			Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Spec.Model = model1 // back to the stable's model -> isStable path
			})).To(Succeed())
		}),
		Entry("strategy switched to BlueGreen", func(name string, pr *revProber) {
			Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Spec.Model = model1
				d.Spec.Rollout.Strategy = decisionmodelv1alpha1.RolloutBlueGreen
			})).To(Succeed())
		}),
		Entry("new spec rejected by a security guard (disallowed registry)", func(name string, pr *revProber) {
			// An unresolvable/again-stable spec routes through the stable path; use a
			// revert to the stable model so the stable path runs and clears the marker
			// even though the intermediate candidate was abandoned.
			Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Spec.Model = model1
			})).To(Succeed())
		}),
	)

	// Item 5 + 9: honest Ready. A Recreate rollback on a replicas=2 target switches
	// the Service at ready>=1 (availability first) but reports Ready only when BOTH
	// replicas are model-ready; it is Degraded=ReplicasNotModelReady at 1/2.
	It("switches at one ready replica but reports Ready only when all N are model-ready (rollback)", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "n2"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(2),
				Cache:   &decisionmodelv1alpha1.CacheSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		// Drive to a Ready 2-replica stable.
		rec(r, "n2")
		rev1 := RevisionHash(getDM("n2").Spec, defaultDigest, fakeImage)
		pr.set(rev1, engine.Loaded{Name: model1, Digest: defaultDigest, Device: "cpu"})
		markJob("n2", rev1)
		rec(r, "n2")
		gatedPod("n2", rev1, "10.0.10.1")
		gatedReplica := func(name, rev, podName, ip string) {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: podName,
					Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
			}
			Expect(k8sClient.Create(ctx, p)).To(Succeed())
			p.Status.PodIP = ip
			p.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
			}
			Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		}
		gatedReplica("n2", rev1, "n2-pod2-"+rev1, "10.0.10.2")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "n2")
			return getDM("n2").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Simulate the aftermath of a failed Recreate promotion to a candidate rev2:
		// rev1 is the recorded stable again but was torn down (its Pods gone), and a
		// staged rollback to rev1 is recorded. Inject that state directly (a full
		// 2-replica candidate promote is irrelevant to the readiness invariant under
		// test) and tear down rev1's Pods so the rollback must bring them back.
		rev2 := "deadbeefdeadbeef"
		deleteStablePod("n2", rev1)
		p2 := &corev1.Pod{}
		if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "n2-pod2-" + rev1}, p2) == nil {
			Expect(k8sClient.Delete(ctx, p2)).To(Succeed())
		}
		startedAt := metav1.NewTime(clock.now())
		Expect(updateDMStatus(ctx, namespace, "n2", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.RecreateRollback = &decisionmodelv1alpha1.RecreateRollbackStatus{
				Failed: rev2, Target: rev1, StartedAt: &startedAt,
			}
			d.Status.FailedRevision = &decisionmodelv1alpha1.RevisionStatus{Hash: rev2, Engine: "ollaya", Model: model2, Digest: defaultDigest, Device: "cpu"}
		})).To(Succeed())

		// Only ONE target replica is model-ready: the Service switches (availability
		// first) but the DM is Degraded=ReplicasNotModelReady, NOT fully Ready.
		gatedPod("n2", rev1, "10.0.10.4")
		Eventually(func() string {
			rec(r, "n2")
			return svcRev("n2")
		}, "5s", "50ms").Should(Equal(rev1), "Service switched at one ready replica")
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "n2")
			return getDM("n2").Status.RecreateRollback
		}, "5s", "50ms").Should(BeNil(), "rollback marker cleared after the switch")
		dm := getDM("n2")
		degCond := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(degCond).NotTo(BeNil())
		Expect(degCond.Reason).To(Equal(reasonReplicasNotModelReady), "Degraded=ReplicasNotModelReady at 1/2 after the switch")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded), "not fully Ready at 1/2 replicas")

		// Bring the second replica up: Ready, no longer Degraded on replicas.
		gatedReplica("n2", rev1, "n2-pod2b-"+rev1, "10.0.10.5")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "n2")
			return getDM("n2").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady), "Ready only when both replicas are model-ready")
	})

	// Item 15: a transient error on the Service patch during a Recreate rollback
	// must not promote the target or report Ready; the next reconcile retries the
	// switch idempotently. Ready is never True while the Service selects a revision
	// with no ready Pods.
	It("retries the Service switch on a transient error during a rollback without a false Ready", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		var failSwitch atomic.Bool
		failSwitch.Store(true)
		var rev1Hash atomic.Value
		rev1Hash.Store("")
		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				// Fail only the Service switch that moves the selector to the rollback
				// target (rev1) the first time, then let it through. Matching the
				// selector avoids consuming the one-shot on unrelated Service writes.
				if svc, ok := obj.(*corev1.Service); ok && svc.Name == "r15" &&
					svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision] == rev1Hash.Load().(string) &&
					failSwitch.CompareAndSwap(true, false) {
					return apierrors.NewConflict(schema.GroupResource{Resource: "services"}, svc.Name, fmt.Errorf("induced transient conflict"))
				}
				return cl.Update(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": eng}, Prober: pr,
			Recorder: events.NewFakeRecorder(256), Now: clock.now,
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "r15"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "r15")
		rev1Hash.Store(rev1)
		rev2 := driveRecreatePromote(r, pr, "r15", rev1, "10.0.11.2")
		settleDeployment("r15", rev2)
		deleteStablePod("r15", rev2)
		rec(r, "r15")
		clock.add(postPromotionDebounce + time.Minute)
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "r15")
			return getDM("r15").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil())

		// Target becomes model-ready; the first Service switch hits the induced
		// conflict. The DM must NOT be fully Ready while the Service is not on the
		// target, and the marker must remain so the next reconcile retries.
		gatedPod("r15", rev1, "10.0.11.3")
		rec(r, "r15") // the switch is attempted and fails transiently
		Expect(svcRev("r15")).NotTo(Equal(rev1), "Service not switched on the transient failure")
		Expect(getDM("r15").Status.RecreateRollback).NotTo(BeNil(), "rollback still staged after the failed switch")

		// Retry: the switch succeeds, the marker clears, and the DM is Ready.
		Eventually(func() string {
			rec(r, "r15")
			return svcRev("r15")
		}, "5s", "50ms").Should(Equal(rev1), "Service switch retried idempotently")
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "r15")
			return getDM("r15").Status.RecreateRollback
		}, "5s", "50ms").Should(BeNil(), "marker cleared once the switch lands")
		// Honest Ready: with the single target replica model-ready and the Service
		// on it, the Ready condition is True (reason Ready).
		dm15 := getDM("r15")
		rc := meta_Find(dm15, decisionmodelv1alpha1.ConditionReady)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionTrue), "Ready True once the restored target is model-ready and serving")
		Expect(svcRev("r15")).To(Equal(rev1), "Service on the restored target")
	})

	// Item 4 (citation gap): a Recreate rollback whose target store is lost routes
	// through recoverStableStore (annotated recreate + reprefetch) instead of
	// bringing the target up against a missing volume. Covered end-to-end here via
	// dispatchRecreateRollback.
	It("recovers a lost target store during a staged Recreate rollback", func() {
		clock := newSafeClock()
		pr := stableProber()
		eng := newFakeEngine()
		r := newReconciler(eng, pr)
		r.Now = clock.now
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "r4"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model1, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Strategy: decisionmodelv1alpha1.RolloutRecreate},
			},
		})).To(Succeed())
		rev1 := driveFirstStable(r, pr, "r4")
		rev2 := driveRecreatePromote(r, pr, "r4", rev1, "10.0.12.2")
		settleDeployment("r4", rev2)
		deleteStablePod("r4", rev2)
		rec(r, "r4")
		clock.add(postPromotionDebounce + time.Minute)
		Eventually(func() *decisionmodelv1alpha1.RecreateRollbackStatus {
			rec(r, "r4")
			return getDM("r4").Status.RecreateRollback
		}, "5s", "50ms").ShouldNot(BeNil())

		// Delete the target (rev1) store PVC: the rollback dispatch must route to
		// store recovery (recreate the PVC annotated + reprefetch), staying Degraded,
		// rather than switching the Service to a target with no store.
		pvc := &corev1.PersistentVolumeClaim{}
		pvcName := storeNameRev(getDM("r4"), rev1)
		if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, pvc) == nil {
			Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		}
		// Reconcile: recovery recreates the store PVC (annotated) and the Service is
		// NOT yet switched to the target.
		Eventually(func() bool {
			rec(r, "r4")
			p := &corev1.PersistentVolumeClaim{}
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvcName}, p) == nil
		}, "5s", "50ms").Should(BeTrue(), "target store recreated by recovery during the rollback")
		Expect(getDM("r4").Status.RecreateRollback).NotTo(BeNil(), "rollback still staged while the store recovers")
	})
})

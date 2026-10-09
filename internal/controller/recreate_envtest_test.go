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
	"time"

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
})

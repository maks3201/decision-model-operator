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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// While a candidate is RolloutQueued behind the fleet budget, the serving stable
// must still be maintained (Deployment/Service/PDB recreated, API key rotated,
// Pods re-gated, and an unhealthy stable rolled back) — and no candidate object
// is created.
var _ = Describe("stable maintenance while a candidate is RolloutQueued", func() {
	const digestB = "c1c1340000000000000000000000000000000000000000000000000000000000"
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
	depExists := func(name, rev string) bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, &appsv1.Deployment{}) == nil
	}
	svcRev := func(name string) string {
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}

	// drive1 brings a brand-new DM to Ready (first revision).
	drive1 := func(r *DecisionModelReconciler, prober *revProber, name, model, ip string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		prober.set(rev, engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"})
		rec(r, name)
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev, ip)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	// occupySlot drives a holder DM into a candidate that stays in Starting (its
	// candidate Pod never becomes ready), so it holds the single rollout slot.
	occupySlot := func(r *DecisionModelReconciler, prober *revProber, name, ip string) {
		rev1 := drive1(r, prober, name, "laya:en", ip)
		_ = rev1
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digestB
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, digestB, fakeImage)
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: digestB, Device: "cpu"})
		rec(r, name)
		markJob(name, rev2)
		rec(r, name) // candidate Deployment created; no gated Pod -> holds the slot in Starting
		fake.digest = defaultDigest
		Expect(getDM(name).Status.CandidateRevision).NotTo(BeNil())
	}

	// queueCandidate gives an already-stable DM a new spec that must queue behind
	// the full budget. Returns (stableRev, candidateRev).
	queueCandidate := func(r *DecisionModelReconciler, prober *revProber, name string) (string, string) {
		stableRev := getDM(name).Status.StableRevision.Hash
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digestB
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		candRev := RevisionHash(getDM(name).Spec, digestB, fakeImage)
		prober.set(candRev, engine.Loaded{Name: "kev:en", Digest: digestB, Device: "cpu"})
		fake.digest = defaultDigest // keep the stable resolvable from its recorded identity
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "5s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhasePending), "the new candidate must queue behind the full budget")
		return stableRev, candRev
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("queuedstab-%d", counter)
		clock = time.Now()
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("recreates a deleted stable Deployment and Service while the candidate is queued, and creates no candidate object", func() {
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(proberB, 1)
		// B reaches a serving stable first (uses then frees the single slot).
		stableRev := drive1(rB, proberB, "svc-b", "laya:en", "10.0.1.2")

		// Now a holder DM occupies the only slot with a candidate stuck in Starting.
		proberHold := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rHold := newRec(proberHold, 1)
		occupySlot(rHold, proberHold, "holder", "10.0.1.1")

		// B gets a new spec: it must queue behind the full budget.
		sRev, candRev := queueCandidate(rB, proberB, "svc-b")
		Expect(sRev).To(Equal(stableRev))

		// Queued: no candidate PVC/Job/Deployment.
		Expect(depExists("svc-b", candRev)).To(BeFalse(), "no candidate Deployment while queued")
		jobErr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("svc-b", candRev)}, &batchv1.Job{})
		Expect(apierrors.IsNotFound(jobErr)).To(BeTrue(), "no candidate prefetch Job while queued")
		pvcErr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM("svc-b"), candRev)}, &corev1.PersistentVolumeClaim{})
		Expect(apierrors.IsNotFound(pvcErr)).To(BeTrue(), "no candidate store PVC while queued")

		// Delete the stable Deployment: the next (still queued) reconcile recreates it.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "svc-b-" + stableRev}, dep)).To(Succeed())
		Expect(k8sClient.Delete(ctx, dep)).To(Succeed())
		Eventually(func() bool {
			rec(rB, "svc-b")
			return depExists("svc-b", stableRev)
		}, "5s", "50ms").Should(BeTrue(), "stable Deployment recreated while queued")
		Expect(getDM("svc-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "still queued")

		// Delete the stable Service: recreated, still selecting the stable.
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "svc-b"}, svc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, svc)).To(Succeed())
		Eventually(func() string {
			rec(rB, "svc-b")
			s := &corev1.Service{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "svc-b"}, s); err != nil {
				return ""
			}
			return s.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
		}, "5s", "50ms").Should(Equal(stableRev), "stable Service recreated selecting the stable")
		Expect(svcRev("svc-b")).To(Equal(stableRev))
	})

	It("applies an API-key rotation to the stable while the candidate is queued", func() {
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(proberB, 1)
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "key", Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}},
			Data:       map[string][]byte{"token": []byte("secret-1")},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "key-b"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key"}, Key: "token",
				}},
			},
		})).To(Succeed())
		stableRev := RevisionHash(getDM("key-b").Spec, defaultDigest, fakeImage)
		proberB.set(stableRev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		rec(rB, "key-b")
		markJob("key-b", stableRev)
		rec(rB, "key-b")
		gatedPod("key-b", stableRev, "10.0.2.2")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(rB, "key-b") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Occupy the only slot with a holder, then queue a new candidate for key-b.
		proberHold := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rHold := newRec(proberHold, 1)
		occupySlot(rHold, proberHold, "holder2", "10.0.2.1")
		_, _ = queueCandidate(rB, proberB, "key-b")

		// Record the stable's current template checksum, rotate the key, and assert
		// the stable Deployment rolls (template checksum changes) while still queued.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key-b-" + stableRev}, dep)).To(Succeed())
		before := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]

		sec := &corev1.Secret{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key"}, sec)).To(Succeed())
		sec.Data["token"] = []byte("secret-2-rotated")
		Expect(k8sClient.Update(ctx, sec)).To(Succeed())

		Eventually(func() string {
			rec(rB, "key-b")
			d := &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key-b-" + stableRev}, d); err != nil {
				return ""
			}
			return d.Spec.Template.Annotations[apiKeyChecksumAnnotation]
		}, "5s", "50ms").ShouldNot(Equal(before), "API-key rotation rolled the stable template while queued")
		Expect(getDM("key-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "still queued")
	})

	It("recreates a deleted stable PodDisruptionBudget while the candidate is queued", func() {
		// A replicas>1 stable has a PDB (maxUnavailable=1). While a candidate is
		// queued the stable is fully maintained, so a hand-deleted PDB is recreated
		// without ending the queue.
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(proberB, 1)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pdb-b"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(2),
				// RWX so replicas>1 needs no co-location dance; a PDB is still created.
				Cache: &decisionmodelv1alpha1.CacheSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			},
		})).To(Succeed())
		stableRev := RevisionHash(getDM("pdb-b").Spec, defaultDigest, fakeImage)
		proberB.set(stableRev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		rec(rB, "pdb-b")
		markJob("pdb-b", stableRev)
		rec(rB, "pdb-b")
		gatedPod("pdb-b", stableRev, "10.0.3.2")
		// A second ready replica Pod with the SAME revision label (distinct name).
		pod2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "pdb-b-pod2-" + stableRev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: "pdb-b", decisionmodelv1alpha1.LabelRevision: stableRev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod2)).To(Succeed())
		pod2.Status.PodIP = "10.0.3.3"
		pod2.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod2)).To(Succeed())
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(rB, "pdb-b") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		pdbKey := types.NamespacedName{Namespace: namespace, Name: pdbName(getDM("pdb-b"), stableRev)}
		Expect(k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{})).To(Succeed(), "stable PDB exists at replicas 2")

		// Occupy the only slot, then queue a new candidate for pdb-b.
		proberHold := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rHold := newRec(proberHold, 1)
		occupySlot(rHold, proberHold, "holder3", "10.0.3.1")
		_, _ = queueCandidate(rB, proberB, "pdb-b")
		Expect(getDM("pdb-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending))

		// Delete the stable PDB: a still-queued reconcile recreates it.
		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, pdbKey, pdb)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pdb)).To(Succeed())
		Eventually(func() bool {
			rec(rB, "pdb-b")
			return k8sClient.Get(ctx, pdbKey, &policyv1.PodDisruptionBudget{}) == nil
		}, "5s", "50ms").Should(BeTrue(), "stable PDB recreated while the candidate is queued")
		Expect(getDM("pdb-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "still queued after the PDB recreate")
	})

	It("surfaces a stable health problem on Degraded while the candidate stays RolloutQueued", func() {
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(proberB, 1)
		stableRev := drive1(rB, proberB, "deg-b", "laya:en", "10.0.4.2")

		// Occupy the only slot, then queue a new candidate for deg-b.
		proberHold := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rHold := newRec(proberHold, 1)
		occupySlot(rHold, proberHold, "holder4", "10.0.4.1")
		_, _ = queueCandidate(rB, proberB, "deg-b")
		Expect(getDM("deg-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending))

		// Break the stable: delete its only model-ready Pod. While still queued, the
		// next reconcile must report Degraded (the broken production stable is not
		// hidden behind "RolloutQueued"), and the phase stays Pending.
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "deg-b-pod-" + stableRev}, pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())

		Eventually(func() string {
			rec(rB, "deg-b")
			d := meta_Find(getDM("deg-b"), decisionmodelv1alpha1.ConditionDegraded)
			if d == nil || d.Status != metav1.ConditionTrue {
				return ""
			}
			return d.Reason
		}, "5s", "50ms").Should(Equal(reasonNoModelReadyPods), "stable shortfall surfaced on Degraded while queued")
		Expect(getDM("deg-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "phase stays queued, not flipped")
		// Ready reason remains the queue reason (the candidate is still queued).
		ready := meta_Find(getDM("deg-b"), decisionmodelv1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(reasonRolloutQueued))

		// Restore the stable Pod: Degraded clears, still queued.
		gatedPod("deg-b", stableRev, "10.0.4.3")
		Eventually(func() metav1.ConditionStatus {
			rec(rB, "deg-b")
			d := meta_Find(getDM("deg-b"), decisionmodelv1alpha1.ConditionDegraded)
			if d == nil {
				return metav1.ConditionUnknown
			}
			return d.Status
		}, "5s", "50ms").Should(Equal(metav1.ConditionFalse), "Degraded clears once the stable is model-ready again")
		Expect(getDM("deg-b").Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "still queued")
	})
})

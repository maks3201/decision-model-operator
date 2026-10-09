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
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// envtest has no garbage collector, so ownership is verified by asserting a
// controller OwnerReference on every child the operator creates (that is what a
// real cluster's GC follows when the DecisionModel is deleted). Deleting the DM
// must also release any fleet-budget slot it held, in every phase.
var _ = Describe("owner references on children and clean deletion", func() {
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
	newRec := func(pr Prober, maxRollouts int) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:               map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                pr,
			Recorder:              events.NewFakeRecorder(128),
			MaxConcurrentRollouts: maxRollouts,
			WatchNamespaces:       []string{namespace},
		}
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
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
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

	// ownedByDM asserts obj carries a controller OwnerReference to dm.
	ownedByDM := func(obj client.Object, dm *decisionmodelv1alpha1.DecisionModel) {
		found := false
		for _, o := range obj.GetOwnerReferences() {
			if o.UID == dm.UID && o.Controller != nil && *o.Controller {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "%T %q must have a controller OwnerReference to the DecisionModel",
			obj, obj.GetName())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("ownerrefs-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("sets a controller OwnerReference on every child it creates", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr, 0)
		eng := r.Engines["ollaya"].(*fakeEngine)
		eng.manifest = []byte(`{"schemaVersion":2,"layers":[{"digest":"sha256:owner"}]}`)
		wantDigest := digestOf(eng.manifest)
		pr.fallback = engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "own"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(2),
				Cache: &decisionmodelv1alpha1.CacheSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			},
		})).To(Succeed())
		rec(r, "own")
		dm := getDM("own")
		rev := RevisionHash(dm.Spec, wantDigest, fakeImage)
		pr.set(rev, engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"})

		// After the first reconcile: PVC, prefetch Job and the manifest ConfigMap exist.
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(dm, rev)}, pvc)).To(Succeed())
		ownedByDM(pvc, dm)
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("own", rev)}, job)).To(Succeed())
		ownedByDM(job, dm)
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "own-manifest-" + rev}, cm)).To(Succeed())
		ownedByDM(cm, dm)

		// Drive to serving: Deployment, Service and PDB appear.
		markJob("own", rev)
		rec(r, "own")
		gatedPod("own", rev, "10.0.20.1")
		p2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "own-pod2-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: "own", decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p2)).To(Succeed())
		p2.Status.PodIP = "10.0.20.2"
		p2.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p2)).To(Succeed())
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "own")
			return getDM("own").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "own-" + rev}, dep)).To(Succeed())
		ownedByDM(dep, dm)
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "own"}, svc)).To(Succeed())
		ownedByDM(svc, dm)
		pdb := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pdbName(dm, rev)}, pdb)).To(Succeed())
		ownedByDM(pdb, dm)
	})

	// Items 54-56: deleting the DM in a given phase releases its fleet-budget slot
	// (so a queued peer can proceed) and leaves no in-RAM reservation. envtest has
	// no GC, so children are reclaimed by their owner references (asserted above).
	DescribeTable("releases the budget slot when the DM is deleted in a phase",
		func(drive func(r *DecisionModelReconciler, pr *revProber, name string)) {
			pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
			r := newRec(pr, 1)
			drive(r, pr, "d")
			// It holds the only slot: a reconcile of a fresh peer would queue.
			rec(r, "d")
			Expect(getDM("d").Status.CandidateRevision).NotTo(BeNil(), "d admitted (holds the slot)")

			// Delete the DM and reconcile the NotFound: the reservation is released.
			Expect(k8sClient.Delete(ctx, getDM("d"))).To(Succeed())
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "d"}})
			r.budgetMu.Lock()
			_, held := r.budgetReservations[namespace+"/d"]
			r.budgetMu.Unlock()
			Expect(held).To(BeFalse(), "the fleet-budget reservation is released on deletion")
		},
		Entry("Caching", func(r *DecisionModelReconciler, pr *revProber, name string) {
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
			rec(r, name) // admitted -> Caching
			Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))
		}),
		Entry("Starting", func(r *DecisionModelReconciler, pr *revProber, name string) {
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
			rec(r, name)
			rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
			pr.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
			markJob(name, rev)
			rec(r, name) // -> Starting (serving Deployment, no gated Pod)
			Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseStarting))
		}),
	)
})

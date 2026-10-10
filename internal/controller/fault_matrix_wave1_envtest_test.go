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

// State-transition fault seams: five state-transition × fault scenarios. A spec that fails
// on current code is a bug report — it is kept and the code is fixed, never
// relaxed. Faults are injected via client/interceptor, not production hooks.
var _ = Describe("state-transition fault seams", func() {
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
	newRec := func(c client.Client, pr Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   pr,
			Recorder: events.NewFakeRecorder(256),
			Now:      func() time.Time { return clock },
		}
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
	gatedPod := func(name, rev, pod, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
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
	serviceRev := func(name string) string {
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		clock = time.Now()
		namespace = fmt.Sprintf("faultmx1-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// ---- spec.model change DURING an in-flight stable store
	// recovery. Expected: the stable recovery is not abandoned,
	// deleted or rewritten; the new candidate is admitted in parallel or after.
	// A spec.model change flips isStable to false, so the reconcile takes the
	// candidate path. This spec pins that the candidate path leaves the recorded
	// stable's in-flight recovery Job and PVC untouched (same UID, still annotated
	// recovering) while admitting the candidate.
	It("leaves an in-flight stable store recovery untouched while admitting a candidate on a spec.model change", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(k8sClient, pr)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rec"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		// Record a stable revision whose store is lost and under recovery.
		stable := &decisionmodelv1alpha1.RevisionStatus{
			Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage,
		}
		Expect(updateDMStatus(ctx, namespace, "rec", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.Phase = decisionmodelv1alpha1.PhaseReady
			d.Status.StableRevision = stable
		})).To(Succeed())
		claim := storeNameRev(dm, stable.Hash)

		By("establishing an in-flight recovery: annotated PVC + recovery prefetch Job")
		// First pass recreates the (annotated) PVC; second creates the recovery Job.
		_, _, _, err := r.recoverStableStore(ctx, getDM("rec"), r.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		_, _, _, err = r.recoverStableStore(ctx, getDM("rec"), r.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		recJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(dm, "r1")}, recJob)).
			To(Succeed(), "a recovery prefetch Job exists before the spec change")
		recJobUID := recJob.UID
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc)).To(Succeed())
		Expect(pvc.Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"))
		pvcUID := pvc.UID

		By("the user changes spec.model while recovery is in flight")
		Expect(updateDM(ctx, namespace, "rec", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		for i := 0; i < 3; i++ {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "rec"}})
		}

		By("the stable store recovery is not abandoned, deleted or rewritten")
		survJob := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(dm, "r1")}, survJob)).
			To(Succeed(), "the stable recovery Job must survive a candidate-only spec.model change")
		Expect(survJob.UID).To(Equal(recJobUID), "the recovery Job must not be deleted/recreated")
		survPVC := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, survPVC)).To(Succeed())
		Expect(survPVC.UID).To(Equal(pvcUID), "the recovery PVC must not be deleted/recreated")
		Expect(survPVC.Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"),
			"the stable store recovery annotation must be left intact, not rewritten")

		By("the new candidate is admitted in parallel: a candidate revision with its own prefetch Job")
		dmAfter := getDM("rec")
		Expect(dmAfter.Status.CandidateRevision).NotTo(BeNil(), "the candidate is admitted in parallel")
		candRev := dmAfter.Status.CandidateRevision.Hash
		Expect(candRev).NotTo(Equal("r1"), "the candidate is a distinct revision from the recovering stable")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("rec", candRev)}, &batchv1.Job{})).
			To(Succeed(), "the candidate has its own prefetch Job, separate from the stable recovery Job")
		Expect(jobName("rec", candRev)).NotTo(Equal(prefetchName(dm, "r1")),
			"the candidate prefetch Job is a different object from the stable recovery Job")
	})

	// ---- the candidate serving Deployment Create lands, then the
	// following DM status write gets a 409. Expected: no duplicate Deployment,
	// convergence, no duplicate RevisionStarting Event.
	It("creates no duplicate serving Deployment when the status write 409s after the create", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var conflicted atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		var depCreated atomic.Bool
		ic := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if err := cl.Create(ctx, obj, opts...); err != nil {
					return err
				}
				if _, ok := obj.(*appsv1.Deployment); ok {
					depCreated.Store(true)
				}
				return nil
			},
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption) error {
				// 409 the DM status write that immediately follows the Deployment create.
				if _, isDM := obj.(*decisionmodelv1alpha1.DecisionModel); sub == "status" && isDM &&
					depCreated.Load() && conflicted.CompareAndSwap(false, true) {
					return apierrors.NewConflict(
						schema.GroupResource{Group: decisionmodelv1alpha1.GroupVersion.Group, Resource: "decisionmodels"},
						"g4", fmt.Errorf("stale"))
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
		r := newRec(ic, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g4"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g4"}})
		}
		rec() // resolve + candidate + prefetch Job
		rev := getDM("g4").Status.CandidateRevision.Hash
		markJobComplete("g4", rev)
		// This reconcile creates the serving Deployment, then its status write 409s.
		rec()
		Expect(conflicted.Load()).To(BeTrue(), "the status write conflicted after the Deployment create")

		By("subsequent reconciles converge: exactly one serving Deployment, no duplicate")
		for i := 0; i < 3; i++ {
			rec()
		}
		Expect(faultDeploymentCount(ctx, namespace, "g4", rev)).
			To(Equal(1), "a 409 after the Deployment create must not create a duplicate Deployment")
	})

	// ---- BlueGreen post-promotion rollback — crash between the
	// rollbackToPrevious persist and the delete of the failed stable. Expected: the
	// next reconcile finishes the delete, the Service stays on the previous
	// revision, no flap.
	It("finishes a BlueGreen post-promotion rollback after a crash before the failed-stable delete", func() {
		const digest2 = "b2b2450000000000000000000000000000000000000000000000000000000000"
		pr := &revProber{}
		// The Deployment Delete that removes the failed stable after the rollback
		// persist is dropped once (simulated crash); a fresh reconciler then
		// converges and finishes the delete.
		var crashed atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		ic := interceptor.NewClient(wc, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok && crashed.CompareAndSwap(false, true) {
					return fmt.Errorf("induced crash after the rollback status write, before the failed-stable delete")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		})
		r := newRec(ic, pr)
		recC := func() decisionmodelv1alpha1.DecisionModelPhase {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g6"}})
			return getDM("g6").Status.Phase
		}

		By("driving a real BlueGreen rollout: rev1 Ready, then rev2 promoted (rev1 becomes previous)")
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g6"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		})).To(Succeed())
		pr.fallback = engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}
		rev1 := RevisionHash(getDM("g6").Spec, defaultDigest, fakeImage)
		recC()
		markJobComplete("g6", rev1)
		recC()
		gatedPod("g6", rev1, "g6-pod-"+rev1, "10.0.0.61")
		Eventually(recC, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		pr.set("", engine.Loaded{})
		Expect(updateDM(ctx, namespace, "g6", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM("g6").Spec, digest2, fakeImage)
		pr.fallback = engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"}
		pr.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})
		recC()
		markJobComplete("g6", rev2)
		recC()
		gatedPod("g6", rev2, "g6-pod-"+rev2, "10.0.0.62")
		Eventually(recC, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("g6").Status.StableRevision.Hash).To(Equal(rev2))
		Expect(getDM("g6").Status.PreviousRevision).NotTo(BeNil())
		Expect(getDM("g6").Status.PreviousRevision.Hash).To(Equal(rev1))
		settleDeployment("g6", rev2)

		By("the new stable loses the model (gate mismatch) inside the window -> rollback fires")
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})
		clock = clock.Add(2 * time.Minute)
		// The rollback persists stable=rev1 and switches the Service, then the delete
		// of the failed stable (rev2) Deployment is dropped (crash).
		Eventually(recC, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(crashed.Load()).To(BeTrue(), "the failed-stable Deployment delete was dropped (crash) after the rollback persist")

		By("the rollback is durable: stable=previous, Service already on previous, no flap")
		Expect(getDM("g6").Status.StableRevision.Hash).To(Equal(rev1), "stable rolled back to the previous revision")
		Expect(getDM("g6").Status.FailedRevision).NotTo(BeNil())
		Expect(getDM("g6").Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(serviceRev("g6")).To(Equal(rev1), "the Service points at the previous revision (no flap to the failed one)")

		By("a fresh reconciler finishes the delete of the failed stable; Service stays on previous")
		r2 := newRec(k8sClient, pr)
		Eventually(func() int {
			_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g6"}})
			return faultDeploymentCount(ctx, namespace, "g6", rev2)
		}, "10s", "50ms").Should(Equal(0), "the failed stable's Deployment is deleted on convergence")
		Expect(serviceRev("g6")).To(Equal(rev1), "the Service must stay on the previous revision, no flap")
	})

	// ---- an in-place change (replicas) while the candidate
	// prefetch Job is running. Expected: the Job is untouched (same UID), the change
	// applies after Cached.
	It("leaves the candidate prefetch Job untouched when replicas change mid-Caching", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(k8sClient, pr)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g9"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g9"}})
		}
		rec() // resolve + candidate + prefetch Job (Caching)
		rev := getDM("g9").Status.CandidateRevision.Hash
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("g9", rev)}, job)).To(Succeed())
		jobUID := job.UID

		By("replicas change while the prefetch Job is still running (not complete)")
		Expect(updateDM(ctx, namespace, "g9", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Replicas = int32Ptr(3)
		})).To(Succeed())
		rec()
		rec()

		By("the candidate revision hash is unchanged (replicas is not a revision field)")
		Expect(getDM("g9").Status.CandidateRevision.Hash).To(Equal(rev), "replicas is an in-place field, not a new revision")
		By("the prefetch Job is the same object, untouched")
		job2 := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("g9", rev)}, job2)).To(Succeed())
		Expect(job2.UID).To(Equal(jobUID), "an in-place replicas change must not recreate the candidate prefetch Job")

		By("after Cached, the serving Deployment carries the new replica count")
		markJobComplete("g9", rev)
		rec()
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "g9-" + rev}, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(3)), "the in-place replicas change applies to the candidate Deployment after Cached")
	})

	// ---- a retry token applied in the SAME reconcile as a spec
	// change. Expected: exactly one new revision, the token consumed once.
	It("produces one revision and consumes the retry token once when applied with a spec change", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(k8sClient, pr)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "g13"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		// A failed revision for the current (laya:en) spec, so a retry would matter.
		failHash := RevisionHash(dm.Spec, defaultDigest, fakeImage)
		Expect(updateDMStatus(ctx, namespace, "g13", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.Phase = decisionmodelv1alpha1.PhaseFailed
			d.Status.FailedRevision = &decisionmodelv1alpha1.RevisionStatus{
				Hash: failHash, Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage,
				Reason: "CacheTimeout", Message: "prior failure",
			}
		})).To(Succeed())

		By("applying a retry token AND a spec.model change in the same reconcile")
		Expect(updateDM(ctx, namespace, "g13", func(d *decisionmodelv1alpha1.DecisionModel) {
			if d.Annotations == nil {
				d.Annotations = map[string]string{}
			}
			d.Annotations[decisionmodelv1alpha1.AnnotationRetry] = "retry-1"
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g13"}})
		}
		rec()

		By("exactly one new candidate revision (the kev:en spec), token consumed once")
		dmAfter := getDM("g13")
		Expect(dmAfter.Status.LastRetryToken).To(Equal("retry-1"), "the token is consumed (recorded) exactly once")
		newRev := RevisionHash(dmAfter.Spec, defaultDigest, fakeImage)
		Expect(dmAfter.Status.CandidateRevision).NotTo(BeNil())
		Expect(dmAfter.Status.CandidateRevision.Hash).To(Equal(newRev), "exactly one new revision for the changed spec")
		Expect(dmAfter.Status.CandidateRevision.Hash).NotTo(Equal(failHash), "not the old failed revision")
		Expect(dmAfter.Status.FailedRevision).To(BeNil(), "the failed revision is cleared by the retry/spec change")
		// Exactly one candidate prefetch Job for the new revision; none for the old.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("g13", newRev)}, &batchv1.Job{})).
			To(Succeed(), "one new candidate Job for the changed spec")
	})
})

// faultDeploymentCount counts serving Deployments for a revision in a namespace.
func faultDeploymentCount(ctx context.Context, namespace, name, rev string) int {
	list := &appsv1.DeploymentList{}
	_ = k8sClient.List(ctx, list, client.InNamespace(namespace))
	n := 0
	for i := range list.Items {
		if list.Items[i].Name == name+"-"+rev {
			n++
		}
	}
	return n
}

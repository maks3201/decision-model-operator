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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// close the ownership hole the label-filtered manager cache opens
// (createOrAdopt on AlreadyExists); make stable-store recovery reachable
// from Reconcile; and keep the stable serving behind Degraded=StoreTerminating
// while its store PVC is deleted-but-still-mounted.
var _ = Describe("ownership through the cache and stable-store recovery", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newRec := func(c client.Client, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    c,
			APIReader: k8sClient, // uncached reader: always the real store
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}
	newProber := func() *fakeProber {
		return &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	createDM := func(name string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
	}
	createGatedPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.36"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	markJobComplete := func(name string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	degradedReason := func(dm *decisionmodelv1alpha1.DecisionModel) string {
		c := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		if c == nil {
			return ""
		}
		return c.Reason
	}
	// driveToStable brings name to a Ready stable revision via Reconcile.
	driveToStable := func(r *DecisionModelReconciler, name string) string {
		createDM(name)
		rec := func() {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
			Expect(err).NotTo(HaveOccurred())
		}
		rec()
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name + "-prefetch-" + rev)
		rec()
		createGatedPod(name, rev, name+"-pod-0")
		rec() // probe
		rec() // promote
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		return rev
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "ownrec-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// --- ownership on an AlreadyExists the cache could not see. ---

	// hideGet returns a client whose Get reports NotFound for one name/type,
	// simulating the label-filtered cache missing a foreign/unlabelled object
	// (the real store still rejects the Create with AlreadyExists).
	hideGet := func(kind client.Object, hidden string) client.Client {
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		return interceptor.NewClient(wc, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if fmt.Sprintf("%T", obj) == fmt.Sprintf("%T", kind) && key.Name == hidden {
					return apierrors.NewNotFound(corev1.Resource("x"), hidden)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}

	It("refuses to adopt a foreign unlabelled Service hidden from the cache", func() {
		createDM("adopt-svc")
		// A foreign Service with our name and no decisionmodel.io/name label: the
		// cached Get would miss it, so createOrAdopt must catch it on AlreadyExists.
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "adopt-svc"},
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Name: "x", Port: 4321, Protocol: corev1.ProtocolTCP}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		rvBefore := foreign.ResourceVersion

		r := newRec(hideGet(&corev1.Service{}, "adopt-svc"), newProber())
		dm := getDM("adopt-svc")
		err := r.ensureService(ctx, dm, r.Engines["ollaya"], "somehash")
		Expect(err).To(MatchError(errResourceConflict))

		got := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "adopt-svc"}, got)).To(Succeed())
		Expect(got.ResourceVersion).To(Equal(rvBefore), "foreign Service left untouched")
		Expect(degradedReason(dm)).To(Equal(reasonResourceConflict))
	})

	It("re-labels an owned Deployment whose cache-selector label was stripped", func() {
		createDM("adopt-dep")
		dm := getDM("adopt-dep")
		// An owned Deployment (controller ref = dm) but WITHOUT decisionmodel.io/name:
		// the cache cannot see it, Create returns AlreadyExists, createOrAdopt must
		// patch the label back rather than conflict.
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "adopt-dep-r9"},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "x"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: fakeImage}}},
				},
			},
		}
		Expect(controllerutil.SetControllerReference(dm, dep, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())

		r := newRec(hideGet(&appsv1.Deployment{}, "adopt-dep-r9"), newProber())
		params := r.paramsFor(dm, defaultDigest, fakeImage, "r9")
		Expect(r.ensureDeployment(ctx, dm, r.Engines["ollaya"], params, "r9", false, "", false)).To(Succeed())

		got := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "adopt-dep-r9"}, got)).To(Succeed())
		Expect(got.Labels).To(HaveKeyWithValue(decisionmodelv1alpha1.LabelName, "adopt-dep"),
			"cache-selector label patched back onto the owned Deployment")
	})

	// --- lost stable store recovered through Reconcile (not recoverStableStore). ---

	It("recovers a lost per-revision stable store through Reconcile", func() {
		r := newRec(k8sClient, newProber())
		rev := driveToStable(r, "lost-rev")
		pvcKey := types.NamespacedName{Namespace: namespace, Name: "lost-rev-store-" + rev}
		depKey := types.NamespacedName{Namespace: namespace, Name: "lost-rev-" + rev}

		depBefore := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, depBefore)).To(Succeed())

		// creationTimestamp has one-second resolution and a Job counts as stale only
		// when it is strictly older than the store PVC. Let a second pass so the
		// recreated PVC is newer than the first rollout's Job and the stale path
		// (delete, then a fresh recovery Job) is always taken.
		time.Sleep(1100 * time.Millisecond)

		// Delete the stable store PVC (drop the pvc-protection finalizer so it
		// actually disappears).
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
		pvc.Finalizers = nil
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
		}, "5s", "50ms").Should(BeTrue())

		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "lost-rev"}})
		}

		// Reconcile recreates the PVC carrying the recovering annotation (not an
		// unannotated empty store), and holds Degraded=StoreLost.
		rec()
		recreated := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, recreated)).To(Succeed())
		Expect(recreated.Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, annotationTrue),
			"recovery recreated the PVC annotated, so it counts as unpopulated")
		Expect(degradedReason(getDM("lost-rev"))).To(Equal(reasonStoreLost))

		// The Deployment template must not have been re-rendered during recovery.
		depDuring := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, depDuring)).To(Succeed())
		Expect(depDuring.Spec.Template).To(Equal(depBefore.Spec.Template))

		// Drive the recovery prefetch to completion. Wait for the fresh recovery Job:
		// the stale Complete Job from the first rollout has the same name and is
		// deleted first, and re-marking it would hit the immutable status times.
		Eventually(func() bool {
			rec()
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "lost-rev-prefetch-" + rev}, job); err != nil {
				return false
			}
			return job.Status.CompletionTime == nil
		}, "3s", "20ms").Should(BeTrue())
		markJobComplete("lost-rev-prefetch-" + rev)
		rec() // observe Complete -> clear the annotation
		rec() // annotation gone -> serve

		served := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, served)).To(Succeed())
		Expect(served.Annotations).NotTo(HaveKey(storeRecoveringAnnotation), "annotation cleared once repopulated")
		Expect(getDM("lost-rev").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
	})

	It("recovers a lost legacy shared store through Reconcile", func() {
		r := newRec(k8sClient, newProber())
		rev := driveToStable(r, "lost-leg")
		dm := getDM("lost-leg")
		// Simulate a legacy shared-store stable: point the live Deployment at the
		// shared <dm>-store and create that PVC owned by the DM.
		depKey := types.NamespacedName{Namespace: namespace, Name: "lost-leg-" + rev}
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		for i := range dep.Spec.Template.Spec.Volumes {
			if src := dep.Spec.Template.Spec.Volumes[i].PersistentVolumeClaim; src != nil {
				src.ClaimName = storeName(dm)
			}
		}
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())

		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "lost-leg"}})
		}
		// The legacy shared PVC does not exist -> Reconcile must route to recovery
		// (annotated create under the shared name) rather than return NotFound.
		rec()
		legacyKey := types.NamespacedName{Namespace: namespace, Name: storeName(dm)}
		legacyPVC := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, legacyKey, legacyPVC)).To(Succeed())
		Expect(legacyPVC.Name).To(Equal(storeName(dm)), "recreated under the shared name, not a per-revision PVC")
		Expect(legacyPVC.Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, annotationTrue))
		Expect(degradedReason(getDM("lost-leg"))).To(Equal(reasonStoreLost))
	})

	// --- terminating-but-mounted stable store. ---

	It("keeps the stable serving behind Degraded=StoreTerminating while its store is deleted-but-mounted", func() {
		r := newRec(k8sClient, newProber())
		rev := driveToStable(r, "term")
		pvcKey := types.NamespacedName{Namespace: namespace, Name: "term-store-" + rev}
		depKey := types.NamespacedName{Namespace: namespace, Name: "term-" + rev}

		// Delete the PVC but keep the pvc-protection finalizer: it stays
		// Terminating (as it would while the Pods still mount it).
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
		uid := pvc.UID
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
		Expect(pvc.DeletionTimestamp).NotTo(BeNil(), "PVC is Terminating")

		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "term"}})
		Expect(err).NotTo(HaveOccurred())
		// Stable keeps serving (ready Pod): Ready=True, Degraded=StoreTerminating.
		dm := getDM("term")
		Expect(degradedReason(dm)).To(Equal(reasonStoreTerminating))
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionReady)).To(BeTrue())

		// The operator did NOT stop the stable: the Deployment is untouched and
		// the PVC was neither recreated nor force-finalized.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
		Expect(pvc.UID).To(Equal(uid))
		Expect(pvc.DeletionTimestamp).NotTo(BeNil())

		// Once the PVC is actually gone, recovery takes over (annotated recreate).
		pvc.Finalizers = nil
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
		}, "5s", "50ms").Should(BeTrue())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "term"}})
		recreated := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, recreated)).To(Succeed())
		Expect(recreated.Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, annotationTrue))
		Expect(degradedReason(getDM("term"))).To(Equal(reasonStoreLost))
	})

	// A new decisionmodel.io/retry token resets an exhausted stable-store
	// recovery and is consumed once; the same token does nothing. The token logic
	// lives in applyRetryToken; it is exercised deterministically below (the full
	// recovery reconcile is covered by the specs). A single
	// through-Reconcile smoke confirms the wiring.
	It("resets an exhausted stable-store recovery on a new retry token", func() {
		r := newRec(k8sClient, newProber())
		rev := driveToStable(r, "retry")
		pvcKey := types.NamespacedName{Namespace: namespace, Name: "retry-store-" + rev}

		// Lose the store so the stable path is in recovery.
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
		pvc.Finalizers = nil
		Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
		}, "5s", "50ms").Should(BeTrue())

		// --- Deterministic token semantics (no reconcile/recovery interplay). ---
		// countFailedRecovery now records into dm.Status.StoreRecovery, so the
		// bound is persisted and survives a restart. Exhaust a local object.
		exhaust := func(d *decisionmodelv1alpha1.DecisionModel, prefix string) {
			for i := 0; i < maxStoreRecoverAttempts; i++ {
				r.countFailedRecovery(d, types.UID(prefix+itoa(i)))
			}
			Expect(storeRecoverExhausted(d)).To(BeTrue())
		}

		// First observation of token "t1": clears the latch, records the token.
		d1 := getDM("retry")
		exhaust(d1, "f1-")
		d1.Annotations = map[string]string{decisionmodelv1alpha1.AnnotationRetry: "t1"}
		r.applyRetryToken(d1, true)
		Expect(storeRecoverExhausted(d1)).To(BeFalse(), "new token clears the exhausted latch")
		Expect(d1.Status.LastRetryToken).To(Equal("t1"), "token recorded (consumed once)")

		// Same token again: re-exhaust, apply "t1" with lastRetryToken already "t1"
		// -> no reset.
		exhaust(d1, "f2-")
		d1.Annotations[decisionmodelv1alpha1.AnnotationRetry] = "t1"
		r.applyRetryToken(d1, true) // d1.Status.LastRetryToken == "t1"
		Expect(storeRecoverExhausted(d1)).To(BeTrue(), "the same token does not restart recovery")

		// A different token resets again.
		d1.Annotations[decisionmodelv1alpha1.AnnotationRetry] = "t2"
		r.applyRetryToken(d1, true)
		Expect(storeRecoverExhausted(d1)).To(BeFalse(), "a new token restarts recovery again")
		Expect(d1.Status.LastRetryToken).To(Equal("t2"))

		// --- Through-Reconcile smoke: an exhausted latch persisted in status plus a
		// retry annotation leads to a non-exhausted latch (Reconcile calls
		// applyRetryToken). The exhausted state is now stored in status, so persist
		// it to the cluster before reconciling. ---
		Expect(updateDMStatus(ctx, namespace, "retry", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.StoreRecovery = &decisionmodelv1alpha1.StoreRecoveryStatus{
				Attempts: maxStoreRecoverAttempts, LastFailedJob: "f3", Exhausted: true,
			}
		})).To(Succeed())
		Expect(updateDM(ctx, namespace, "retry", func(d *decisionmodelv1alpha1.DecisionModel) {
			if d.Annotations == nil {
				d.Annotations = map[string]string{}
			}
			d.Annotations[decisionmodelv1alpha1.AnnotationRetry] = "t3"
		})).To(Succeed())
		Eventually(func() bool {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "retry"}})
			return !storeRecoverExhausted(getDM("retry"))
		}, "5s", "20ms").Should(BeTrue(), "a retry annotation clears the exhausted latch through Reconcile")
	})
})

// a foreign Service blocks Ready at the first promotion and on the
// stable path; a Terminating stable store freezes the Pod template.
var _ = Describe("foreign Service not Ready; frozen template on terminating store", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	newRec := func(prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Prober: prober,
			Recorder: events.NewFakeRecorder(64),
		}
	}
	newProber := func() *fakeProber {
		return &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	createDM := func(name string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
	}
	markJobComplete := func(name string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	createGatedPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.37"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "svcconflict-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("never reports Ready while a foreign Service occupies the name; promotes once it is removed", func() {
		r := newRec(newProber())
		// A pre-existing foreign (unlabelled, not owned) Service named like the DM.
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "fsvc"},
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Name: "x", Port: 9, Protocol: corev1.ProtocolTCP}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

		createDM("fsvc")
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "fsvc"}})
		}
		rec()
		rev := RevisionHash(getDM("fsvc").Spec, defaultDigest, fakeImage)
		markJobComplete("fsvc-prefetch-" + rev)
		rec()
		createGatedPod("fsvc", rev, "fsvc-pod-0")
		// Drive several reconciles: the candidate is model-ready but must never be
		// promoted to Ready while the foreign Service holds the name.
		for i := 0; i < 4; i++ {
			rec()
		}
		dm := getDM("fsvc")
		Expect(meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionReady)).
			To(BeFalse(), "never Ready behind a foreign Service")
		ready := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionReady)
		Expect(ready.Reason).To(Equal(reasonResourceConflict))
		Expect(getDM("fsvc").Status.FailedRevision).To(BeNil(), "a name conflict is not a rollout failure")
		// The foreign Service is untouched.
		got := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "fsvc"}, got)).To(Succeed())
		Expect(got.Labels).NotTo(HaveKey(decisionmodelv1alpha1.LabelName))

		// Remove the foreign Service: promotion proceeds and the DM goes Ready.
		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec()
			return getDM("fsvc").Status.Phase
		}, "3s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
	})

	It("does not change the stable Pod template while the store PVC is Terminating", func() {
		r := newRec(newProber())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "frz"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		params := r.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Seed a stable Deployment carrying key-v1 in the template.
		Expect(r.ensureDeployment(ctx, dm, r.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v1"), false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "frz-r1"}, dep)).To(Succeed())
		tmplBefore := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]
		Expect(tmplBefore).NotTo(BeEmpty())

		// A key rotation WHILE the store is Terminating (freezeTemplate=true) must
		// not touch the Pod template, even though it would otherwise roll it.
		Expect(r.ensureDeployment(ctx, dm, r.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v2"), true)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "frz-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).
			To(Equal(tmplBefore), "template frozen while the store is Terminating")

		// Replicas may still be applied while frozen.
		two := int32(2)
		dm.Spec.Replicas = &two
		Expect(r.ensureDeployment(ctx, dm, r.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v2"), true)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "frz-r1"}, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(two))
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "still frozen")

		// After recovery (freezeTemplate=false), the rotation is finally applied.
		Expect(r.ensureDeployment(ctx, dm, r.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v2"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "frz-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).
			NotTo(Equal(tmplBefore), "template rolls once the store is recovered")
	})
})

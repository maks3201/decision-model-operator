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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("ownership and store recovery", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(c client.Client, eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    c,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	createDM := func(name string) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
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
		pod.Status.PodIP = "10.0.0.31"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	markJob := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	// driveToStable brings name to a Ready stable revision and returns the hash.
	driveToStable := func(r *DecisionModelReconciler, name string) string {
		createDM(name)
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		}
		rec()
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJob(name, rev)
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
		namespace = "own31-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// a foreign Service with the DM's name is never modified; Degraded=ResourceConflict.
	It("refuses to modify a foreign Service and reports ResourceConflict", func() {
		createDM("conflict-svc")
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "conflict-svc", Labels: map[string]string{"owner": "someone-else"}},
			Spec: corev1.ServiceSpec{
				Type:  corev1.ServiceTypeClusterIP,
				Ports: []corev1.ServicePort{{Name: "x", Port: 1234, Protocol: corev1.ProtocolTCP}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		rvBefore := foreign.ResourceVersion

		r := newReconciler(k8sClient, newFakeEngine(), &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		dm := getDM("conflict-svc")
		err := r.ensureService(ctx, dm, r.Engines["ollaya"], "somehash")
		Expect(err).To(MatchError(errResourceConflict))

		got := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "conflict-svc"}, got)).To(Succeed())
		Expect(got.ResourceVersion).To(Equal(rvBefore), "foreign Service left untouched")
		Expect(got.Spec.Ports[0].Port).To(Equal(int32(1234)))
		deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonResourceConflict))
	})

	// gcLegacyStore never deletes a <dm>-store PVC the DM does not own.
	It("never deletes a foreign <dm>-store legacy PVC", func() {
		createDM("conflict-store")
		foreign := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "conflict-store-store", Labels: map[string]string{decisionmodelv1alpha1.LabelName: "conflict-store"}},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		r := newReconciler(k8sClient, newFakeEngine(), &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		Expect(r.gcLegacyStore(ctx, getDM("conflict-store"))).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "conflict-store-store"},
			&corev1.PersistentVolumeClaim{})).To(Succeed(), "foreign legacy store survives GC")
	})

	// a lost per-revision stable store is recreated and prefetched; stable
	// stays Degraded (StoreLost) and does not serve on an empty store.
	It("recreates and reprefetches a lost stable store before serving", func() {
		r := newReconciler(k8sClient, newFakeEngine(), &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		rev := driveToStable(r, "lost")
		storePVC := "lost-store-" + rev
		stable := getDM("lost").Status.StableRevision

		// Delete the stable store PVC and clear its protection finalizer so envtest
		// actually drops it (there is no PV controller in envtest).
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storePVC}, pvc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		Eventually(func() bool {
			p := &corev1.PersistentVolumeClaim{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storePVC}, p)
			if apierrors.IsNotFound(err) {
				return true
			}
			if err == nil && len(p.Finalizers) > 0 {
				p.Finalizers = nil
				_ = k8sClient.Update(ctx, p)
			}
			return false
		}, "3s", "50ms").Should(BeTrue(), "store PVC removed")

		// Drive recovery directly: the store is gone, so recoverStableStore handles
		// it (recreate PVC + fresh prefetch) and reports Degraded=StoreLost.
		dm := getDM("lost")
		dm.Status.StableRevision = stable
		handled, _, _, _ := r.recoverStableStore(ctx, dm, r.Engines["ollaya"], stable, storePVC, false)
		Expect(handled).To(BeTrue(), "recovery takes over while the store is missing")
		deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonStoreLost))
		// The PVC was recreated and a fresh prefetch Job exists for the stable rev.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storePVC},
			&corev1.PersistentVolumeClaim{})).To(Succeed(), "store PVC recreated")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "lost-prefetch-" + rev},
			&batchv1.Job{})).To(Succeed(), "prefetch re-run for the recovered store")
	})

	// a Pod List failure aborts the reconcile (error returned) rather than
	// being scored as "0 ready" (which would wrongly roll back / degrade).
	It("aborts the reconcile on a Pod List error instead of reporting 0 ready", func() {
		var fail atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok && fail.Load() {
					return fmt.Errorf("boom: pod list unavailable")
				}
				return cl.List(ctx, list, opts...)
			},
		})
		r := newReconciler(c, newFakeEngine(), &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		rev := driveToStable(r, "listerr")
		Expect(getDM("listerr").Status.StableRevision.Hash).To(Equal(rev))

		fail.Store(true)
		_, rerr := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "listerr"}})
		Expect(rerr).To(HaveOccurred(), "a Pod List failure must surface as a reconcile error (workqueue backoff)")
		// The stable revision is unchanged; no spurious rollback/fail from "0 ready".
		Expect(getDM("listerr").Status.FailedRevision).To(BeNil())
	})

	// a steady-state stable issues no Service write (desired port carries TCP).
	It("issues no Service write in steady state", func() {
		var svcWrites atomic.Int32
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					svcWrites.Add(1)
				}
				return cl.Update(ctx, obj, opts...)
			},
		})
		r := newReconciler(c, newFakeEngine(), &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
		driveToStable(r, "svc-steady")
		before := svcWrites.Load()
		for i := 0; i < 4; i++ {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "svc-steady"}})
		}
		Expect(svcWrites.Load()).To(Equal(before), "no Service Update on a no-drift reconcile")
		// And the live Service port records Protocol TCP.
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "svc-steady"}, svc)).To(Succeed())
		Expect(svc.Spec.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP))
	})
})

var _ = Describe("deleteRevisionWorkloads ownership", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "del31-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("never deletes a foreign Deployment with the revision's name", func() {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "del"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu"},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rev := "cafebabecafebabe"
		foreign := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "del-" + rev, Labels: map[string]string{"owner": "else"}},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"a": "b"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: fakeImage}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Recorder: events.NewFakeRecorder(16),
		}
		Expect(r.deleteRevisionWorkloads(ctx, dm, rev)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "del-" + rev},
			&appsv1.Deployment{})).To(Succeed(), "foreign Deployment survives rollback cleanup")
	})
})

var _ = Describe("errors", func() {
	It("errResourceConflict and errPodListFailed are distinct sentinels", func() {
		Expect(apierrors.IsNotFound(errResourceConflict)).To(BeFalse())
		Expect(errResourceConflict).NotTo(MatchError(errPodListFailed))
	})
})

var _ = Describe("ensure* conflict and legacy recovery", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	mkDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu"},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		return dm
	}
	r := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Recorder: events.NewFakeRecorder(16),
		}
	}
	foreignLabels := map[string]string{"owner": "else"}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "ens31-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("ensurePVC refuses a foreign per-revision store PVC", func() {
		dm := mkDM("p")
		Expect(k8sClient.Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: storeNameRev(dm, "r1"), Labels: foreignLabels},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		})).To(Succeed())
		_, err := r().ensurePVC(ctx, dm, "r1")
		Expect(err).To(MatchError(errResourceConflict))
	})

	It("ensureDeployment and ensurePDB refuse foreign objects", func() {
		dm := mkDM("d")
		Expect(k8sClient.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: revisionName(dm, "r1"), Labels: foreignLabels},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"a": "b"}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: fakeImage}}}},
			},
		})).To(Succeed())
		params := r().paramsFor(dm, defaultDigest, fakeImage, "r1")
		Expect(r().ensureDeployment(ctx, dm, r().Engines["ollaya"], params, "r1", false, "", false)).To(MatchError(errResourceConflict))

		two := int32(2)
		dm.Spec.Replicas = &two
		Expect(k8sClient.Create(ctx, &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: pdbName(dm, "r1"), Labels: foreignLabels},
			Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}}},
		})).To(Succeed())
		Expect(r().ensurePDB(ctx, dm, "r1")).To(MatchError(errResourceConflict))
	})

	It("recovers a lost legacy shared store under its own name", func() {
		dm := mkDM("leg")
		stable := &decisionmodelv1alpha1.RevisionStatus{Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage}
		// claim is the legacy shared <dm>-store; it is missing -> recreate under
		// the SAME name (never a per-revision PVC) and start a prefetch.
		handled, _, _, _ := r().recoverStableStore(ctx, dm, r().Engines["ollaya"], stable, storeName(dm), true)
		Expect(handled).To(BeTrue())
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeName(dm)}, pvc)).To(Succeed())
		Expect(ownedBy(pvc, dm)).To(BeTrue(), "recreated legacy store is owned by the DM")
	})

	It("completes recovery once the fresh prefetch finishes (handled=false)", func() {
		rr := r()
		dm := mkDM("rec")
		stable := &decisionmodelv1alpha1.RevisionStatus{Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage}
		claim := storeNameRev(dm, stable.Hash)
		// First pass: store missing -> recreate PVC + requeue (no Job yet).
		handled, _, _, _ := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(handled).To(BeTrue())
		// Second pass: PVC present, recovery active -> start the prefetch Job.
		handled, _, _, _ = rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(handled).To(BeTrue())
		// Mark the fresh prefetch Job complete.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rec-prefetch-r1"}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
		// Third pass: Complete Job fills the PVC -> clear the recovering annotation.
		handled, _, _, _ = rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(handled).To(BeTrue())
		// Fourth pass: annotation gone -> store populated -> recovery steps aside.
		handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeFalse(), "serving resumes once the store is recovered")
	})

	It("requeues while the fresh prefetch is still running", func() {
		rr := r()
		dm := mkDM("recrun")
		stable := &decisionmodelv1alpha1.RevisionStatus{Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage}
		claim := storeNameRev(dm, stable.Hash)
		handled, _, res, _ := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(handled).To(BeTrue())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0), "requeue while the prefetch runs")
	})

	It("propagates a non-NotFound store Get error from recovery", func() {
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					return fmt.Errorf("boom: pvc get unavailable")
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		})
		rr := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Recorder: events.NewFakeRecorder(16),
		}
		dm := mkDM("recerr")
		stable := &decisionmodelv1alpha1.RevisionStatus{Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage}
		dm.Status.StableRevision = stable
		handled, _, _, rerr := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, storeNameRev(dm, "r1"), false)
		Expect(handled).To(BeTrue())
		Expect(rerr).To(HaveOccurred(), "a transport error on the store Get aborts, not treated as missing")
	})
})

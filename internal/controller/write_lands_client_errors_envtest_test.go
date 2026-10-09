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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Item 61: a write (Create/Patch/Delete) that LANDS on the API server but whose
// response the client never sees (a timeout after the mutation applied) must not
// cause a duplicate or an irreversible divergence — the next reconcile is
// idempotent. Distinct from a write that fails outright (the object never
// changes): here the delegate runs, THEN the interceptor returns an error.
var _ = Describe("writes that land but return an error to the client are idempotent", func() {
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
	createDM := func(name string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("writeland-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// countOwned lists objects of a kind in the namespace that are owned by dm, to
	// prove no duplicate was created after the "landed but errored" write.
	It("creates no duplicate candidate PVC when its Create lands but the client errors", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var firedOnce atomic.Bool
		firedOnce.Store(true)
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				// Let the PVC create LAND, then return an error exactly once (the
				// response was lost after the object was persisted).
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && firedOnce.CompareAndSwap(true, false) {
					if cerr := cl.Create(ctx, obj, opts...); cerr != nil {
						return cerr
					}
					return fmt.Errorf("induced lost response after the PVC create landed")
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(128),
		}
		createDM("pvc")
		rev := RevisionHash(getDM("pvc").Spec, defaultDigest, fakeImage)
		prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})

		// First reconcile hits the induced error after the PVC landed.
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "pvc"}})
		// Subsequent reconciles must converge WITHOUT creating a second PVC.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "pvc"}})
			return getDM("pvc").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseCaching), "converges to Caching after the lost response")

		pvcs := &corev1.PersistentVolumeClaimList{}
		Expect(k8sClient.List(ctx, pvcs, client.InNamespace(namespace))).To(Succeed())
		n := 0
		for i := range pvcs.Items {
			if pvcs.Items[i].Name == storeNameRev(getDM("pvc"), rev) {
				n++
			}
		}
		Expect(n).To(Equal(1), "exactly one candidate store PVC exists (no duplicate from the retried create)")
	})

	It("creates no duplicate serving Deployment when its Create lands but the client errors", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var firedOnce atomic.Bool
		firedOnce.Store(true)
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok && firedOnce.CompareAndSwap(true, false) {
					if cerr := cl.Create(ctx, obj, opts...); cerr != nil {
						return cerr
					}
					return fmt.Errorf("induced lost response after the Deployment create landed")
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(128),
		}
		createDM("dep")
		rev := RevisionHash(getDM("dep").Spec, defaultDigest, fakeImage)
		prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})

		// Drive to the point the serving Deployment is created (after Caching).
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dep"}})
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("dep", rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		// This reconcile creates the serving Deployment; the response is lost.
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dep"}})
		// Converge: subsequent reconciles must not create a second Deployment.
		Eventually(func() bool {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "dep"}})
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "dep-" + rev}, &appsv1.Deployment{}) == nil
		}, "5s", "50ms").Should(BeTrue(), "serving Deployment exists after the lost response")

		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(ctx, deps, client.InNamespace(namespace))).To(Succeed())
		n := 0
		for i := range deps.Items {
			if deps.Items[i].Name == "dep-"+rev {
				n++
			}
		}
		Expect(n).To(Equal(1), "exactly one serving Deployment exists (no duplicate from the retried create)")
	})
})

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

// Restart safety: a candidate failure is persisted before its workloads are
// deleted, and the bounded store-recovery count lives in status so it survives a
// manager restart.
var _ = Describe("restart safety", func() {
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
	newR := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(256),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	failJob := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	jobExists := func(name, rev string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, &batchv1.Job{})
		return err == nil
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("restart-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Persist-first rollbackOrFail: when the status write that records
	// failedRevision loses an optimistic-lock race, the candidate's workloads are
	// NOT deleted — otherwise a crash would leave no failure record and the next
	// reconcile would restart the same rollout.
	It("does not delete candidate workloads when the failure status write conflicts", func() {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cf"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		r := newR()
		rec(r, "cf") // resolve + create prefetch Job
		rev := RevisionHash(getDM("cf").Spec, defaultDigest, fakeImage)
		Expect(jobExists("cf", rev)).To(BeTrue(), "candidate prefetch Job created")
		failJob("cf", rev)

		// A client that fails the first DM status write with a Conflict.
		var failOnce bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, isDM := obj.(*decisionmodelv1alpha1.DecisionModel); sub == "status" && isDM && !failOnce {
					failOnce = true
					return apierrors.NewConflict(
						schema.GroupResource{Group: decisionmodelv1alpha1.GroupVersion.Group, Resource: "decisionmodels"},
						"cf", fmt.Errorf("stale"))
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
		rc := newR()
		rc.Client = c

		// Reconcile observes the failed Job and wants to fail the candidate, but the
		// status write conflicts: the prefetch Job (candidate workload) must survive
		// and no failedRevision is recorded.
		_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "cf"}})
		Expect(failOnce).To(BeTrue(), "the status write was attempted and conflicted")
		Expect(jobExists("cf", rev)).To(BeTrue(), "candidate workloads untouched on a conflicting write")
		Expect(getDM("cf").Status.FailedRevision).To(BeNil(), "no failure recorded on a conflicting write")

		// Retry (same client, no more failures): the failure is recorded first, then
		// the candidate workloads are deleted, and the rollout is NOT restarted.
		Eventually(func() *decisionmodelv1alpha1.RevisionStatus {
			_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "cf"}})
			return getDM("cf").Status.FailedRevision
		}, "10s", "50ms").ShouldNot(BeNil())
		Expect(getDM("cf").Status.FailedRevision.Hash).To(Equal(rev))
		Expect(getDM("cf").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Eventually(func() bool {
			_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "cf"}})
			return jobExists("cf", rev)
		}, "10s", "50ms").Should(BeFalse(), "candidate workloads deleted after the failure is durable")
		// The failed revision is not retried: no new prefetch Job reappears.
		for i := 0; i < 3; i++ {
			rec(rc, "cf")
		}
		Expect(jobExists("cf", rev)).To(BeFalse(), "a failed revision is not retried automatically")
		Expect(getDM("cf").Status.CandidateRevision).To(BeNil())
	})

	// A crash after the durable failure write but before the workload delete must
	// still converge: a fresh reconciler deletes the candidate workloads from the
	// persisted failedRevision and does not restart the rollout.
	It("converges a partial failure delete after a crash", func() {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "crashfail"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		r := newR()
		rec(r, "crashfail")
		rev := RevisionHash(getDM("crashfail").Spec, defaultDigest, fakeImage)
		failJob("crashfail", rev)

		// A client that lets the status write through but fails the Job delete once
		// (a crash right after the durable write).
		var crashed bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.Job); ok && !crashed {
					crashed = true
					return fmt.Errorf("simulated crash after status write")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		})
		rc := newR()
		rc.Client = c
		_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "crashfail"}})
		Expect(getDM("crashfail").Status.FailedRevision).NotTo(BeNil(), "failure is durable before the delete")
		Expect(getDM("crashfail").Status.FailedRevision.Hash).To(Equal(rev))

		// A fresh reconciler finishes the delete from the persisted failedRevision
		// and never restarts the rollout.
		r2 := newR()
		Eventually(func() bool {
			rec(r2, "crashfail")
			return jobExists("crashfail", rev)
		}, "10s", "50ms").Should(BeFalse(), "the partial delete is finished from persisted status")
		Expect(getDM("crashfail").Status.CandidateRevision).To(BeNil())
		Expect(getDM("crashfail").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
	})

	// The bounded store-recovery count is stored in status, so it survives a
	// manager restart: 2 attempts, a brand-new reconciler (empty RAM), 1 more
	// attempt -> exhausted (not 3 more).
	It("keeps the store-recovery attempt bound across a restart", func() {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "rec"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())

		r := newR()
		// Two distinct failed Jobs counted -> not exhausted yet (bound is 3).
		Expect(r.countFailedRecovery(dm, types.UID("j1"))).To(BeFalse())
		Expect(r.countFailedRecovery(dm, types.UID("j2"))).To(BeFalse())
		Expect(dm.Status.StoreRecovery.Attempts).To(Equal(int32(2)))
		// Persist the status (as the surrounding finish would).
		Expect(k8sClient.Status().Update(ctx, dm)).To(Succeed())

		// Restart: a brand-new reconciler with empty in-memory state, reading the
		// persisted status. One more distinct failure exhausts (2 + 1 == bound 3),
		// not three more.
		r2 := newR()
		fresh := getDM("rec")
		Expect(fresh.Status.StoreRecovery).NotTo(BeNil(), "attempts persisted across the restart")
		Expect(fresh.Status.StoreRecovery.Attempts).To(Equal(int32(2)))
		Expect(r2.countFailedRecovery(fresh, types.UID("j3"))).To(BeTrue(), "one more attempt exhausts after restart")
		Expect(fresh.Status.StoreRecovery.Exhausted).To(BeTrue())
	})

	// Cleanup on delete: the per-DM store-recovery state lives in status, so it is
	// garbage-collected with the object (no in-memory map keyed by UID to leak).
	// The rollout-budget reservation (keyed ns/name) is released in the NotFound
	// path. This asserts no state outlives the DM.
	It("leaves no per-DM state after the DM is deleted during recovery", func() {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "del"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		r := newR()
		r.MaxConcurrentRollouts = 1
		// Record a recovery attempt and a budget reservation for it.
		Expect(r.countFailedRecovery(dm, types.UID("j1"))).To(BeFalse())
		Expect(k8sClient.Status().Update(ctx, dm)).To(Succeed())
		r.budgetReservations = map[string]string{namespace + "/del": "somerev"}

		// Delete the DM, then reconcile its NotFound: the reservation is released;
		// the status (and its StoreRecovery) is gone with the object.
		Expect(k8sClient.Delete(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "del"}})).To(Succeed())
		Eventually(func() bool {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "del"}})
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "del"},
				&decisionmodelv1alpha1.DecisionModel{}))
		}, "10s", "50ms").Should(BeTrue())
		_, ok := r.budgetReservations[namespace+"/del"]
		Expect(ok).To(BeFalse(), "the rollout-budget reservation is released on delete")
	})
})

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

// A FIRST rollout (no stable yet) that is refused mid-flight — the spec edited to
// something the preflight rejects, or the operator restarted with
// --allow-unpinned-runtime-images off — must not leave the in-flight candidate's
// Deployment / Job / PVC (and its rollout-budget slot) behind forever. The
// no-stable refusal paths abandon the candidate the same way the stable path does.
var _ = Describe("a no-stable preflight refusal abandons the in-flight candidate", func() {
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
	newR := func(allowUnpinned bool) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                    map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                     &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder:                   events.NewFakeRecorder(256),
			AllowUnpinnedRuntimeImages: allowUnpinned,
			MaxConcurrentRollouts:      1,
			WatchNamespaces:            []string{namespace},
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	cond := func(name, t string) *metav1.Condition {
		dm := getDM(name)
		for i := range dm.Status.Conditions {
			if dm.Status.Conditions[i].Type == t {
				return &dm.Status.Conditions[i]
			}
		}
		return nil
	}
	// workloadsExist reports whether each owned object is still present AND not
	// being deleted. envtest has no garbage collector, so a Delete that uses
	// background/foreground propagation (Jobs) or hits a protection finalizer
	// (PVCs) leaves the object with a deletionTimestamp rather than removing it;
	// such an object is treated as "collected" (the operator issued the delete).
	live := func(obj client.Object, name string) bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj); err != nil {
			return false // NotFound (or other) -> not live
		}
		return obj.GetDeletionTimestamp() == nil
	}
	workloadsExist := func(name, rev string) (dep, job, pvc bool) {
		dep = live(&appsv1.Deployment{}, name+"-"+rev)
		job = live(&batchv1.Job{}, jobName(name, rev))
		pvc = live(&corev1.PersistentVolumeClaim{}, name+"-store-"+rev)
		return
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("abandon-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// driveFirstCandidate creates a first rollout (no stable) and reconciles until
	// its candidate revision is recorded and its prefetch Job + store PVC exist.
	driveFirstCandidate := func(r *DecisionModelReconciler, name string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec(r, name)
		rev := getDM(name).Status.CandidateRevision.Hash
		dep, job, pvc := workloadsExist(name, rev)
		Expect(dep || job || pvc).To(BeTrue(), "the first candidate allocated at least a Job/PVC")
		return rev
	}
	// markJob completes the candidate's prefetch Job so the next reconcile creates
	// the serving Deployment (the Starting phase), giving GC a Deployment to delete.
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
	// driveToStarting advances a first candidate to the Starting phase, where its
	// serving Deployment exists.
	driveToStarting := func(r *DecisionModelReconciler, name string) string {
		rev := driveFirstCandidate(r, name)
		markJob(name, rev)
		rec(r, name)
		dep, _, _ := workloadsExist(name, rev)
		Expect(dep).To(BeTrue(), "the candidate's serving Deployment exists in Starting")
		return rev
	}

	It("abandons the candidate when the runtime image becomes unpinned (operator restarted with the flag off)", func() {
		// First rollout admitted with the flag ON (pre-enforcement build).
		r1 := newR(true)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "flip"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				RuntimeVersion: fakeUnpinnedRuntimeVersion,
			},
		})).To(Succeed())
		rec(r1, "flip")
		rev := getDM("flip").Status.CandidateRevision.Hash
		dep, job, pvc := workloadsExist("flip", rev)
		Expect(dep || job || pvc).To(BeTrue(), "the first candidate allocated workloads")

		// Operator restart with --allow-unpinned-runtime-images OFF: the candidate
		// is now refused. It must be abandoned, not left holding its workloads.
		r2 := newR(false)
		rec(r2, "flip")

		Expect(cond("flip", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonUnpinnedRuntimeImage))
		Expect(getDM("flip").Status.CandidateRevision).To(BeNil(), "the in-flight candidate is abandoned")
		Eventually(func() bool {
			rec(r2, "flip")
			d, j, p := workloadsExist("flip", rev)
			return !d && !j && !p
		}, "5s", "50ms").Should(BeTrue(), "the abandoned candidate's Deployment/Job/PVC are collected")
	})

	It("abandons the candidate when the spec is edited to an invalid runtimeVersion", func() {
		r := newR(false)
		rev := driveFirstCandidate(r, "badver")

		// Edit the spec to a too-old runtimeVersion: the no-stable preflight refuses
		// with InvalidRuntimeVersion (degradeSecretReason path).
		Expect(updateDM(ctx, namespace, "badver", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1"
		})).To(Succeed())
		rec(r, "badver")

		Expect(cond("badver", decisionmodelv1alpha1.ConditionReady).Reason).To(Equal(reasonInvalidRuntimeVersion))
		Expect(getDM("badver").Status.CandidateRevision).To(BeNil(), "the in-flight candidate is abandoned")
		Eventually(func() bool {
			rec(r, "badver")
			d, j, p := workloadsExist("badver", rev)
			return !d && !j && !p
		}, "5s", "50ms").Should(BeTrue(), "the abandoned candidate's workloads are collected")
	})

	It("abandons the candidate when a security guard refuses the edited spec", func() {
		r := newR(false)
		r.AllowImageOverride = false
		rev := driveFirstCandidate(r, "sec")

		// Edit spec.image without --allow-image-override: guardSecurity refuses it
		// (Failed/ImageOverrideNotAllowed). The candidate must still be abandoned.
		Expect(updateDM(ctx, namespace, "sec", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Image = "registry.example.com/ollaya:tag"
		})).To(Succeed())
		rec(r, "sec")

		Expect(getDM("sec").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(getDM("sec").Status.CandidateRevision).To(BeNil(), "the in-flight candidate is abandoned")
		Eventually(func() bool {
			rec(r, "sec")
			d, j, p := workloadsExist("sec", rev)
			return !d && !j && !p
		}, "5s", "50ms").Should(BeTrue(), "the abandoned candidate's workloads are collected")
	})

	It("starts a fresh candidate once the spec becomes valid again", func() {
		r := newR(false)
		_ = driveFirstCandidate(r, "revive")
		Expect(updateDM(ctx, namespace, "revive", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1" // invalid -> refused, candidate abandoned
		})).To(Succeed())
		rec(r, "revive")
		Expect(getDM("revive").Status.CandidateRevision).To(BeNil())

		// Fix the spec to a valid, DIFFERENT revision (new model -> new store name,
		// so it does not collide with the abandoned revision's PVC, which lingers
		// Terminating under envtest's missing garbage collector): a fresh candidate
		// starts normally.
		Expect(updateDM(ctx, namespace, "revive", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = ""
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		rec(r, "revive")
		cand := getDM("revive").Status.CandidateRevision
		Expect(cand).NotTo(BeNil(), "a valid spec resumes the rollout")
		Expect(cand.Model).To(Equal("kev:en"))
	})

	It("collects the leftovers on a later reconcile when GC fails once", func() {
		r := newR(false)
		rev := driveToStarting(r, "gcfail")

		// A client that rejects the first owned-Deployment delete, then allows it.
		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		var deleteFailed atomic.Bool
		failing := interceptor.NewClient(wc, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok && deleteFailed.CompareAndSwap(false, true) {
					return fmt.Errorf("injected delete failure")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		})
		rf := newR(false)
		rf.Client = failing

		// Refuse the candidate; the status clear persists, but the first GC delete
		// of the Deployment fails -> the reconcile returns the GC error (retryable).
		Expect(updateDM(ctx, namespace, "gcfail", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1"
		})).To(Succeed())
		_, err := rf.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "gcfail"}})
		Expect(err).To(HaveOccurred(), "a GC error is returned so the workqueue retries")
		Expect(deleteFailed.Load()).To(BeTrue(), "the Deployment delete was attempted and rejected once")
		Expect(getDM("gcfail").Status.CandidateRevision).To(BeNil(), "the clear is already durable")

		// A later reconcile (candidate already nil) must still run GC and collect.
		Eventually(func() bool {
			rec(rf, "gcfail")
			d, j, p := workloadsExist("gcfail", rev)
			return !d && !j && !p
		}, "5s", "50ms").Should(BeTrue(), "a later reconcile collects the leftovers")
	})

	It("collects the leftovers after a crash between the status write and GC", func() {
		r := newR(false)
		rev := driveToStarting(r, "crash")

		// Simulate the crash: the candidate-clear was persisted but GC never ran.
		// status has no candidate; the Deployment/Job/PVC are still present, and the
		// DM is in the refusal state (invalid runtimeVersion).
		Expect(updateDM(ctx, namespace, "crash", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1"
		})).To(Succeed())
		Expect(updateDMStatus(ctx, namespace, "crash", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.CandidateRevision = nil
		})).To(Succeed())
		dep, _, _ := workloadsExist("crash", rev)
		Expect(dep).To(BeTrue(), "precondition: the orphan Deployment is still present")

		// The next reconcile (candidate already nil) must collect the orphans.
		Eventually(func() bool {
			rec(r, "crash")
			d, j, p := workloadsExist("crash", rev)
			return !d && !j && !p
		}, "5s", "50ms").Should(BeTrue(), "the next reconcile collects the orphaned workloads")
	})
})

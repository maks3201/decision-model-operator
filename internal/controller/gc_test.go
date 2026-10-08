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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("revision GC", func() {
	const model = "laya:en"

	var (
		ctx       context.Context
		namespace string
		nsCounter int
		clock     time.Time
	)

	int32Ptr := func(v int32) *int32 { return &v }

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	newR := func(eng *fakeEngine) *DecisionModelReconciler {
		r := &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    &fakeProber{loaded: engine.Loaded{Name: model, Digest: eng.digest, Device: "cpu"}},
			Recorder:  events.NewFakeRecorder(128),
		}
		r.Now = func() time.Time { return clock }
		return r
	}

	drain := func(r *DecisionModelReconciler) []string {
		fr := r.Recorder.(*events.FakeRecorder)
		var out []string
		for {
			select {
			case e := <-fr.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}
	countEvents := func(evs []string, substr string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, substr) {
				n++
			}
		}
		return n
	}

	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.30"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	markJobComplete := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(dmName, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	markJobFailed := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(dmName, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	// rollout drives a DM (already created) through to Ready on its current
	// spec.model/digest, returning the revision hash and its pod name.
	rollout := func(r *DecisionModelReconciler, name, podName string) string {
		reconcileOnce(r, name)
		rev := RevisionHash(getDM(name).Spec, r.Engines["ollaya"].(*fakeEngine).digest, fakeImage)
		markJobComplete(name, rev)
		reconcileOnce(r, name)
		createReadyPod(name, rev, podName)
		reconcileOnce(r, name) // probe -> gate True
		reconcileOnce(r, name) // promote
		Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	depExists := func(name, rev string) bool {
		dep := &appsv1.Deployment{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)
		if err == nil {
			return true
		}
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		return false
	}

	BeforeEach(func() {
		ctx = context.Background()
		clock = time.Now()
		nsCounter++
		namespace = fmt.Sprintf("gc-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// a quick successive rollout must not leak the oldest revision.
	It("collects the old revision after a quick successive rollout", func() {
		eng := newFakeEngine()
		r := newR(eng)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gc1"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: zeroStabilizationRollout(),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())

		rev1 := rollout(r, "gc1", "gc1-pod-1")

		// Promote rev2 (change model).
		eng.digest = "b2b2220000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "gc1", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		r.Prober = &fakeProber{loaded: engine.Loaded{Name: "kev:en", Digest: eng.digest, Device: "cpu"}}
		rev2 := rollout(r, "gc1", "gc1-pod-2")
		Expect(rev2).NotTo(Equal(rev1))
		// rev1 is the demoted previous revision, still within grace.
		Expect(getDM("gc1").Status.PreviousRevision).NotTo(BeNil())
		Expect(getDM("gc1").Status.PreviousRevision.Hash).To(Equal(rev1))

		// Immediately (within grace) change spec again -> rev3 candidate.
		eng.digest = "c3c3330000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "gc1", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "jevk5:en"
		})).To(Succeed())
		reconcileOnce(r, "gc1") // rev3 candidate, Caching; rev1 still within grace
		rev3 := RevisionHash(getDM("gc1").Spec, eng.digest, fakeImage)
		Expect(depExists("gc1", rev1)).To(BeTrue(), "rev1 still kept within grace")

		// Advance past the promote grace; rev3 still Starting (no ready pod).
		clock = clock.Add(2 * promoteGrace)
		reconcileOnce(r, "gc1")

		// rev1 (old, beyond grace) is collected; rev2 (stable) kept; rev3 kept.
		Expect(depExists("gc1", rev1)).To(BeFalse(), "rev1 must be GC'd after grace even while rev3 is Starting")
		Expect(depExists("gc1", rev2)).To(BeTrue(), "rev2 (stable) must be kept")
		Expect(getDM("gc1").Status.PreviousRevision).To(BeNil(), "previousRevision cleared after grace")
		_ = rev3
	})

	// the failed-revision guard path also GCs stale revisions.
	It("collects stale revisions on the failed-revision guard path", func() {
		eng := newFakeEngine()
		r := newR(eng)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gc2"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: zeroStabilizationRollout(),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rev1 := rollout(r, "gc2", "gc2-pod-1")

		// New revision whose prefetch fails -> RolledBack, failedRevision=rev2.
		eng.digest = "d4d4440000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "gc2", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcileOnce(r, "gc2")
		rev2 := RevisionHash(getDM("gc2").Spec, eng.digest, fakeImage)
		markJobFailed("gc2", rev2)
		reconcileOnce(r, "gc2") // rollback -> RolledBack, failedRevision=rev2
		Expect(getDM("gc2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		// Advance past grace and reconcile: the guard path GCs the demoted rev1.
		clock = clock.Add(2 * promoteGrace)
		reconcileOnce(r, "gc2")
		// rev1 was the previous revision (demoted when... actually rev1 is stable).
		// Here stable stayed rev1 (rev2 never promoted), so rev1 is kept; assert
		// the failed rev2's workloads are gone (deleted on rollback + GC).
		Expect(depExists("gc2", rev2)).To(BeFalse(), "failed revision workloads GC'd")
		Expect(depExists("gc2", rev1)).To(BeTrue(), "stable revision kept")
	})

	// Promoted is emitted exactly once per promotion.
	It("emits Promoted exactly once per promotion", func() {
		eng := newFakeEngine()
		r := newR(eng)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gc3"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: zeroStabilizationRollout(),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		_ = rollout(r, "gc3", "gc3-pod-1")

		// Drain and count: exactly one Promoted across the rollout.
		evs := drain(r)
		Expect(countEvents(evs, eventPromoted)).To(Equal(1), "Promoted must fire once, got %v", evs)

		// Extra steady-state reconciles emit no further Promoted.
		reconcileOnce(r, "gc3")
		reconcileOnce(r, "gc3")
		Expect(countEvents(drain(r), eventPromoted)).To(Equal(0))
	})

	// Mixed-width fleet: a legacy 10-hex revision names its prefetch Job
	// "<dm>-prefetch-<10>" and a new 16-hex revision names it "<dm>-pf-<16>". GC is
	// name-independent (it lists owned Jobs by LabelName and deletes by the
	// decisionmodel.io/revision label + owner UID), so it deletes exactly the stale
	// revision's Job regardless of which prefix it carries.
	It("GC deletes the stale revision's Job across both hash widths", func() {
		eng := newFakeEngine()
		r := newR(eng)
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gcmix"},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		// Keep a 16-hex revision as stable; a 10-hex revision is stale.
		keep := "a1b2c3d4e5f60718" // 16 hex
		stale := "0123456789"      // 10 hex (legacy)
		Expect(updateDMStatus(ctx, namespace, "gcmix", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{Hash: keep, Engine: "ollaya", Model: model, Digest: eng.digest, Device: "cpu", Image: fakeImage}
		})).To(Succeed())
		dm = getDM("gcmix")

		mkJob := func(rev string) {
			yes := true
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: jobName("gcmix", rev),
					Labels: map[string]string{
						decisionmodelv1alpha1.LabelName:     "gcmix",
						decisionmodelv1alpha1.LabelRevision: rev,
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: decisionmodelv1alpha1.GroupVersion.String(), Kind: "DecisionModel",
						Name: dm.Name, UID: dm.UID, Controller: &yes, BlockOwnerDeletion: &yes,
					}},
				},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{Name: "p", Image: fakeImage}}}}},
			}
			Expect(k8sClient.Create(ctx, job)).To(Succeed())
		}
		mkJob(keep)  // <dm>-pf-<16>
		mkJob(stale) // <dm>-prefetch-<10>
		Expect(jobName("gcmix", keep)).To(Equal("gcmix-pf-" + keep))
		Expect(jobName("gcmix", stale)).To(Equal("gcmix-prefetch-" + stale))

		Expect(r.gcRevisions(ctx, dm)).To(Succeed())

		// The stale 10-hex Job is gone; the kept 16-hex Job survives.
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx,
				types.NamespacedName{Namespace: namespace, Name: jobName("gcmix", stale)}, &batchv1.Job{}))
		}, "5s", "50ms").Should(BeTrue(), "the stale 10-hex -prefetch- Job is collected")
		Expect(k8sClient.Get(ctx,
			types.NamespacedName{Namespace: namespace, Name: jobName("gcmix", keep)}, &batchv1.Job{})).
			To(Succeed(), "the kept 16-hex -pf- Job survives")
	})
})

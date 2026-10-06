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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("post-promotion stabilization window", func() {
	const digest2 = "b2b2450000000000000000000000000000000000000000000000000000000000"
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
	markJob := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	gatedPod := func(name, rev string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-pod-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.90"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	serviceRev := func(name string) string {
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}
	depExists := func(name, rev string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, &appsv1.Deployment{})
		return err == nil
	}

	// newRec builds a reconciler with a per-revision prober so a test can make the
	// new stable's Pod lose the model (gate mismatch) during the window.
	newRec := func(prober *revProber) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(256),
			Now:      func() time.Time { return clock },
		}
	}
	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}
	drainHas := func(r *DecisionModelReconciler, substr string) bool {
		fr := r.Recorder.(*events.FakeRecorder)
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, substr) {
					return true
				}
			default:
				return false
			}
		}
	}

	// setStable creates a DM, drives rev1 to Ready, then changes the model to drive
	// rev2 to Ready and promote it (rev1 becomes the previous revision in the
	// window). Returns (rev1, rev2). The prober keeps both revisions healthy.
	setStable := func(r *DecisionModelReconciler, prober *revProber, name string, stabilization *metav1.Duration) (string, string) {
		rollout := &decisionmodelv1alpha1.RolloutSpec{Stabilization: stabilization}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1), Rollout: rollout,
			},
		})).To(Succeed())
		prober.fallback = engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}
		rev1 := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		rec(r, name)
		markJob(name, rev1)
		rec(r, name)
		gatedPod(name, rev1)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Second revision (model change) -> promote; rev1 becomes previous.
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		prober.set("", engine.Loaded{}) // clear fallback use; set per-rev below
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, digest2, fakeImage)
		prober.fallback = engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"}
		prober.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})
		rec(r, name)
		markJob(name, rev2)
		rec(r, name)
		gatedPod(name, rev2)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(rev2))
		Expect(getDM(name).Status.PreviousRevision).NotTo(BeNil())
		Expect(getDM(name).Status.PreviousRevision.Hash).To(Equal(rev1))
		return rev1, rev2
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("stab-%d", counter)
		clock = time.Now()
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("keeps the previous revision through the window, then collects it when healthy", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-healthy", nil) // default 5m

		// Within the window the previous revision's Deployment is kept.
		clock = clock.Add(2 * time.Minute)
		rec(r, "s-healthy")
		Expect(depExists("s-healthy", rev1)).To(BeTrue(), "previous kept within the window")
		stabilizing := meta_Find(getDM("s-healthy"), decisionmodelv1alpha1.ConditionStabilizing)
		Expect(stabilizing).NotTo(BeNil())
		Expect(stabilizing.Status).To(Equal(metav1.ConditionTrue))

		// Past the window, healthy: previous collected, Stabilized emitted.
		clock = clock.Add(5 * time.Minute)
		Eventually(func() bool { rec(r, "s-healthy"); return depExists("s-healthy", rev1) }, "10s", "50ms").
			Should(BeFalse(), "previous collected after a healthy window")
		Expect(getDM("s-healthy").Status.PreviousRevision).To(BeNil())
		Expect(serviceRev("s-healthy")).To(Equal(rev2))
		Expect(drainHas(r, "Stabilized")).To(BeTrue())
	})

	It("rolls back to the previous revision when the new stable's gate mismatches", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-mismatch", nil)

		// The new stable loses the model (wrong digest) -> immediate rollback.
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})
		// Advance a little (well inside the window) and reconcile until the gate
		// flips and the rollback fires.
		clock = clock.Add(2 * time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-mismatch") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		dm := getDM("s-mismatch")
		Expect(serviceRev("s-mismatch")).To(Equal(rev1), "traffic switched back to the previous revision")
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev1))
		Expect(dm.Status.FailedRevision).NotTo(BeNil())
		Expect(dm.Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(dm.Status.FailedRevision.Reason).To(Equal(reasonPostPromotionUnhealthy))
		Expect(dm.Status.FailedRevision.FailedAt).NotTo(BeNil())
		Expect(dm.Status.PreviousRevision).To(BeNil())
		Expect(drainHas(r, "RolledBackAfterPromotion")).To(BeTrue())
	})

	It("rolls back after the debounce when the new stable drops below quorum", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-quorum", nil)

		// Delete the new stable's only Pod: it drops below quorum (0 of 1).
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "s-quorum-pod-" + rev2}})).To(Succeed())

		// First reconcile marks it unhealthy (debounce starts), no rollback yet.
		rec(r, "s-quorum")
		Expect(getDM("s-quorum").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		// Past the debounce it rolls back.
		clock = clock.Add(postPromotionDebounce + time.Second)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-quorum") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-quorum")).To(Equal(rev1))
		Expect(getDM("s-quorum").Status.FailedRevision.Hash).To(Equal(rev2))
		_ = rev1
	})

	It("with stabilization 0 collects the previous revision after the short grace", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, _ := setStable(r, prober, "s-zero", &metav1.Duration{Duration: 0})
		// No stabilizing condition is set when the window is disabled.
		Expect(meta_Find(getDM("s-zero"), decisionmodelv1alpha1.ConditionStabilizing)).To(BeNil())
		// After the endpoint-gap grace the previous revision is collected.
		clock = clock.Add(2 * promoteGrace)
		Eventually(func() bool { rec(r, "s-zero"); return depExists("s-zero", rev1) }, "10s", "50ms").Should(BeFalse())
		Expect(getDM("s-zero").Status.PreviousRevision).To(BeNil())
	})

	It("ends the window and does not roll back when a new candidate starts mid-window", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-super", nil)
		_ = rev2

		// A spec change mid-window starts a new candidate; the window ends.
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = "c3c3450000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "s-super", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "jevk5:en"
		})).To(Succeed())
		rec(r, "s-super")
		Expect(getDM("s-super").Status.PreviousRevision).To(BeNil(), "the window ends when a new candidate starts")
		Expect(getDM("s-super").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		_ = rev1
	})

	It("is idempotent across a restart: a fresh reconciler keeps the window and collects on time", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-restart", nil)

		// Simulate a restart: a brand-new reconciler (empty in-memory state) with
		// the same clock and persisted status.
		r2 := newRec(prober)
		clock = clock.Add(2 * time.Minute)
		rec(r2, "s-restart")
		Expect(depExists("s-restart", rev1)).To(BeTrue(), "window honoured from persisted status after restart")

		clock = clock.Add(5 * time.Minute)
		Eventually(func() bool { rec(r2, "s-restart"); return depExists("s-restart", rev1) }, "10s", "50ms").Should(BeFalse())
		Expect(serviceRev("s-restart")).To(Equal(rev2))
	})
})

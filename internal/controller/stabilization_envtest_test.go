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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	// settleDeployment marks a revision's Deployment as a completed rollout, the
	// way the real deployment controller does: Progressing=True with reason
	// NewReplicaSetAvailable and observedGeneration == generation. This stays set
	// even if Pods later go unready, so stableRolloutState reports rolloutIdle.
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
	// rollingDeployment marks a revision's Deployment mid-rollout, as the real
	// controller does during a scale-up or template change: generation bumped
	// (observedGeneration behind) and Progressing=True with reason ReplicaSetUpdated.
	rollingDeployment := func(name, rev string, specReplicas int32) {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		// observedGeneration deliberately behind generation: the change is not
		// observed yet. Also set the Progressing reason the controller uses.
		dep.Status.ObservedGeneration = dep.Generation - 1
		dep.Status.Replicas = specReplicas
		dep.Status.UpdatedReplicas = specReplicas - 1
		dep.Status.AvailableReplicas = 0
		dep.Status.ReadyReplicas = 0
		dep.Status.Conditions = []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue,
			Reason: "ReplicaSetUpdated", Message: "ReplicaSet is progressing.",
		}}
		Expect(k8sClient.Status().Update(ctx, dep)).To(Succeed())
	}
	// deadlineExceededDeployment marks a revision's Deployment as a stuck rollout:
	// Progressing=False, ProgressDeadlineExceeded.
	deadlineExceededDeployment := func(name, rev string) {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		dep.Status.ObservedGeneration = dep.Generation
		dep.Status.Conditions = []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
			Reason: "ProgressDeadlineExceeded", Message: "progress deadline exceeded",
		}}
		Expect(k8sClient.Status().Update(ctx, dep)).To(Succeed())
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
		// Mark the new stable fully rolled out, as a real Deployment controller
		// would once its Pods are ready; stabilizationFor then sees rolloutIdle.
		settleDeployment(name, rev2)
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

	// A shortfall that begins just before the window end must not be declared
	// Stabilized (which would GC the rollback target); the window has no more time
	// to prove the new stable, so it rolls back to the previous revision instead.
	It("rolls back at window end when the new stable is below quorum, never announcing Stabilized", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-endquorum", nil) // default 5m window

		// Advance to within the last debounce of the window, then drop the new
		// stable below quorum (delete its only Pod).
		clock = clock.Add(5*time.Minute - 10*time.Second)
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "s-endquorum-pod-" + rev2}})).To(Succeed())
		// This reconcile marks it unhealthy (debounce starts); still inside the
		// window and before the debounce, so no rollback yet.
		rec(r, "s-endquorum")
		Expect(getDM("s-endquorum").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		// Now cross the window end while still below quorum (debounce has NOT
		// elapsed). The window-end branch must roll back, not announce Stabilized.
		clock = clock.Add(30 * time.Second) // past the window, within the debounce
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-endquorum") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-endquorum")).To(Equal(rev1), "traffic back on the previous revision")
		Expect(getDM("s-endquorum").Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(getDM("s-endquorum").Status.PreviousRevision).To(BeNil(), "previous not GC'd as a healthy stabilize")
		// Drain once and assert on the collected events (drainHas consumes the
		// channel, so a single pass must check both).
		fr := r.Recorder.(*events.FakeRecorder)
		sawStabilized, sawRolledBack := false, false
		for drained := false; !drained; {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, "Stabilized") {
					sawStabilized = true
				}
				if strings.Contains(e, "RolledBackAfterPromotion") {
					sawRolledBack = true
				}
			default:
				drained = true
			}
		}
		Expect(sawStabilized).To(BeFalse(), "never announced Stabilized while below quorum")
		Expect(sawRolledBack).To(BeTrue())
	})

	// An intentional in-place rollout of the stable (replicas scale-up) drops
	// below quorum while new Pods load the model. That is NOT a health failure, so
	// it must not roll back; once the rollout settles it stabilizes normally.
	It("does not roll back a scale-up rollout that is below quorum, then stabilizes", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-scaleup", nil)

		// Scale 1 -> 3; the stable Deployment is mid-rollout (new Pods not ready).
		Expect(updateDM(ctx, namespace, "s-scaleup", func(dm *decisionmodelv1alpha1.DecisionModel) {
			three := int32(3)
			dm.Spec.Replicas = &three
		})).To(Succeed())
		clock = clock.Add(1 * time.Minute)
		rec(r, "s-scaleup") // applies the new replicas to the Deployment
		rollingDeployment("s-scaleup", rev2, 3)

		// Below quorum (ready=1 of 2) for longer than the debounce: still no
		// rollback, because the Deployment is mid-rollout.
		clock = clock.Add(postPromotionDebounce + time.Minute)
		for i := 0; i < 3; i++ {
			Expect(rec(r, "s-scaleup")).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		}
		st := meta_Find(getDM("s-scaleup"), decisionmodelv1alpha1.ConditionStabilizing)
		Expect(st).NotTo(BeNil())
		Expect(st.Status).To(Equal(metav1.ConditionTrue))
		Expect(st.Reason).To(Equal(reasonStableRolling))
		Expect(getDM("s-scaleup").Status.PreviousRevision).NotTo(BeNil(), "rollback target still kept")

		// The rollout settles (all 3 Pods ready); the window then completes healthy.
		for _, ip := range []string{"10.0.0.91", "10.0.0.92"} {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: "s-scaleup-pod-" + rev2 + "-" + ip[len(ip)-2:],
					Labels: map[string]string{decisionmodelv1alpha1.LabelName: "s-scaleup", decisionmodelv1alpha1.LabelRevision: rev2},
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
		clock = clock.Add(10 * time.Minute)
		Eventually(func() bool {
			// Keep the Deployment reported fully rolled out (envtest has no
			// Deployment controller; a reconcile may bump its generation).
			settleDeployment("s-scaleup", rev2)
			rec(r, "s-scaleup")
			return depExists("s-scaleup", rev1)
		}, "10s", "50ms").Should(BeFalse(), "previous collected once the rollout settled healthy")
		Expect(getDM("s-scaleup").Status.PreviousRevision).To(BeNil())
		_ = rev1
	})

	// API-key rotation on an RWO/replicas-1 stable uses Recreate: the only Pod is
	// gone for a cold load (0 ready) for longer than the debounce. That is an
	// intentional rollout, not a failure, so it must not roll back.
	It("does not roll back a key-rotation rollout with zero ready Pods", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-keyrot", nil)

		// Simulate the rolling Recreate: delete the Pod and mark the Deployment
		// mid-rollout (generation bumped, Progressing=ReplicaSetUpdated).
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "s-keyrot-pod-" + rev2}})).To(Succeed())
		rollingDeployment("s-keyrot", rev2, 1)

		clock = clock.Add(postPromotionDebounce + time.Minute)
		for i := 0; i < 3; i++ {
			Expect(rec(r, "s-keyrot")).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		}
		st := meta_Find(getDM("s-keyrot"), decisionmodelv1alpha1.ConditionStabilizing)
		Expect(st).NotTo(BeNil())
		Expect(st.Reason).To(Equal(reasonStableRolling))
		Expect(getDM("s-keyrot").Status.PreviousRevision).NotTo(BeNil())
		_ = rev1
	})

	// A genuine failure during a rollout (new Pods come up with the wrong model)
	// still rolls back immediately — the rollout mask only covers a quorum
	// shortfall, never a gate DigestMismatch/DeviceMismatch.
	It("rolls back immediately on a gate mismatch even while mid-rollout", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-rollfail", nil)

		rollingDeployment("s-rollfail", rev2, 2) // mid-rollout
		// The serving Pod reports the wrong device: an immediate failure.
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cuda"})
		clock = clock.Add(1 * time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-rollfail") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-rollfail")).To(Equal(rev1))
		Expect(getDM("s-rollfail").Status.FailedRevision.Hash).To(Equal(rev2))
	})

	// A rollout that never finishes must not suspend protection forever: once the
	// Deployment reports ProgressDeadlineExceeded, the shortfall is treated as a
	// failure and rolled back.
	It("rolls back a stuck rollout that exceeds its progress deadline", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-stuck", nil)

		// Below quorum (Pod gone) AND the rollout is stuck past its deadline.
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "s-stuck-pod-" + rev2}})).To(Succeed())
		deadlineExceededDeployment("s-stuck", rev2)
		clock = clock.Add(1 * time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-stuck") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-stuck")).To(Equal(rev1))
		dm := getDM("s-stuck")
		Expect(dm.Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(dm.Status.FailedRevision.Message).To(ContainSubstring("progress deadline"))
	})

	// Regression: a COMPLETED rollout (Progressing=True NewReplicaSetAvailable,
	// observedGeneration == generation) whose Pods later go unready has
	// available < spec but is NOT "rolling" — it must still roll back after the
	// debounce. This is the case that an availableReplicas-based signal broke.
	It("rolls back a completed rollout whose Pods go unready, despite available < spec", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-doneunready", nil) // setStable settles rev2

		// The rollout is complete; now its only Pod goes away (available < spec),
		// but the Deployment keeps Progressing=True NewReplicaSetAvailable, exactly
		// as a real controller reports a post-rollout health failure.
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: "s-doneunready-pod-" + rev2}})).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s-doneunready-" + rev2}, dep)).To(Succeed())
		dep.Status.ObservedGeneration = dep.Generation
		dep.Status.AvailableReplicas = 0 // Pods unready, but the rollout already completed
		dep.Status.ReadyReplicas = 0
		dep.Status.Conditions = []appsv1.DeploymentCondition{{
			Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue,
			Reason: "NewReplicaSetAvailable", Message: "ReplicaSet has successfully progressed.",
		}}
		Expect(k8sClient.Status().Update(ctx, dep)).To(Succeed())

		// First reconcile marks it unhealthy (not StableRolling); past the debounce
		// it rolls back.
		rec(r, "s-doneunready")
		st := meta_Find(getDM("s-doneunready"), decisionmodelv1alpha1.ConditionStabilizing)
		Expect(st).NotTo(BeNil())
		Expect(st.Reason).To(Equal(reasonPostPromotionUnhealthy), "a completed rollout is not StableRolling")
		clock = clock.Add(postPromotionDebounce + time.Second)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-doneunready") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-doneunready")).To(Equal(rev1))
		Expect(getDM("s-doneunready").Status.FailedRevision.Hash).To(Equal(rev2))
	})

	// A scale-up whose new Pods never become ready must not hold forever: the
	// rollout eventually trips ProgressDeadlineExceeded and rolls back.
	It("rolls back a scale-up whose new Pods never become ready (deadline)", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-scalestuck", nil)

		Expect(updateDM(ctx, namespace, "s-scalestuck", func(dm *decisionmodelv1alpha1.DecisionModel) {
			three := int32(3)
			dm.Spec.Replicas = &three
		})).To(Succeed())
		clock = clock.Add(1 * time.Minute)
		rec(r, "s-scalestuck")

		// While progressing: no rollback.
		rollingDeployment("s-scalestuck", rev2, 3)
		clock = clock.Add(postPromotionDebounce + time.Minute)
		Expect(rec(r, "s-scalestuck")).NotTo(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(meta_Find(getDM("s-scalestuck"), decisionmodelv1alpha1.ConditionStabilizing).Reason).
			To(Equal(reasonStableRolling))

		// The new Pods never come up: the rollout trips its deadline -> rollback.
		deadlineExceededDeployment("s-scalestuck", rev2)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, "s-scalestuck") }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-scalestuck")).To(Equal(rev1))
		Expect(getDM("s-scalestuck").Status.FailedRevision.Message).To(ContainSubstring("progress deadline"))
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

	// Durability-first: if the status write loses an optimistic-lock race, the
	// rollback must NOT touch the Service or delete the failed revision — otherwise
	// the persisted status (still stable=<failed>) and the cluster would disagree
	// and the next reconcile would re-create the bad model as stable. The next
	// reconcile then redoes the whole rollback from fresh state.
	It("does not move the Service or delete workloads when the status write conflicts", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-conflict", nil)

		// The new stable loses the model: the next reconcile wants to roll back.
		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})
		clock = clock.Add(2 * time.Minute)

		// A client that fails the first status write with a Conflict, then behaves
		// normally. recoverable: only the status subresource write is intercepted.
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
						"s-conflict", fmt.Errorf("stale"))
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
		rc := newRec(prober)
		rc.Client = c

		// Reconcile with the conflicting write: the rollback decision is made but
		// the status write fails, so Service and the failed Deployment are intact.
		_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s-conflict"}})
		Expect(failOnce).To(BeTrue(), "the status write was attempted and conflicted")
		Expect(serviceRev("s-conflict")).To(Equal(rev2), "Service unchanged on a conflicting write")
		Expect(depExists("s-conflict", rev2)).To(BeTrue(), "failed revision not deleted on a conflicting write")
		Expect(getDM("s-conflict").Status.StableRevision.Hash).To(Equal(rev2), "persisted stable unchanged")

		// Next reconcile (same client, no more failures) completes the rollback.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s-conflict"}})
			return getDM("s-conflict").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(serviceRev("s-conflict")).To(Equal(rev1), "rollback completed on the retry")
		Expect(getDM("s-conflict").Status.StableRevision.Hash).To(Equal(rev1))
		Expect(getDM("s-conflict").Status.FailedRevision.Hash).To(Equal(rev2))
	})

	// A crash after the durable status write but before the Service switch / delete
	// must still converge: the next reconcile runs the normal stable path from the
	// persisted status (stable=previous, failed=new) and points the Service back
	// and GCs the failed revision.
	It("converges when a crash lands after the status write but before Service and delete", func() {
		prober := &revProber{}
		r := newRec(prober)
		rev1, rev2 := setStable(r, prober, "s-crash", nil)

		prober.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})
		clock = clock.Add(2 * time.Minute)

		// A client that lets the status write through but fails the Service Update
		// (and any workload Delete) once, simulating a crash right after the write.
		var crashed bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.Service); ok && !crashed {
					crashed = true
					return fmt.Errorf("simulated crash after status write")
				}
				return cl.Update(ctx, obj, opts...)
			},
		})
		rc := newRec(prober)
		rc.Client = c

		// The reconcile persists the rollback status, then "crashes" on the Service
		// switch. Status is durable; Service and the failed Deployment are not yet
		// converged.
		_, _ = rc.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s-crash"}})
		Expect(crashed).To(BeTrue(), "the Service switch was attempted after the status write")
		Expect(getDM("s-crash").Status.StableRevision.Hash).To(Equal(rev1), "rollback status is durable")
		Expect(getDM("s-crash").Status.FailedRevision.Hash).To(Equal(rev2))
		Expect(getDM("s-crash").Status.PreviousRevision).To(BeNil())

		// A fresh reconciler (normal client) runs the stable path from the
		// persisted status and converges: Service back to previous, failed GCed.
		r2 := newRec(prober)
		Eventually(func() string {
			_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s-crash"}})
			return serviceRev("s-crash")
		}, "10s", "50ms").Should(Equal(rev1), "Service converges to the previous revision")
		Eventually(func() bool {
			_, _ = r2.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s-crash"}})
			return depExists("s-crash", rev2)
		}, "10s", "50ms").Should(BeFalse(), "the failed revision is collected")
	})
})

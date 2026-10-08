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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// operator restart resilience. Every phase must resume from cluster
// state when the manager restarts (in-memory eval store / warmup map / buffered
// events+metrics are gone). These tests build a *fresh* reconciler mid-phase and
// assert the outcome plus that Events/metrics are not double-counted.
var _ = Describe("restart resilience", func() {
	const model = "laya:en"

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	reconcile1 := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	countEvents := func(r *DecisionModelReconciler, substrs ...string) map[string]int {
		fr := r.Recorder.(*events.FakeRecorder)
		out := map[string]int{}
		for _, s := range substrs {
			out[s] = 0
		}
		for drained := false; !drained; {
			select {
			case e := <-fr.Events:
				for _, s := range substrs {
					if strings.Contains(e, s) {
						out[s]++
					}
				}
			default:
				drained = true
			}
		}
		return out
	}

	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      podName,
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
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	markJobComplete := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: namespace, Name: jobName(dmName, rev),
		}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	datasetCM := func(name, jsonl string) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Data:       map[string]string{"cases.jsonl": jsonl},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(128),
		}
	}

	createEvalDM := func(name, cmName string) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine:   "ollaya",
				Model:    model,
				Device:   "cpu",
				Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
						DatasetRef: decisionmodelv1alpha1.DatasetRef{
							ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: cmName, Key: "cases.jsonl"},
						},
						MinAccuracy: "0.90",
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

	// allChoice builds a JSONL dataset of n cases; the first `correct` expect
	// "billing" (the fake's answer) and the rest expect "other".
	allChoice := func(n, correct int) string {
		var b []byte
		for i := 0; i < n; i++ {
			exp := "other"
			if i < correct {
				exp = "billing"
			}
			line := `{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"` + exp + `"}}` + "\n"
			b = append(b, []byte(line)...)
		}
		return string(b)
	}

	// driveToEvaluating brings a DM with one ready candidate Pod up to the point
	// where evaluation has started (phase Evaluating, EvaluationStarted emitted),
	// returning the revision hash. The eval engine is slow so the eval is still
	// running when this returns.
	driveToEvaluating := func(r *DecisionModelReconciler, name string) string {
		reconcile1(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, rev)
		reconcile1(r, name)
		createReadyPod(name, rev, name+"-pod-0")
		reconcile1(r, name) // probe -> ready -> starts eval
		return rev
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "restart-test-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		resetMetrics()
	})

	It("does not re-emit EvaluationStarted or double-count rollouts after a restart mid-evaluation", func() {
		// A slow eval so it is still running when we simulate the restart.
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", delay: 2 * time.Second}
		prober := &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}}
		r1 := newReconciler(eng, prober)
		datasetCM("golden-restart", allChoice(10, 10)) // 100% -> will promote
		createEvalDM("rev1", "golden-restart")

		rev := driveToEvaluating(r1, "rev1")

		// r1 announced the evaluation exactly once and it is running.
		Expect(getDM("rev1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseEvaluating))
		Expect(countEvents(r1, eventEvaluationStarted)[eventEvaluationStarted]).To(Equal(1),
			"r1 announces the eval once")

		// Simulate a manager restart / leadership change: a brand-new reconciler
		// with empty in-memory stores, using a fast engine so its restarted eval
		// completes and promotes.
		fastEng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r2 := newReconciler(fastEng, prober)

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			_, _ = r2.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: "rev1"},
			})
			dm := &decisionmodelv1alpha1.DecisionModel{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rev1"}, dm); err != nil {
				return ""
			}
			return dm.Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		dm := getDM("rev1")
		Expect(dm.Status.StableRevision).NotTo(BeNil())
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev))

		// The restarted reconciler must NOT re-announce EvaluationStarted (it was
		// already announced before the restart), but it does emit Promoted once.
		r2events := countEvents(r2, eventEvaluationStarted, eventPromoted)
		Expect(r2events[eventEvaluationStarted]).To(Equal(0),
			"restarted reconciler must not re-announce EvaluationStarted")
		Expect(r2events[eventPromoted]).To(Equal(1), "Promoted fires once after restart")

		// rollouts_total counts the promotion exactly once across the restart.
		got := testutil.ToFloat64(rolloutsTotal.WithLabelValues(namespace, "rev1", rolloutPromoted))
		Expect(got).To(Equal(1.0), "promoted rollout counted exactly once")
	})

	It("keeps the evaluation deadline anchored to phaseTransitionTime across a restart", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", delay: time.Hour}
		prober := &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}}
		clock := time.Now()
		r1 := newReconciler(eng, prober)
		r1.Now = func() time.Time { return clock }
		datasetCM("golden-deadline", allChoice(10, 10))
		createEvalDM("rev2", "golden-deadline")

		driveToEvaluating(r1, "rev2")
		startTransition := getDM("rev2").Status.PhaseTransitionTime
		Expect(startTransition).NotTo(BeNil())

		// Restart: fresh reconciler, same frozen clock. The phase stays Evaluating
		// and its transition time must not be reset (deadline anchored).
		r2 := newReconciler(eng, prober)
		r2.Now = func() time.Time { return clock }
		reconcile1(r2, "rev2")

		dm := getDM("rev2")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseEvaluating))
		Expect(dm.Status.PhaseTransitionTime.Time.Equal(startTransition.Time)).To(BeTrue(),
			"phaseTransitionTime must not be reset on restart, so the deadline does not extend")
	})

	It("still garbage-collects the previous revision after a restart, using persisted promotedAt", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}}
		clock := time.Now()
		r1 := newReconciler(eng, prober)
		r1.Now = func() time.Time { return clock }

		// Bring rev up to Ready (no eval gate: promotes on model-ready).
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gc1"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: zeroStabilizationRollout(),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		reconcile1(r1, "gc1")
		rev1 := RevisionHash(getDM("gc1").Spec, defaultDigest, fakeImage)
		markJobComplete("gc1", rev1)
		reconcile1(r1, "gc1")
		createReadyPod("gc1", rev1, "gc1-pod-0")
		reconcile1(r1, "gc1")
		Expect(getDM("gc1").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))

		// Change the device -> new revision; promote it so rev1 becomes the
		// previousRevision with a persisted promotedAt.
		Expect(updateDM(ctx, namespace, "gc1", func(cur *decisionmodelv1alpha1.DecisionModel) {
			cur.Spec.Device = "cuda"
		})).To(Succeed())
		reconcile1(r1, "gc1")
		// The device changed to cuda, so the resolved serving image is the CUDA
		// image (the fake engine mirrors the real engine's device-specific image).
		rev2 := RevisionHash(getDM("gc1").Spec, defaultDigest, fakeImageCUDA)
		Expect(rev2).NotTo(Equal(rev1))
		markJobComplete("gc1", rev2)
		reconcile1(r1, "gc1")
		createReadyPod("gc1", rev2, "gc1-pod-1")
		reconcile1(r1, "gc1")
		prev := getDM("gc1").Status.PreviousRevision
		Expect(prev).NotTo(BeNil())
		Expect(prev.Hash).To(Equal(rev1))
		Expect(prev.PromotedAt).NotTo(BeNil())

		// Restart AFTER the grace window elapses (advance a fresh reconciler's
		// clock). GC must collect rev1's Deployment using the persisted promotedAt.
		r2 := newReconciler(eng, prober)
		r2.Now = func() time.Time { return clock.Add(2 * promoteGrace) }
		reconcile1(r2, "gc1")

		dep := &appsv1.Deployment{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "gc1-" + rev1}, dep)
		Expect(err).To(HaveOccurred(), "previous revision Deployment should be GC'd after grace, even across a restart")
	})
})

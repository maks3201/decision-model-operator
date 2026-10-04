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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// deciderFakeEngine is a fakeEngine that also implements engine.Decider. Its
// Decide answers every question with a fixed choice, so a test can control
// accuracy by how many dataset cases expect that choice. A delay simulates a
// slow runtime (for timeout / cancellation tests). conf, when >0, sets the
// probability assigned to the chosen label (for calibration tests). confByModel,
// when it has an entry for the request's model, overrides conf for that model —
// this lets a test fix the stable (baseline) and candidate confidences up front
// so neither is read through a field mutated concurrently with the async eval
// goroutine (the cal3 data race).
type deciderFakeEngine struct {
	*fakeEngine
	choice      string
	conf        float64
	confByModel map[string]float64
	delay       time.Duration
	// decideErr, when set, makes Decide return a transport error. Atomic so a
	// test can clear it between reconciles to simulate a blip that recovers.
	decideErr atomic.Bool
}

func (d *deciderFakeEngine) Decide(
	ctx context.Context,
	_, _ string,
	req engine.DecideRequest,
) (engine.DecideResponse, error) {
	if d.decideErr.Load() {
		return engine.DecideResponse{}, fmt.Errorf("boom: transport error")
	}
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return engine.DecideResponse{}, ctx.Err()
		}
	}
	conf := d.conf
	if c, ok := d.confByModel[req.Model]; ok {
		conf = c
	}
	ans := engine.Answer{Type: "choice", Choice: d.choice}
	if conf > 0 {
		ans.Probabilities = map[string]float64{d.choice: conf}
	}
	// Answer q1 (the dataset questions all use q1) with the fixed choice.
	return engine.DecideResponse{Answers: map[string]engine.Answer{"q1": ans}}, nil
}

var _ engine.Decider = (*deciderFakeEngine)(nil)

// nonDeciderEngine is an engine.Engine WITHOUT Decider (for the unsupported case).
type nonDeciderEngine struct{ *fakeEngine }

var _ = Describe("Eval-gated rollout", func() {
	const model = "laya:en"

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	// pollReconcile runs a reconcile tolerating transient errors (e.g. status
	// patch conflicts) and returns the current phase — for use inside Eventually.
	pollReconcile := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		dm := &decisionmodelv1alpha1.DecisionModel{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm); err != nil {
			return ""
		}
		return dm.Status.Phase
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
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
		pod.Status.PodIP = "10.0.0.20"
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
			Namespace: namespace, Name: dmName + "-prefetch-" + rev,
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

	// datasetCM creates a ConfigMap with the given JSONL under key cases.jsonl.
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
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	createEvalDM := func(name, cmName string, mutate func(*decisionmodelv1alpha1.EvaluationSpec)) {
		evalSpec := &decisionmodelv1alpha1.EvaluationSpec{
			DatasetRef: decisionmodelv1alpha1.DatasetRef{
				ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: cmName, Key: "cases.jsonl"},
			},
			MinAccuracy: "0.90",
		}
		if mutate != nil {
			mutate(evalSpec)
		}
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine:   "ollaya",
				Model:    model,
				Device:   "cpu",
				Replicas: int32Ptr(1),
				Rollout:  &decisionmodelv1alpha1.RolloutSpec{Evaluation: evalSpec},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

	// driveToEvaluating brings a DM with one ready candidate Pod up to the point
	// where evaluation runs, returning the revision hash. The eval-triggering
	// reconcile may fail permanently (e.g. unsupported engine / invalid dataset),
	// which is a valid terminal outcome; that error is tolerated here and the
	// caller asserts the resulting Failed/RolledBack status.
	driveToEvaluating := func(r *DecisionModelReconciler, name string) string {
		reconcileOnce(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, rev)
		reconcileOnce(r, name)
		createReadyPod(name, rev, name+"-pod-0")
		_, _ = r.Reconcile(ctx, reconcile.Request{ // probe -> ready -> starts eval (may terminal-fail)
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		return rev
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

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmtEvalNS(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("promotes when candidate accuracy passes", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-pass", allChoice(10, 10)) // 100% match
		createEvalDM("ev1", "golden-pass", nil)

		rev := driveToEvaluating(r, "ev1")

		// Async eval; requeue until it completes and promotes.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "ev1")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		dm := getDM("ev1")
		Expect(dm.Status.StableRevision).NotTo(BeNil())
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev))
		Expect(dm.Status.Evaluation).NotTo(BeNil())
		Expect(dm.Status.Evaluation.Accuracy).To(Equal("1.0000"))
		Expect(meta_IsTrue(dm, decisionmodelv1alpha1.ConditionEvaluated)).To(BeTrue())

		// EvaluationPassed and Promoted each fire exactly once.
		fr := r.Recorder.(*events.FakeRecorder)
		var passed, promoted int
		for drained := false; !drained; {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, eventEvaluationPassed) {
					passed++
				}
				if strings.Contains(e, eventPromoted) {
					promoted++
				}
			default:
				drained = true
			}
		}
		Expect(passed).To(Equal(1), "EvaluationPassed must fire once")
		Expect(promoted).To(Equal(1), "Promoted must fire once")
	})

	It("rolls back when accuracy is below minAccuracy", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-fail", allChoice(10, 5)) // 50% < 90%
		createEvalDM("ev2", "golden-fail", nil)

		driveToEvaluating(r, "ev2")

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "ev2")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed))

		dm := getDM("ev2")
		Expect(dm.Status.StableRevision).To(BeNil())
		Expect(dm.Status.FailedRevision).NotTo(BeNil())
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonEvaluationFailed))
	})

	It("fails when the engine does not support evaluation", func() {
		eng := &nonDeciderEngine{fakeEngine: newFakeEngine()}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-uns", allChoice(3, 3))
		createEvalDM("ev3", "golden-uns", nil)

		driveToEvaluating(r, "ev3")

		dm := getDM("ev3")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonEvaluationUnsupported))
	})

	It("rolls back on an invalid dataset", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-bad", "this is not json\n")
		createEvalDM("ev4", "golden-bad", nil)

		driveToEvaluating(r, "ev4")

		dm := getDM("ev4")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonDatasetInvalid))
	})

	It("times out a slow evaluation", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", delay: time.Hour}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		// Injected clock: the eval deadline is measured from now(); advance it so
		// runEvaluation returns timedOut on its first case check. A safeClock is
		// used because the async eval goroutine reads r.Now concurrently.
		clock := newSafeClock()
		r.Now = clock.now
		datasetCM("golden-slow", allChoice(3, 3))
		createEvalDM("ev5", "golden-slow", nil)

		rev := driveToEvaluating(r, "ev5")
		_ = rev

		// Advance the clock beyond the eval deadline; the goroutine's first
		// now().After(deadline) check trips and records timedOut.
		clock.add(evalDeadline + time.Minute)

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "ev5")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed))

		deg := meta_Find(getDM("ev5"), decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonEvaluationTimeout))
	})

	It("cancels a running evaluation when the revision changes", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", delay: 200 * time.Millisecond}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-cancel", allChoice(50, 50))
		createEvalDM("ev6", "golden-cancel", nil)

		rev1 := driveToEvaluating(r, "ev6")
		reconcileOnce(r, "ev6") // eval running for rev1

		// Change the model -> new revision; the rev1 eval must be cancelled.
		eng.digest = "eee5550000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "ev6", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcileOnce(r, "ev6")

		store := r.evalStoreOrInit()
		_, ok := store.get(evalKey{namespace, "ev6", rev1, datasetHashOf(allChoice(50, 50)), defaultMaxCases})
		Expect(ok).To(BeFalse(), "rev1 evaluation entry should be forgotten after rev change")
	})

	It("promotes without evaluation when rollout.evaluation is unset", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "ev7"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())

		rev := driveToEvaluating(r, "ev7")
		reconcileOnce(r, "ev7") // promote immediately

		got := getDM("ev7")
		Expect(got.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(got.Status.StableRevision.Hash).To(Equal(rev))
		Expect(got.Status.Evaluation).To(BeNil())
	})

	// well-calibrated answers (correct, high confidence) pass maxECE.
	It("passes when answers are well-calibrated", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", conf: 0.99}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-cal", allChoice(10, 10)) // all expect "billing" -> all correct
		createEvalDM("cal1", "golden-cal", func(s *decisionmodelv1alpha1.EvaluationSpec) {
			s.MaxECE = "0.10"
		})

		driveToEvaluating(r, "cal1")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "cal1")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		ev := getDM("cal1").Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.Accuracy).To(Equal("1.0000"))
		Expect(ev.ECE).NotTo(BeEmpty())
	})

	// accuracy passes but mis-calibration (under-confident) fails maxECE.
	It("fails maxECE when accuracy passes but calibration is poor", func() {
		// All answers correct (accuracy 1.0) but confidence 0.5 -> ECE ~0.5.
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", conf: 0.5}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-mis", allChoice(10, 10))
		createEvalDM("cal2", "golden-mis", func(s *decisionmodelv1alpha1.EvaluationSpec) {
			s.MaxECE = "0.10"
		})

		driveToEvaluating(r, "cal2")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "cal2")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed))

		dm := getDM("cal2")
		Expect(dm.Status.Evaluation.Accuracy).To(Equal("1.0000"))
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonEvaluationFailed))
		Expect(deg.Message).To(ContainSubstring("maxECE"))
	})

	// an ECE increase vs the stable baseline fails maxECEIncrease.
	It("fails maxECEIncrease when calibration regresses vs baseline", func() {
		// The stable model (laya:en) is well-calibrated (conf ~1.0, low ECE); the
		// candidate model (kev:en) is poorly calibrated (conf 0.5, high ECE).
		// Confidence is keyed by model so the baseline and candidate confidences
		// are fixed up front — neither is read through a field mutated
		// concurrently with the async eval goroutine (the former cal3 race).
		eng := &deciderFakeEngine{
			fakeEngine: newFakeEngine(),
			choice:     "billing",
			confByModel: map[string]float64{
				"laya:en": 0.99,
				"kev:en":  0.5,
			},
		}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		datasetCM("golden-inc", allChoice(10, 10))
		createEvalDM("cal3", "golden-inc", func(s *decisionmodelv1alpha1.EvaluationSpec) {
			s.MaxECEIncrease = "0.05"
		})

		driveToEvaluating(r, "cal3")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "cal3")
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		// Wait until the first revision is actually recorded as stable before
		// driving the candidate, so the baseline is evaluated against a settled
		// StableRevision (no race between promotion and the candidate).
		var stableRev string
		Eventually(func() string {
			s := getDM("cal3").Status.StableRevision
			if s == nil {
				return ""
			}
			stableRev = s.Hash
			return s.Hash
		}, "2s", "50ms").ShouldNot(BeEmpty())

		// New revision, same accuracy but much worse calibration (conf 0.5). The
		// digest change is applied before the resolving reconcile (synchronous),
		// so it is not read by any goroutine.
		eng.digest = "ca10000000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "cal3", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		r.Prober = &fakeProber{loaded: engine.Loaded{Name: "kev:en", Digest: eng.digest, Device: "cpu"}}

		// Drive the candidate deterministically to the point where evaluation
		// runs: resolve+cache (reconcile) -> mark the prefetch Job complete ONCE
		// -> reconcile -> create the ready candidate Pod ONCE. Re-marking the Job
		// on every poll iteration (the former pattern) raced the controller's GC
		// of the Job after rollback and intermittently failed the status Update.
		reconcileOnce(r, "cal3")
		candRev := getDM("cal3").Status.CandidateRevision.Hash
		markJobComplete("cal3", candRev)
		reconcileOnce(r, "cal3")
		createReadyPod("cal3", candRev, "cal3-cand")

		// Now only poll reconciles; the async candidate eval and stable baseline
		// complete and the gate (ECE increase vs baseline) rolls the candidate back.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			return pollReconcile(r, "cal3")
		}, "20s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		dm := getDM("cal3")
		Expect(dm.Status.StableRevision.Hash).To(Equal(stableRev), "stable kept after calibration regression")
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Message).To(ContainSubstring("maxECEIncrease"))
	})
})

// small helpers to avoid importing meta in this file's assertions.
func meta_IsTrue(dm *decisionmodelv1alpha1.DecisionModel, condType string) bool {
	c := meta_Find(dm, condType)
	return c != nil && c.Status == metav1.ConditionTrue
}

func meta_Find(dm *decisionmodelv1alpha1.DecisionModel, condType string) *metav1.Condition {
	for i := range dm.Status.Conditions {
		if dm.Status.Conditions[i].Type == condType {
			return &dm.Status.Conditions[i]
		}
	}
	return nil
}

func datasetHashOf(s string) string { return datasetHash([]byte(s)) }

func fmtEvalNS(n int) string { return "eval-test-" + itoa(n) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

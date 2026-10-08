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

// manual promotion gate (spec.rollout.manualPromotion).
var _ = Describe("manual promotion", func() {
	const (
		datasetName = "golden-manual"
		digest2     = "b2b2250000000000000000000000000000000000000000000000000000000000"
		digest3     = "c3c3250000000000000000000000000000000000000000000000000000000000"
	)

	var (
		ctx       context.Context
		namespace string
		nsCounter int
		clock     time.Time
	)

	int32Ptr := func(v int32) *int32 { return &v }
	promoteKey := decisionmodelv1alpha1.AnnotationPromote

	// mp bundles one DecisionModel under test with its reconciler and engine.
	type mp struct {
		name      string
		r         *DecisionModelReconciler
		setDigest func(string)
		rev1      string // first (stable) revision
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	// reconcile runs one reconcile tolerating transient errors (status conflicts
	// requeue) and returns the resulting phase, for use in Eventually.
	reconcile1 := func(m *mp) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = m.r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: m.name}})
		return getDM(m.name).Status.Phase
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

	createGatedPod := func(dmName, rev string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: dmName + "-pod-" + rev,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.50"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	// countEvents drains the reconciler's recorder and counts events containing
	// each substring.
	countEvents := func(m *mp, substrs ...string) map[string]int {
		fr := m.r.Recorder.(*events.FakeRecorder)
		out := map[string]int{}
		for _, s := range substrs {
			out[s] = 0
		}
		for {
			select {
			case e := <-fr.Events:
				for _, s := range substrs {
					if strings.Contains(e, s) {
						out[s]++
					}
				}
			default:
				return out
			}
		}
	}

	condition := func(name, condType string) *metav1.Condition {
		return meta_Find(getDM(name), condType)
	}

	serviceRevision := func(name string) string {
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}

	annotate := func(name, value string) {
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			if dm.Annotations == nil {
				dm.Annotations = map[string]string{}
			}
			dm.Annotations[promoteKey] = value
		})).To(Succeed())
	}

	// newManual creates a manualPromotion DM (optionally with evaluation) and
	// drives its FIRST revision to Ready. The first revision must not be held.
	newManual := func(name string, withEval bool, manual bool) *mp {
		fake := newFakeEngine()
		var eng engine.Engine = fake
		if withEval {
			eng = &deciderFakeEngine{fakeEngine: fake, choice: "billing"}
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: datasetName},
				Data:       map[string]string{"cases.jsonl": strings.Repeat(`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}`+"\n", 10)},
			}
			Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		}
		clock = time.Now()
		m := &mp{
			name: name,
			r: &DecisionModelReconciler{
				Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
				Engines:  map[string]engine.Engine{"ollaya": eng},
				Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
				Recorder: events.NewFakeRecorder(256),
				Now:      func() time.Time { return clock },
			},
			setDigest: func(d string) { fake.mu.Lock(); fake.digest = d; fake.mu.Unlock() },
		}
		rollout := &decisionmodelv1alpha1.RolloutSpec{}
		if manual {
			rollout.Promotion = decisionmodelv1alpha1.PromotionManual
		}
		if withEval {
			rollout.Evaluation = &decisionmodelv1alpha1.EvaluationSpec{
				DatasetRef: decisionmodelv1alpha1.DatasetRef{
					ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: datasetName, Key: "cases.jsonl"},
				},
				MinAccuracy: "0.90",
			}
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1), Rollout: rollout,
			},
		})).To(Succeed())

		reconcile1(m)
		m.rev1 = RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, m.rev1)
		reconcile1(m)
		createGatedPod(name, m.rev1)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return reconcile1(m) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady), "the first revision is never held for approval")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1))
		return m
	}

	// startCandidate switches the model, which creates a new candidate revision,
	// and drives it to the point where its gate is evaluated. It returns the
	// candidate hash. It does not assert the resulting phase.
	startCandidate := func(m *mp, model, digest string) string {
		m.setDigest(digest)
		Expect(updateDM(ctx, namespace, m.name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = model
		})).To(Succeed())
		reconcile1(m)
		cand := getDM(m.name).Status.CandidateRevision
		Expect(cand).NotTo(BeNil())
		markJobComplete(m.name, cand.Hash)
		reconcile1(m)
		createGatedPod(m.name, cand.Hash)
		return cand.Hash
	}

	awaiting := func(m *mp) func() decisionmodelv1alpha1.DecisionModelPhase {
		return func() decisionmodelv1alpha1.DecisionModelPhase { return reconcile1(m) }
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("manualpromo-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	DescribeTable("holds a passing candidate until the matching hash is approved",
		func(withEval bool) {
			name := "mp-hold"
			m := newManual(name, withEval, true)
			cand := startCandidate(m, "kev:en", digest2)
			Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))

			// Waiting: stable keeps serving, Promoted=False/AwaitingApproval.
			dm := getDM(name)
			Expect(dm.Status.StableRevision.Hash).To(Equal(m.rev1))
			Expect(dm.Status.CandidateRevision.Hash).To(Equal(cand))
			Expect(serviceRevision(name)).To(Equal(m.rev1), "traffic stays on the stable revision")
			promoted := condition(name, decisionmodelv1alpha1.ConditionPromoted)
			Expect(promoted).NotTo(BeNil())
			Expect(promoted.Status).To(Equal(metav1.ConditionFalse))
			Expect(promoted.Reason).To(Equal("PromotionPending"))
			Expect(promoted.Message).To(ContainSubstring(cand))
			if withEval {
				Expect(meta_IsTrue(dm, decisionmodelv1alpha1.ConditionEvaluated)).To(BeTrue())
				Expect(dm.Status.Evaluation).NotTo(BeNil())
			}

			// Re-reconciling while waiting is stable: same phase, one Event only.
			for i := 0; i < 5; i++ {
				Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
			}
			ev := countEvents(m, "AwaitingPromotion", "Promoted ")
			Expect(ev["AwaitingPromotion"]).To(Equal(1), "announced once, not on every reconcile")
			Expect(ev["Promoted "]).To(Equal(1), "only the first revision was promoted so far")

			// No timeout fires while a human decides (days, far beyond every
			// progress timeout).
			clock = clock.Add(72 * time.Hour)
			for i := 0; i < 3; i++ {
				Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
			}
			Expect(getDM(name).Status.FailedRevision).To(BeNil())

			// A wrong hash (stale approval for some other revision) is ignored.
			annotate(name, m.rev1)
			Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
			Expect(serviceRevision(name)).To(Equal(m.rev1))

			// Approve: with evaluation the bare hash is NOT honoured, only the
			// approvalID is; without evaluation the bare hash still works.
			token := cand
			if withEval {
				token = getDM(name).Status.Evaluation.ApprovalID
				Expect(token).NotTo(BeEmpty())
			}
			annotate(name, token)
			Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
			dm = getDM(name)
			Expect(dm.Status.StableRevision.Hash).To(Equal(cand))
			Expect(dm.Status.CandidateRevision).To(BeNil())
			Expect(serviceRevision(name)).To(Equal(cand))
			promoted = condition(name, decisionmodelv1alpha1.ConditionPromoted)
			Expect(promoted).NotTo(BeNil())
			Expect(promoted.Status).To(Equal(metav1.ConditionTrue))
			ev = countEvents(m, "PromotionApproved", "Promoted ")
			Expect(ev["PromotionApproved"]).To(Equal(1))
			Expect(ev["Promoted "]).To(Equal(1), "the normal Promoted event fires for the approved promotion")

			// The approval is NOT consumed by the reconcile that promotes: it is
			// removed only once the promotion is the persisted stable revision.
			Expect(getDM(name).Annotations).To(HaveKeyWithValue(promoteKey, token))
			Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseReady))
			Expect(getDM(name).Annotations).NotTo(HaveKey(promoteKey))
		},
		Entry("without evaluation", false),
		Entry("with evaluation", true),
	)

	It("mentions the evaluation result in the AwaitingPromotion Event", func() {
		m := newManual("mp-ev", true, true)
		startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		fr := m.r.Recorder.(*events.FakeRecorder)
		var found string
		for len(fr.Events) > 0 {
			if e := <-fr.Events; strings.Contains(e, "AwaitingPromotion") {
				found = e
			}
		}
		Expect(found).To(ContainSubstring("accuracy 1.0000"))
		Expect(found).To(ContainSubstring(promoteKey))
	})

	It("replaces the waiting candidate when the spec changes, and a stale approval does not promote the new one", func() {
		name := "mp-replace"
		m := newManual(name, false, true)
		cand2 := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))

		// Change the spec while waiting: a third model becomes the candidate.
		cand3 := startCandidate(m, "jevk5:en", digest3)
		Expect(cand3).NotTo(Equal(cand2))

		// An approval for the OLD candidate arrives late: it must not promote the new one.
		annotate(name, cand2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		dm := getDM(name)
		Expect(dm.Status.CandidateRevision.Hash).To(Equal(cand3))
		Expect(dm.Status.StableRevision.Hash).To(Equal(m.rev1), "stale approval must not promote")

		// The replaced candidate's Deployment is garbage-collected.
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + cand2}, &appsv1.Deployment{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "old candidate Deployment is GC'd, got %v", err)

		// Approving the new candidate promotes it.
		annotate(name, cand3)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand3))
	})

	It("drops the pending-approval condition when the parked candidate stops being model-ready", func() {
		name := "mp-unready"
		m := newManual(name, false, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))

		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: name + "-pod-" + cand}})).To(Succeed())
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseStarting))
		Expect(condition(name, decisionmodelv1alpha1.ConditionPromoted)).To(BeNil())
	})

	It("promotes without approval when the promotion policy is switched to Automatic while waiting", func() {
		name := "mp-off"
		m := newManual(name, false, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))

		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout.Promotion = decisionmodelv1alpha1.PromotionAutomatic
		})).To(Succeed())
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
	})

	It("treats the deprecated manualPromotion:true as promotion: Manual", func() {
		name := "mp-alias"
		fake := newFakeEngine()
		clock = time.Now()
		m := &mp{
			name: name,
			r: &DecisionModelReconciler{
				Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
				Engines:  map[string]engine.Engine{"ollaya": fake},
				Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
				Recorder: events.NewFakeRecorder(256),
				Now:      func() time.Time { return clock },
			},
			setDigest: func(d string) { fake.mu.Lock(); fake.digest = d; fake.mu.Unlock() },
		}
		//nolint:staticcheck // exercising the deprecated alias on purpose
		rollout := &decisionmodelv1alpha1.RolloutSpec{ManualPromotion: true}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1), Rollout: rollout,
			},
		})).To(Succeed())
		reconcile1(m)
		m.rev1 = RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, m.rev1)
		reconcile1(m)
		createGatedPod(name, m.rev1)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return reconcile1(m) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// A second revision must now be held for approval, exactly like promotion: Manual.
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		annotate(name, cand)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
	})

	It("keeps an annotation that does not match the stable revision", func() {
		name := "mp-keep"
		m := newManual(name, false, true)
		startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		// Only an approval equal to the persisted stable hash is consumed.
		annotate(name, "deadbeef00")
		Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		Expect(getDM(name).Annotations).To(HaveKeyWithValue(promoteKey, "deadbeef00"))
	})

	It("promotes immediately with promotion: Automatic and ignores the annotation", func() {
		name := "mp-auto"
		m := newManual(name, false, false)
		annotate(name, "whatever")
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
		// Promoted is True (reason Promoted) on the normal promotion; it is never
		// held for approval, so there is no AwaitingPromotion / PromotionPending.
		promoted := condition(name, decisionmodelv1alpha1.ConditionPromoted)
		Expect(promoted).NotTo(BeNil())
		Expect(promoted.Status).To(Equal(metav1.ConditionTrue))
		Expect(promoted.Reason).To(Equal("Promoted"))
		Expect(countEvents(m, "AwaitingPromotion")["AwaitingPromotion"]).To(BeZero())
	})

	// a policy change while parked re-evaluates instead of promoting on
	// the stale result; an approval set together with the policy change must not
	// promote on the old result.
	It("re-evaluates a parked candidate when the eval policy changes; approval does not promote on the stale result", func() {
		name := "mp-policy"
		m := newManual(name, true, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		recorded := getDM(name).Status.Evaluation
		Expect(recorded).NotTo(BeNil())
		Expect(recorded.PolicyHash).NotTo(BeEmpty())

		// Tighten the accuracy floor AND approve in the same edit. The approval
		// must not promote the candidate on the stale result; the policy change
		// forces re-evaluation first.
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout.Evaluation.MinAccuracy = "0.95"
			if dm.Annotations == nil {
				dm.Annotations = map[string]string{}
			}
			dm.Annotations[promoteKey] = cand
		})).To(Succeed())

		// The next reconcile leaves AwaitingPromotion to re-evaluate (not Ready).
		ph := reconcile1(m)
		Expect(ph).NotTo(Equal(decisionmodelv1alpha1.PhaseReady), "must not promote on the stale result")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1), "stable unchanged while re-evaluating")
		// The result was re-recorded under the new policy hash (the re-eval ran
		// before any promotion could be honoured on the stale result).
		Expect(getDM(name).Status.Evaluation.PolicyHash).NotTo(Equal(recorded.PolicyHash),
			"result re-recorded under the new policy hash before honouring the approval")
	})

	// with only an absolute gate (no maxAccuracyDrop / maxECEIncrease)
	// the stable baseline is never computed, so promotion is not delayed by it.
	It("does not compute or wait on a baseline when no relative gate is set", func() {
		name := "mp-nobaseline"
		m := newManual(name, true, false) // eval + auto-promote; absolute gate only
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
		ev := getDM(name).Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.BaselineAccuracy).To(BeEmpty(), "baseline not run without a relative gate")
	})

	// Promotion by approvalID (the current form); a wrong value is ignored.
	It("promotes on the approvalID and records it in status", func() {
		name := "mp-approvalid"
		m := newManual(name, true, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		id := getDM(name).Status.Evaluation.ApprovalID
		Expect(id).NotTo(BeEmpty())

		// A wrong value (neither approvalID nor the candidate hash) is ignored.
		annotate(name, "deadbeefdead")
		Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		Expect(serviceRevision(name)).To(Equal(m.rev1))

		// The approvalID promotes.
		annotate(name, id)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
		// The annotation is consumed once the promotion is durable.
		Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Annotations).NotTo(HaveKey(promoteKey))
	})

	// Changing ONLY scoreTolerance while a candidate is parked for approval must
	// re-evaluate under the new tolerance (it is part of the verdict): the policy
	// hash and approvalId rotate, the old approvalId no longer promotes, and the
	// re-scored accuracy actually changes (not just the hash). A score answer of
	// 2.6 against expected level 2 is correct within tolerance 0.75 but wrong at
	// the default 0.5.
	It("re-evaluates on a scoreTolerance change and the re-scored verdict differs", func() {
		name := "mp-scoretol"
		fake := newFakeEngine()
		score := 2.6
		eng := &deciderFakeEngine{fakeEngine: fake, score: &score}
		// A score dataset: one case expecting integer level 2.
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden-score"},
			Data:       map[string]string{"cases.jsonl": `{"state":{},"questions":{"q1":{"type":"score"}},"expected":{"q1":2}}` + "\n"},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		clock = time.Now()
		m := &mp{
			name: name,
			r: &DecisionModelReconciler{
				Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
				Engines:  map[string]engine.Engine{"ollaya": eng},
				Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
				Recorder: events.NewFakeRecorder(256),
				Now:      func() time.Time { return clock },
			},
			setDigest: func(d string) { fake.mu.Lock(); fake.digest = d; fake.mu.Unlock() },
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{
					Promotion: decisionmodelv1alpha1.PromotionManual,
					Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
						DatasetRef:     decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-score", Key: "cases.jsonl"}},
						MinAccuracy:    "0.90",
						ScoreTolerance: "0.75", // 2.6 vs 2 is correct here
					},
				},
			},
		})).To(Succeed())
		// Drive the first revision to Ready (never held).
		reconcile1(m)
		m.rev1 = RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, m.rev1)
		reconcile1(m)
		createGatedPod(name, m.rev1)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return reconcile1(m) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Candidate: passes at tolerance 0.75 and parks for approval (accuracy 1).
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		oldEv := getDM(name).Status.Evaluation
		Expect(oldEv).NotTo(BeNil())
		Expect(oldEv.Accuracy).To(Equal("1.0000"), "2.6 within tolerance 0.75 of level 2 is correct")
		oldID := oldEv.ApprovalID
		Expect(oldID).NotTo(BeEmpty())
		oldPolicy := oldEv.PolicyHash

		// Tighten the tolerance to the default AND approve on the old id in one
		// edit: the operator must re-evaluate under the new tolerance, not promote.
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout.Evaluation.ScoreTolerance = "0.5"
			if dm.Annotations == nil {
				dm.Annotations = map[string]string{}
			}
			dm.Annotations[promoteKey] = oldID
		})).To(Succeed())

		ph := reconcile1(m)
		Expect(ph).NotTo(Equal(decisionmodelv1alpha1.PhaseReady), "the stale approval must not promote under the new tolerance")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1), "stable unchanged")

		// The policy hash rotated and the re-scored verdict actually changed: at
		// tolerance 0.5, 2.6 vs 2 is wrong, so accuracy drops to 0 and the gate
		// fails -> the candidate is rejected (not promoted on the old result).
		Eventually(func() string {
			reconcile1(m)
			ev := getDM(name).Status.Evaluation
			if ev == nil {
				return ""
			}
			return ev.PolicyHash
		}, "10s", "50ms").ShouldNot(Equal(oldPolicy))
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return reconcile1(m) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack), "re-scored accuracy fails the gate under the stricter tolerance")
		Expect(getDM(name).Status.Evaluation.Accuracy).To(Equal("0.0000"), "re-scored accuracy changed with the tolerance")
		Expect(getDM(name).Status.FailedRevision).NotTo(BeNil())
		Expect(getDM(name).Status.FailedRevision.Hash).To(Equal(cand))
	})

	// An operator upgrade that changes the scorer (scorerVersion bump) must
	// re-evaluate a parked candidate: its recorded policyHash (produced by the old
	// scorer) no longer matches the current one, so the old approvalId does not
	// promote and a fresh evaluation runs. The upgrade is simulated by rewriting
	// the recorded policyHash to a value from a different scorer version (exactly
	// what a pre-upgrade result looks like after the bump).
	It("re-evaluates a parked candidate after a scorer-version change and ignores the stale approval", func() {
		name := "mp-scorer"
		m := newManual(name, true, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		recorded := getDM(name).Status.Evaluation
		Expect(recorded.ScorerVersion).To(Equal(int32(scorerVersion)), "status records the scorer version")
		oldID := recorded.ApprovalID
		Expect(oldID).NotTo(BeEmpty())

		// Simulate the pre-upgrade result: a policyHash from a different scorer
		// version. (scorerVersion is a build constant; a bump changes the hash the
		// same way this does.) Set the OLD approvalId in the same edit.
		stalePolicy := recorded.PolicyHash + "x"
		Expect(updateDMStatus(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.Evaluation.PolicyHash = stalePolicy
		})).To(Succeed())
		annotate(name, oldID)

		ph := reconcile1(m)
		Expect(ph).NotTo(Equal(decisionmodelv1alpha1.PhaseReady), "the stale approval must not promote after a scorer change")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1))
		// It re-evaluates and records the current scorer version / a fresh policyHash.
		Eventually(func() string {
			reconcile1(m)
			ev := getDM(name).Status.Evaluation
			if ev == nil {
				return ""
			}
			return ev.PolicyHash
		}, "10s", "50ms").ShouldNot(Equal(stalePolicy))
		Expect(getDM(name).Status.Evaluation.ScorerVersion).To(Equal(int32(scorerVersion)))
		_ = cand
	})

	// Editing the dataset content while parked re-evaluates (DatasetChanged) and
	// rotates the approvalID; an approval for the OLD identity does not promote.
	It("re-evaluates on a dataset content change and rotates the approvalID", func() {
		name := "mp-dschange"
		m := newManual(name, true, true)
		startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		oldEv := getDM(name).Status.Evaluation
		Expect(oldEv.DatasetDigest).NotTo(BeEmpty())
		oldID := oldEv.ApprovalID

		// Edit the dataset content in place AND set the old approvalID in the same
		// edit: the operator must re-evaluate (not promote on the stale result).
		Expect(func() error {
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: datasetName}, cm); err != nil {
				return err
			}
			cm.Data["cases.jsonl"] = strings.Repeat(
				`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}`+"\n", 12)
			return k8sClient.Update(ctx, cm)
		}()).To(Succeed())
		annotate(name, oldID)

		ph := reconcile1(m)
		Expect(ph).NotTo(Equal(decisionmodelv1alpha1.PhaseReady), "must not promote on the stale dataset result")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1))

		// It re-parks under the new dataset digest with a different approvalID.
		Eventually(func() string {
			reconcile1(m)
			ev := getDM(name).Status.Evaluation
			if ev == nil {
				return ""
			}
			return ev.DatasetDigest
		}, "10s", "50ms").ShouldNot(Equal(oldEv.DatasetDigest))
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		newID := getDM(name).Status.Evaluation.ApprovalID
		Expect(newID).NotTo(Equal(oldID), "approvalID rotates when the dataset changes")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1), "still not promoted on the old approval")
	})

	// With evaluation configured, a bare revision hash never promotes: even with
	// the identity intact it is ignored with a DeprecatedApproval Warning; only
	// the approvalID promotes.
	It("ignores a bare-hash approval when evaluation is configured", func() {
		name := "mp-barehash"
		m := newManual(name, true, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		id := getDM(name).Status.Evaluation.ApprovalID
		Expect(id).NotTo(Equal(cand), "the approvalID is not the bare revision hash")

		// Bare hash: ignored, stays parked, a DeprecatedApproval Warning names the ID.
		annotate(name, cand)
		for i := 0; i < 3; i++ {
			Expect(reconcile1(m)).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		}
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(m.rev1), "bare hash must not promote with eval")
		fr := m.r.Recorder.(*events.FakeRecorder)
		var warned bool
		for len(fr.Events) > 0 {
			e := <-fr.Events
			if strings.Contains(e, "DeprecatedApproval") && strings.Contains(e, id) {
				warned = true
			}
		}
		Expect(warned).To(BeTrue(), "a DeprecatedApproval Warning naming the approvalID must be emitted")

		// The approvalID promotes.
		annotate(name, id)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
	})

	// Without evaluation the bare hash still works for one release (deprecated).
	It("still accepts a bare-hash approval without evaluation", func() {
		name := "mp-barehash-noeval"
		m := newManual(name, false, true)
		cand := startCandidate(m, "kev:en", digest2)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		annotate(name, cand)
		Eventually(awaiting(m), "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(cand))
	})
})

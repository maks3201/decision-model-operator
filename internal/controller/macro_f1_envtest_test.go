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

// Macro-F1 gates catch class imbalance that accuracy hides: the fake engine
// always answers "billing", so a dataset dominated by "billing" scores high
// accuracy but a low macro-F1 (the minority class has recall 0).
var _ = Describe("macro-F1 evaluation gate", func() {
	const model = "laya:en"
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": eng}, Prober: prober,
			Recorder: events.NewFakeRecorder(64),
		}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	createReadyPod := func(dmName, rev, podName, ip string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev}},
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
	markJob := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(dmName, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	// imbalanced builds a single-question ("q1", choice) dataset with `billing`
	// cases that the fake answers correctly and `other` cases it gets wrong.
	imbalanced := func(billing, other int) string {
		var b strings.Builder
		for i := 0; i < billing; i++ {
			b.WriteString(`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}` + "\n")
		}
		for i := 0; i < other; i++ {
			b.WriteString(`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"other"}}` + "\n")
		}
		return b.String()
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "macrof1-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// 9 billing + 1 other: accuracy 0.90 passes minAccuracy 0.80, but macro-F1
	// (~0.47) is below minMacroF1 0.80, so the candidate is rolled back
	// (no stable yet -> Failed) with EvaluationFailed naming macroF1.
	It("fails a candidate that passes accuracy but fails the macro-F1 floor", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden"},
			Data:       map[string]string{"cases.jsonl": imbalanced(9, 1)},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "imb"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
					MinAccuracy: "0.80",
					MinMacroF1:  "0.80",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, "imb")
		rev := RevisionHash(getDM("imb").Spec, defaultDigest, fakeImage)
		markJob("imb", rev)
		rec(r, "imb")
		createReadyPod("imb", rev, "imb-pod-0", "10.0.0.31")

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "imb")
			return getDM("imb").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed), "the macro-F1 floor must block promotion")

		ev := getDM("imb").Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.Result).To(Equal(decisionmodelv1alpha1.EvaluationFailed))
		Expect(ev.Reason).To(ContainSubstring("minMacroF1"))
		// accuracy was recorded and did pass its own floor.
		Expect(ev.Accuracy).To(Equal("0.9000"))
		Expect(ev.MacroF1).NotTo(BeEmpty())
		Expect(ev.MinMacroF1).To(Equal("0.80"))
		// status carries the per-question breakdown for the one classifiable question.
		Expect(ev.Questions).To(HaveLen(1))
		Expect(ev.Questions[0].ID).To(Equal("q1"))
		Expect(ev.ClassifiableCases).To(Equal(int32(10)))
		cond := meta_Find(getDM("imb"), decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(reasonEvaluationFailed))
	})

	// A dataset of only score questions has no choice/bool class to average: a
	// configured macro-F1 gate must fail closed (ClassificationUnavailable), never
	// pass silently.
	It("fails closed when a macro-F1 gate is set but the dataset has no classifiable question", func() {
		one := float64(1)
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), score: &one}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden-score"},
			Data:       map[string]string{"cases.jsonl": `{"state":{},"questions":{"q1":{"type":"score"}},"expected":{"q1":1}}` + "\n"},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "scoreonly"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-score", Key: "cases.jsonl"}},
					MinAccuracy: "0.50",
					MinMacroF1:  "0.80",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, "scoreonly")
		rev := RevisionHash(getDM("scoreonly").Spec, defaultDigest, fakeImage)
		markJob("scoreonly", rev)
		rec(r, "scoreonly")
		createReadyPod("scoreonly", rev, "scoreonly-pod-0", "10.0.0.32")

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "scoreonly")
			return getDM("scoreonly").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed), "a macro-F1 gate with no class must fail closed")

		ev := getDM("scoreonly").Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.Result).To(Equal(decisionmodelv1alpha1.EvaluationFailed))
		Expect(ev.ClassifiableCases).To(Equal(int32(0)))
		Expect(ev.MacroF1).To(BeEmpty(), "macroF1 is not reported when there is no classifiable question")
		cond := meta_Find(getDM("scoreonly"), decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(reasonClassificationUnavailable))
	})

	// The review blocker: the fake only ever answers q1 ("billing"); a dataset
	// that also expects a minority-class question q2 the model never answers must
	// NOT score macro-F1 1.0 — the missing q2 answers are misses, dragging macro-F1
	// below the floor.
	It("fails when the candidate skips a minority-class question (missing answers are misses)", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		// Every case answers q1 ("billing") correctly and also expects q2 ("refund"),
		// which the fake never returns -> missing -> a miss for class "refund".
		line := `{"state":{},"questions":{"q1":{"type":"choice"},"q2":{"type":"choice"}},"expected":{"q1":"billing","q2":"refund"}}` + "\n"
		data := ""
		for i := 0; i < 10; i++ {
			data += line
		}
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden-skip"},
			Data:       map[string]string{"cases.jsonl": data},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "skip"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-skip", Key: "cases.jsonl"}},
					MinAccuracy: "0.40", // accuracy is 0.5 (q1 right, q2 wrong) -> passes
					MinMacroF1:  "0.80",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, "skip")
		rev := RevisionHash(getDM("skip").Spec, defaultDigest, fakeImage)
		markJob("skip", rev)
		rec(r, "skip")
		createReadyPod("skip", rev, "skip-pod-0", "10.0.0.34")

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "skip")
			return getDM("skip").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseFailed), "a skipped class must fail the macro-F1 floor")

		ev := getDM("skip").Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.Result).To(Equal(decisionmodelv1alpha1.EvaluationFailed))
		Expect(ev.Reason).To(ContainSubstring("minMacroF1"))
		// q2 is a classifiable question too, so it appears with a low macro-F1.
		ids := map[string]bool{}
		for _, q := range ev.Questions {
			ids[q.ID] = true
		}
		Expect(ids["q2"]).To(BeTrue(), "the skipped minority question is recorded")
		Expect(ev.ClassifiableCases).To(Equal(int32(20)), "both q1 and q2 cases feed macro-F1 (10 each)")
	})

	// A balanced dataset the fake answers perfectly promotes, and status records
	// a macro-F1 of 1 with the per-question entry.
	It("promotes and records macro-F1 when the candidate passes", func() {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}})
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden-pass"},
			Data:       map[string]string{"cases.jsonl": imbalanced(5, 0)},
		})).To(Succeed())
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pass"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-pass", Key: "cases.jsonl"}},
					MinAccuracy: "0.90",
					MinMacroF1:  "0.90",
				}},
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, "pass")
		rev := RevisionHash(getDM("pass").Spec, defaultDigest, fakeImage)
		markJob("pass", rev)
		rec(r, "pass")
		createReadyPod("pass", rev, "pass-pod-0", "10.0.0.33")

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, "pass")
			return getDM("pass").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		ev := getDM("pass").Status.Evaluation
		Expect(ev).NotTo(BeNil())
		Expect(ev.Result).To(Equal(decisionmodelv1alpha1.EvaluationPassed))
		Expect(ev.MacroF1).To(Equal("1.0000"))
		Expect(ev.Questions).To(HaveLen(1))
		Expect(ev.Questions[0].MacroF1).To(Equal("1.0000"))
		Expect(ev.Truncated).To(BeFalse())
	})
})

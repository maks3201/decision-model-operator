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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// This suite covers a security seam the no-stable guards (security_envtest_test.go
// E2/E3) do not: a candidate that was ADMITTED under a permissive flag while a
// stable is already serving, and the operator then RESTARTS with the flag flipped
// (allow-image-override off, or the registry allow-list tightened). The guards run
// at the top of every reconcile (reconcilePreflight -> preflightSecurity,
// decisionmodel_controller.go:1010), so the first reconcile under the stricter
// flag must refuse the in-flight candidate WITHOUT touching the running stable:
// the stable is rendered from status.stableRevision (reconcileStablePath via
// maintainStableAfterPreflight, decisionmodel_controller.go:1294), so its Pod
// template is byte-identical and its Deployment is not rolled, the candidate is
// abandoned (candidateRevision cleared), the refusal is surfaced on Resolved
// (not a hard Failed, because a stable is serving), and a CandidateRejected
// Warning is emitted. Nothing is promoted.
var _ = Describe("a security flag tightened mid-rollout refuses the candidate and keeps the stable", func() {
	const baseModel = "laya:en"
	// pinnedImage is a digest-pinned runtime image, so spec.image is admitted on
	// the image-override flag alone (the unpinned-runtime-image gate never fires).
	const pinnedImage = "ghcr.io/me/ollaya@sha256:" +
		"1111111111111111111111111111111111111111111111111111111111111111"

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
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	resolvedCond := func(name string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionResolved)
	}
	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	// createReadyPod makes a gated, model-ready Pod for a revision so its
	// Deployment satisfies the model-ready gate on the next reconcile.
	createReadyPod := func(dmName, rev, podName, ip string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
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
	podGateTrue := func(podName string) bool {
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, p)).To(Succeed())
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}
	// drainEvents reports whether any buffered Event reason contains sub.
	drainEvents := func(r *DecisionModelReconciler, sub string) bool {
		fr := r.Recorder.(*events.FakeRecorder)
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, sub) {
					return true
				}
			default:
				return false
			}
		}
	}

	// newReconciler builds a reconciler with the fake decider engine and the given
	// flags. AllowUnpinnedRuntimeImages is always true so a digest-pinned
	// spec.image is governed only by AllowImageOverride (the pinning gate, which is
	// a separate seam, never interferes).
	newReconciler := func(allowImageOverride bool, allowedRegistries []string) *DecisionModelReconciler {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		return &DecisionModelReconciler{
			Client:                     k8sClient,
			APIReader:                  k8sClient,
			Scheme:                     k8sClient.Scheme(),
			Engines:                    map[string]engine.Engine{"ollaya": eng},
			Prober:                     &fakeProber{loaded: engine.Loaded{Name: baseModel, Digest: defaultDigest, Device: "cpu"}},
			Recorder:                   events.NewFakeRecorder(128),
			AllowImageOverride:         allowImageOverride,
			AllowUnpinnedRuntimeImages: true,
			AllowedRegistries:          allowedRegistries,
		}
	}

	// goldenCases is a trivial 1-choice dataset the fake decider ("billing")
	// answers correctly, so evaluation passes.
	goldenCM := func(name string) {
		Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Data:       map[string]string{"cases.jsonl": `{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}` + "\n"},
		})).To(Succeed())
	}

	// driveReady brings a single-replica DM to Ready and returns the stable rev.
	driveReady := func(r *DecisionModelReconciler, name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) string {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: baseModel, Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		rec(r, name)
		rev := getDM(name).Status.CandidateRevision.Hash
		markPrefetchComplete(ctx, namespace, name, rev)
		rec(r, name)
		createReadyPod(name, rev, name+"-stable-0", "10.0.1.10")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return getDM(name).Status.Phase
		}, "5s", "25ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		return getDM(name).Status.StableRevision.Hash
	}

	// driveCandidateToEvaluated brings an in-flight candidate (identified by
	// candRev) up to the point where evaluation has passed, so the next reconcile
	// would promote it. It returns once the candidate Pod exists and evaluation
	// has recorded a pass (or the candidate reached Promoting/Ready-pending).
	driveCandidateToEvaluated := func(r *DecisionModelReconciler, name, candRev, ip string) {
		markPrefetchComplete(ctx, namespace, name, candRev)
		rec(r, name)
		createReadyPod(name, candRev, name+"-cand-0", ip)
		// Reconcile until evaluation has passed (Evaluation.Result == Passed) or the
		// candidate is already being promoted. We stop BEFORE a successful promote by
		// flipping the flag in the caller right after this returns.
		Eventually(func() bool {
			rec(r, name)
			dm := getDM(name)
			if ev := dm.Status.Evaluation; ev != nil && ev.Result == decisionmodelv1alpha1.EvaluationPassed {
				return true
			}
			return dm.Status.CandidateRevision != nil &&
				meta.IsStatusConditionTrue(dm.Status.Conditions, decisionmodelv1alpha1.ConditionEvaluated)
		}, "5s", "25ms").Should(BeTrue(), "candidate must reach an evaluated-pass state before the flag flip")
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("flag-flip-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Item 1: an image-override candidate admitted under --allow-image-override,
	// then the operator restarts with the flag off between evaluation pass and
	// promotion. The candidate is refused, the stable keeps serving unchanged,
	// nothing is promoted.
	It("refuses an image-override candidate when --allow-image-override is turned off mid-rollout", func() {
		name := "img"
		goldenCM("golden-img")
		evalMutate := func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
				Promotion: decisionmodelv1alpha1.PromotionAutomatic,
				Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-img", Key: "cases.jsonl"}},
					MinAccuracy: "0.50",
				},
			}
		}

		By("a stable is Ready under the permissive flag (no image override yet)")
		rPermissive := newReconciler(true, nil)
		stableRev := driveReady(rPermissive, name, evalMutate)
		depBefore := stableDep(name, stableRev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		By("a candidate adds spec.image and is admitted under --allow-image-override")
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Image = pinnedImage
		})).To(Succeed())
		rec(rPermissive, name)
		candRev := getDM(name).Status.CandidateRevision.Hash
		Expect(candRev).NotTo(Equal(stableRev), "spec.image must produce a new revision")
		driveCandidateToEvaluated(rPermissive, name, candRev, "10.0.1.20")

		By("the operator restarts with --allow-image-override=false before promotion")
		rStrict := newReconciler(false, nil)
		rec(rStrict, name)

		By("the candidate is refused: Resolved=False/ImageOverrideNotAllowed, candidate abandoned")
		dm := getDM(name)
		Expect(dm.Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseFailed),
			"a serving stable means the object must not be failed by the candidate refusal")
		rc := resolvedCond(name)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonImageOverrideNotAllowed))
		Expect(dm.Status.CandidateRevision).To(BeNil(), "the refused candidate must be abandoned")
		Expect(drainEvents(rStrict, eventCandidateRejected)).To(BeTrue(), "a CandidateRejected Warning must fire")

		By("the stable Deployment is not rolled and its template is byte-identical")
		depAfter := stableDep(name, stableRev)
		Expect(depAfter.Generation).To(Equal(genBefore), "the stable must not roll on the candidate refusal")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable Pod template must be unchanged")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(stableRev), "the stable revision is unchanged")
		Expect(podGateTrue(name+"-stable-0")).To(BeTrue(), "the serving stable Pod stays model-ready")
	})

	// Item 2: the registry allow-list is tightened so the candidate's model host
	// is no longer allowed. The candidate is refused with RegistryNotAllowed, the
	// stable keeps serving, nothing is promoted.
	It("refuses a candidate whose model host leaves the allow-list mid-rollout", func() {
		name := "reg"
		const candModel = "custom.registry.io/ns/laya:en"
		const candHost = "custom.registry.io"
		goldenCM("golden-reg")
		evalMutate := func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
				Promotion: decisionmodelv1alpha1.PromotionAutomatic,
				Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
					DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden-reg", Key: "cases.jsonl"}},
					MinAccuracy: "0.50",
				},
			}
		}

		By("a stable is Ready; both the default and the candidate host are allowed")
		rPermissive := newReconciler(false, []string{defaultAllowedRegistry, candHost})
		stableRev := driveReady(rPermissive, name, evalMutate)
		depBefore := stableDep(name, stableRev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		By("a candidate switches to a model on the still-allowed custom host")
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = candModel
		})).To(Succeed())
		rec(rPermissive, name)
		candRev := getDM(name).Status.CandidateRevision.Hash
		Expect(candRev).NotTo(Equal(stableRev), "the host change must produce a new revision")
		driveCandidateToEvaluated(rPermissive, name, candRev, "10.0.1.30")

		By("the allow-list is tightened to drop the candidate host before promotion")
		rStrict := newReconciler(false, []string{defaultAllowedRegistry})
		rec(rStrict, name)

		By("the candidate is refused: Resolved=False/RegistryNotAllowed, candidate abandoned")
		dm := getDM(name)
		Expect(dm.Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseFailed))
		rc := resolvedCond(name)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonRegistryNotAllowed))
		Expect(dm.Status.CandidateRevision).To(BeNil(), "the refused candidate must be abandoned")
		Expect(drainEvents(rStrict, eventCandidateRejected)).To(BeTrue(), "a CandidateRejected Warning must fire")

		By("the stable Deployment is not rolled and its template is byte-identical")
		depAfter := stableDep(name, stableRev)
		Expect(depAfter.Generation).To(Equal(genBefore))
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(stableRev))
		Expect(podGateTrue(name + "-stable-0")).To(BeTrue())
	})

	// Item 3: the allow-list is tightened while a Ready stable has NO candidate.
	// The stable's own model host is still allowed (it was prefetched against it),
	// so a plain reconcile under the tighter list keeps it serving with no roll and
	// no Degraded. (If the stable's own host were dropped the guard would fire via
	// the spec==stable path; this spec covers the common "tighten to drop OTHER
	// hosts" operation, which must be a no-op for a running stable.)
	It("does not roll a Ready stable when the allow-list is tightened and no candidate exists", func() {
		name := "steady"
		By("a stable is Ready with the default host allow-listed alongside extras")
		rPermissive := newReconciler(false, []string{defaultAllowedRegistry, "extra.example.com"})
		stableRev := driveReady(rPermissive, name, nil)
		depBefore := stableDep(name, stableRev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()
		degradedBefore := meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)

		By("the allow-list is tightened to drop the extra host (the stable's host stays)")
		rStrict := newReconciler(false, []string{defaultAllowedRegistry})
		rec(rStrict, name)

		By("the stable keeps serving: no roll, no new revision, no Degraded flip")
		dm := getDM(name)
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady), "a Ready stable stays Ready")
		Expect(dm.Status.CandidateRevision).To(BeNil(), "no candidate is created")
		Expect(dm.Status.StableRevision.Hash).To(Equal(stableRev), "no new revision")
		depAfter := stableDep(name, stableRev)
		Expect(depAfter.Generation).To(Equal(genBefore), "the stable must not roll")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable template is byte-identical")
		Expect(podGateTrue(name + "-stable-0")).To(BeTrue())
		// No Degraded is introduced by the allow-list change (the stable's host is
		// still allowed, so preflightSecurity passes: decisionmodel_controller.go:1010).
		degradedAfter := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		if degradedBefore == nil {
			if degradedAfter != nil {
				Expect(degradedAfter.Status).To(Equal(metav1.ConditionFalse),
					"the allow-list change must not introduce a Degraded condition")
			}
		}
	})
})

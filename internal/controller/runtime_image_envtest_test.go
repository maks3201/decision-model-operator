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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// A candidate whose runtime image is a mutable tag breaks the
// one-revision-hash-one-bytes guarantee, so it is refused unless the operator
// runs with --allow-unpinned-runtime-images. These specs drive the no-stable
// candidate preflight (refuse / admit-with-flag), the spec.image tag-vs-digest
// rule, and prove a tag-image stable recorded by a previous build is maintained
// unchanged (not re-checked, not re-rolled) across an operator upgrade.
var _ = Describe("unpinned runtime image policy", func() {
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

	newR := func(allowUnpinned, allowImage bool) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                    map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                     &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder:                   events.NewFakeRecorder(256),
			AllowImageOverride:         allowImage,
			AllowUnpinnedRuntimeImages: allowUnpinned,
			WatchNamespaces:            []string{namespace},
		}
	}

	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}

	cond := func(name, t string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, t)
	}

	countEvents := func(r *DecisionModelReconciler) int {
		fr := r.Recorder.(*events.FakeRecorder)
		n := 0
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, "UnpinnedRuntimeImage") {
					n++
				}
			default:
				return n
			}
		}
	}

	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

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
	mkGatedPod := func(name, rev, pod, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("rtimg-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("admits a pinned default runtime image (no stable)", func() {
		r := newR(false, false)
		createDM("ok", nil)
		rec(r, "ok")
		// The default version is pinned, so a candidate starts normally.
		Expect(getDM("ok").Status.CandidateRevision).NotTo(BeNil())
		Expect(cond("ok", decisionmodelv1alpha1.ConditionResolved).Reason).NotTo(Equal(reasonUnpinnedRuntimeImage))
	})

	It("refuses an unpinned candidate (no stable) and creates no Job or PVC", func() {
		r := newR(false, false)
		createDM("deny", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = fakeUnpinnedRuntimeVersion // valid but no digest -> mutable tag
		})
		phase := rec(r, "deny")

		Expect(phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		rc := cond("deny", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonUnpinnedRuntimeImage))
		Expect(rc.Message).To(ContainSubstring("--allow-unpinned-runtime-images"))
		Expect(cond("deny", decisionmodelv1alpha1.ConditionReady).Reason).To(Equal(reasonUnpinnedRuntimeImage))
		Expect(getDM("deny").Status.CandidateRevision).To(BeNil(), "no candidate recorded")

		// Nothing was allocated: no store PVC, no prefetch Job for any revision.
		var pvcs corev1.PersistentVolumeClaimList
		Expect(k8sClient.List(ctx, &pvcs, client.InNamespace(namespace))).To(Succeed())
		Expect(pvcs.Items).To(BeEmpty(), "refusal before any allocation")
		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(namespace))).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "refusal before any allocation")

		// One Warning Event, and the reason is stable across reconciles (no spam).
		Expect(countEvents(r)).To(Equal(1))
		rec(r, "deny")
		Expect(countEvents(r)).To(Equal(0), "no repeat Event for the same refusal")
	})

	It("admits an unpinned candidate under the flag, records ImagePinned=false, warns once", func() {
		r := newR(true, false)
		createDM("allow", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = fakeUnpinnedRuntimeVersion
		})
		rec(r, "allow")

		cand := getDM("allow").Status.CandidateRevision
		Expect(cand).NotTo(BeNil(), "the candidate is admitted under the flag")
		Expect(cand.ImagePinned).NotTo(BeNil())
		Expect(*cand.ImagePinned).To(BeFalse(), "durable record that this revision is unpinned")
		// Resolved is True (the model resolved); the refusal reason is not set.
		Expect(cond("allow", decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue))

		Expect(countEvents(r)).To(Equal(1), "one Warning on admission")
		rec(r, "allow")
		Expect(countEvents(r)).To(Equal(0), "no repeat Warning per reconcile")
	})

	It("treats a spec.image tag as unpinned and an @sha256: image as pinned", func() {
		r := newR(false, true) // allow spec.image override, but not unpinned images

		createDM("img-tag", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Image = "ghcr.io/ollaya-dev/ollaya:0.10.0" // mutable tag
		})
		Expect(rec(r, "img-tag")).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		Expect(cond("img-tag", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonUnpinnedRuntimeImage))
		Expect(getDM("img-tag").Status.CandidateRevision).To(BeNil())

		createDM("img-dig", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Image = "ghcr.io/ollaya-dev/ollaya:0.10.0@sha256:" + strings.Repeat("a", 64)
		})
		rec(r, "img-dig")
		Expect(getDM("img-dig").Status.CandidateRevision).NotTo(BeNil(), "a digest-pinned image is admitted")
	})

	// A stable recorded by a previous build runs a tag-form image (every DM today
	// before this change). An operator upgrade that enforces pinning must NOT
	// re-check or re-render it, re-roll it, or report it as a refused candidate:
	// its Pod template stays byte-identical, Resolved does not go False, no
	// UnpinnedRuntimeImage/CandidateRejected Event is emitted, and it stays Ready.
	// An in-place change (replicas) still applies without a refusal. Only a spec
	// change to a NEW unpinned revision is refused; reverting clears it.
	It("does not report an unchanged unpinned stable as a refused candidate across an upgrade", func() {
		// Build the first revision with the flag ON (the pre-enforcement build), so
		// the unpinned image is admitted; drive it to a recorded stable.
		rBuild := newR(true, false)
		createDM("legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = fakeUnpinnedRuntimeVersion
		})
		rec(rBuild, "legacy")
		rev := getDM("legacy").Status.CandidateRevision.Hash
		markJob("legacy", rev)
		rec(rBuild, "legacy")
		mkGatedPod("legacy", rev, "legacy-a", "10.0.0.150")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(rBuild, "legacy") },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("legacy").Status.StableRevision).NotTo(BeNil())

		// Record the stable as a prior build would: a tag-form image, its runtime
		// version, and ImagePinned unset (the field did not exist then).
		tagImage := fakeImagePrefix + fakeUnpinnedRuntimeVersion
		Expect(updateDMStatus(ctx, namespace, "legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision.Image = tagImage
			dm.Status.StableRevision.RuntimeVersion = fakeUnpinnedRuntimeVersion
			dm.Status.StableRevision.ImagePinned = nil
		})).To(Succeed())

		stableRev := getDM("legacy").Status.StableRevision.Hash
		depKey := types.NamespacedName{Namespace: namespace, Name: "legacy-" + stableRev}
		before := &appsv1.Deployment{}
		rec(rBuild, "legacy")
		Expect(k8sClient.Get(ctx, depKey, before)).To(Succeed())
		beforeTmpl := before.Spec.Template.DeepCopy()

		// Operator upgrade: a fresh reconciler that enforces pinning (flag OFF).
		rUpgraded := newR(false, false)
		_ = countEvents(rUpgraded) // drain any startup events (none expected)
		Expect(rec(rUpgraded, "legacy")).To(Equal(decisionmodelv1alpha1.PhaseReady),
			"the unchanged unpinned stable stays Ready after the upgrade")

		after := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, after)).To(Succeed())
		Expect(after.Spec.Template).To(Equal(*beforeTmpl),
			"the tag-image stable Pod template is byte-identical across the upgrade (no roll, not re-checked)")
		Expect(getDM("legacy").Status.CandidateRevision).To(BeNil(), "no candidate started")
		Expect(getDM("legacy").Status.StableRevision.Image).To(Equal(tagImage), "recorded image kept verbatim")
		// The unchanged spec is NOT reported as a refused candidate.
		rc := cond("legacy", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc.Status).To(Equal(metav1.ConditionTrue), "Resolved stays True, not a refusal")
		Expect(countEvents(rUpgraded)).To(Equal(0), "no UnpinnedRuntimeImage / refusal Event for an unchanged stable")

		// An in-place change (replicas) applies and is still not refused.
		Expect(updateDM(ctx, namespace, "legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Replicas = int32Ptr(2)
		})).To(Succeed())
		rec(rUpgraded, "legacy")
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(int32(2)), "in-place replicas change applied")
		Expect(cond("legacy", decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue))
		Expect(countEvents(rUpgraded)).To(Equal(0), "an in-place change is not a refusal")

		// A spec change to a DIFFERENT unpinned revision is refused (the new-candidate rule).
		Expect(updateDM(ctx, namespace, "legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en" // new revision, still rendered by the unpinned 0.42.0 tag
		})).To(Succeed())
		rec(rUpgraded, "legacy")
		rc = cond("legacy", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonUnpinnedRuntimeImage), "a new unpinned revision is refused")
		Expect(getDM("legacy").Status.StableRevision.Hash).To(Equal(stableRev), "stable unchanged")
		_ = countEvents(rUpgraded) // drain the refusal Event(s)

		// Reverting to the stable's revision clears the refusal: Resolved True again,
		// no new refusal Event.
		Expect(updateDM(ctx, namespace, "legacy", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:en"
		})).To(Succeed())
		rec(rUpgraded, "legacy")
		Expect(cond("legacy", decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue),
			"reverting to the stable revision clears the refusal")
		Expect(countEvents(rUpgraded)).To(Equal(0), "no new refusal Event after reverting")
	})

	// Same guarantee for a stable recorded with an explicit spec.image tag override
	// (not an engine version): an unchanged spec is not refused after the upgrade.
	It("does not refuse an unchanged spec.image-tag stable across an upgrade", func() {
		const tagImage = "registry.example.com/ollaya:prod" // mutable tag, no @sha256:
		rBuild := newR(true, true)                          // flag ON + image override allowed
		createDM("imgstable", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Image = tagImage
		})
		rec(rBuild, "imgstable")
		rev := getDM("imgstable").Status.CandidateRevision.Hash
		markJob("imgstable", rev)
		rec(rBuild, "imgstable")
		mkGatedPod("imgstable", rev, "imgstable-a", "10.0.0.151")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(rBuild, "imgstable") },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		stableRev := getDM("imgstable").Status.StableRevision.Hash

		// Upgrade: enforce pinning (flag OFF) but keep allowing the override; the
		// unchanged spec.image-tag stable must not be refused or re-rolled.
		rUpgraded := newR(false, true)
		_ = countEvents(rUpgraded)
		Expect(rec(rUpgraded, "imgstable")).To(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM("imgstable").Status.StableRevision.Hash).To(Equal(stableRev))
		Expect(getDM("imgstable").Status.CandidateRevision).To(BeNil())
		Expect(cond("imgstable", decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue))
		Expect(countEvents(rUpgraded)).To(Equal(0), "no refusal Event for an unchanged spec.image-tag stable")
	})

	// Edge case: a legacy (10-hex hash) stable whose placement is already recorded
	// and whose image is unpinned. The legacy hash ignores placement, so a bare
	// legacy-hash comparison would wrongly treat a scheduling change as "same
	// revision" and let a new unpinned candidate through. candidateIdentity decides
	// isStable the same way reconcileNormal does (adoption only when placement is
	// empty, then sameIdentity), so a scheduling change on an already-adopted
	// legacy stable is a NEW revision and is refused.
	It("refuses a scheduling change on an already-adopted legacy unpinned stable", func() {
		rBuild := newR(true, false) // flag ON so the unpinned stable can be built
		createDM("adopted", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = fakeUnpinnedRuntimeVersion
		})
		rec(rBuild, "adopted")
		rev := getDM("adopted").Status.CandidateRevision.Hash
		markJob("adopted", rev)
		rec(rBuild, "adopted")
		mkGatedPod("adopted", rev, "adopted-a", "10.0.0.152")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(rBuild, "adopted") },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Rewrite the recorded stable to look like a legacy 10-hex revision whose
		// placement is ALREADY recorded (adoption done) and whose image is unpinned.
		tagImage := fakeImagePrefix + fakeUnpinnedRuntimeVersion
		dmObj := getDM("adopted")
		legacyHash := legacyRevisionHash(dmObj.Spec, dmObj.Status.StableRevision.Digest, tagImage)
		Expect(updateDMStatus(ctx, namespace, "adopted", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision.Hash = legacyHash
			dm.Status.StableRevision.Image = tagImage
			dm.Status.StableRevision.RuntimeVersion = fakeUnpinnedRuntimeVersion
			dm.Status.StableRevision.Placement = placementNone // already recorded, not ""
			dm.Status.StableRevision.ImagePinned = nil
		})).To(Succeed())

		// Upgrade (flag OFF) with the spec unchanged: still the stable revision, not
		// refused.
		rUp := newR(false, false)
		_ = countEvents(rUp)
		rec(rUp, "adopted")
		Expect(cond("adopted", decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue))

		// Now change scheduling: the legacy hash ignores placement, but
		// candidateIdentity sees placement already recorded (not "") so it does NOT
		// adopt, sameIdentity differs on placement -> a NEW unpinned revision ->
		// refused. No candidate workload is created.
		Expect(updateDM(ctx, namespace, "adopted", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{
				NodeSelector: map[string]string{"pool": "gpu"},
			}
		})).To(Succeed())
		rec(rUp, "adopted")
		rc := cond("adopted", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonUnpinnedRuntimeImage), "a scheduling change is a new unpinned revision")
		Expect(getDM("adopted").Status.StableRevision.Hash).To(Equal(legacyHash), "stable unchanged")
		// Refusal happens in preflight, before any allocation: no candidate recorded,
		// so no candidate Deployment / Job / PVC is provisioned for the new revision.
		Expect(getDM("adopted").Status.CandidateRevision).To(BeNil(), "no candidate for the refused revision")
	})
})

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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// A bad candidate spec must never stop the operator from serving and repairing
// the running stable. These specs drive a stable to Ready, then break the live
// spec (missing model, disallowed registry, invalid runtimeVersion, transient
// resolve error) and assert the stable keeps being maintained.
var _ = Describe("a bad candidate never stops stable maintenance", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	markJobComplete := func(name, rev string) {
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
	mkGatedPod := func(name, rev, pod string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = "10.0.0.141"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}

	// newR builds a reconciler; allowedRegistries scopes the registry guard (nil
	// = allow all, so a disallowed-registry case sets it explicitly).
	newR := func(fake *fakeEngine, pr *revProber, allowed []string) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": fake}, Prober: pr,
			Recorder:          events.NewFakeRecorder(64),
			AllowedRegistries: allowed,
		}
	}

	// stable drives a single revision to Ready and returns (reconciler, engine,
	// prober, revA). No candidate is pending.
	stable := func(name string, allowed []string) (*DecisionModelReconciler, *fakeEngine, *revProber, string) {
		fake := newFakeEngine()
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, allowed)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		}
		rec()
		revA := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		pr.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJobComplete(name, revA)
		rec()
		mkGatedPod(name, revA, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return r, fake, pr, revA
	}

	reconcile1 := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	cond := func(name, t string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, t)
	}
	// countEvents drains the reconciler's recorder and counts events containing substr.
	countEvents := func(r *DecisionModelReconciler, substr string) int {
		fr := r.Recorder.(*events.FakeRecorder)
		n := 0
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, substr) {
					n++
				}
			default:
				return n
			}
		}
	}

	// assertStableMaintained: delete the stable Pod, reconcile, and assert the
	// operator recreates it and gates it True (serving/repair continues), the
	// phase stays Ready and Ready=True, while the candidate failure is surfaced on
	// the Resolved condition (never on Degraded, which the stable path owns) and
	// nothing is marked failed.
	assertStableMaintained := func(r *DecisionModelReconciler, name, revA, failReason string) {
		// The stable Pod is deleted (e.g. node drain). Maintenance must recreate it.
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name + "-a"}})).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-a"}, &corev1.Pod{}))
		}, "5s", "50ms").Should(BeTrue())

		reconcile1(r, name)
		// The stable Deployment is still present and selects revA.
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + revA}, dep)).
			To(Succeed(), "stable Deployment maintained despite the bad candidate")
		Expect(dep.Spec.Template.Labels[decisionmodelv1alpha1.LabelRevision]).To(Equal(revA))

		// Recreate the (kubelet-less) stable Pod and let maintenance gate it True.
		mkGatedPod(name, revA, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { reconcile1(r, name); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady), "stable stays Ready")

		ready := cond(name, decisionmodelv1alpha1.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionTrue), "Ready=True while the stable is model-ready")
		// The candidate failure is surfaced on Resolved (never fails the object),
		// and Degraded stays owned by the stable path (healthy here).
		rc := cond(name, decisionmodelv1alpha1.ConditionResolved)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(failReason))
		deg := cond(name, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg.Status).To(Equal(metav1.ConditionFalse), "Degraded stays owned by the stable path")
		Expect(getDM(name).Status.FailedRevision).To(BeNil(), "nothing is marked failed while a stable serves")
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("preflight-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("keeps serving the stable when the new model tag does not exist, then rolls out once fixed", func() {
		r, fake, pr, revA := stable("missing", nil)
		// Point the model at a missing tag: resolve returns ErrNotFound.
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "missing", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())

		assertStableMaintained(r, "missing", revA, reasonModelNotFound)
		Expect(cond("missing", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonModelNotFound))

		// Fix the spec: a valid model resolves and a normal candidate starts.
		fake.mu.Lock()
		fake.resolveErr = nil
		fake.digest = "cafe290000000000000000000000000000000000000000000000000000000000"
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "missing", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcile1(r, "missing")
		Expect(getDM("missing").Status.CandidateRevision).NotTo(BeNil(), "a valid spec resumes the rollout")
		pr.set(getDM("missing").Status.CandidateRevision.Hash, engine.Loaded{})
	})

	It("keeps serving the stable when the new model's registry is not allowed", func() {
		r, _, _, revA := stable("registry", []string{"ollaya.dev"})
		Expect(updateDM(ctx, namespace, "registry", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "evil.example.com/laya:en"
		})).To(Succeed())
		assertStableMaintained(r, "registry", revA, reasonRegistryNotAllowed)
	})

	It("keeps serving the stable when spec.runtimeVersion is invalid", func() {
		r, _, _, revA := stable("rtver", nil)
		Expect(updateDM(ctx, namespace, "rtver", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1" // below the engine minimum -> invalid
		})).To(Succeed())
		assertStableMaintained(r, "rtver", revA, reasonInvalidRuntimeVersion)
	})

	It("keeps serving the stable when the candidate runtime image is an unpinned tag", func() {
		r, _, _, revA := stable("unpinned", nil)
		// 0.9.0 is a valid version the fake renders by a mutable tag (no digest):
		// the candidate is refused (one hash = one set of bytes) but the stable,
		// rendered from status with its pinned image, keeps serving.
		Expect(updateDM(ctx, namespace, "unpinned", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = fakeUnpinnedRuntimeVersion
		})).To(Succeed())
		assertStableMaintained(r, "unpinned", revA, reasonUnpinnedRuntimeImage)
		// No candidate workload was created for the refused revision.
		Expect(getDM("unpinned").Status.CandidateRevision).To(BeNil())
	})

	It("still fails when the model tag is missing and there is NO stable", func() {
		fake := newFakeEngine()
		fake.resolveErr = engine.ErrNotFound
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, nil)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "nostable"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:missing", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		reconcile1(r, "nostable")
		dm := getDM("nostable")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed), "no stable: a missing tag fails as before")
		Expect(cond("nostable", decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonModelNotFound))
	})

	It("maintains the stable and marks nothing failed on a transient resolve error", func() {
		r, fake, _, revA := stable("transient", nil)
		// A registry 5xx (non-NotFound) during resolve.
		fake.mu.Lock()
		fake.resolveErr = fmt.Errorf("registry 503 service unavailable")
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "transient", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())

		reconcile1(r, "transient")
		// Nothing failed; the stable Deployment is intact and Resolved=False/ResolveFailed.
		Expect(getDM("transient").Status.FailedRevision).To(BeNil())
		Expect(getDM("transient").Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseFailed))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "transient-" + revA}, &appsv1.Deployment{})).To(Succeed())
		rc := cond("transient", decisionmodelv1alpha1.ConditionResolved)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonResolveFailed))
		// A transient blip must not mark the DM Degraded and must stay quiet.
		Expect(cond("transient", decisionmodelv1alpha1.ConditionDegraded).Status).To(Equal(metav1.ConditionFalse))
		Expect(countEvents(r, eventCandidateRejected)).To(BeZero(), "a transient resolve blip emits no Warning")
	})

	It("emits one rejection Event across many reconciles with the same bad spec", func() {
		r, fake, _, _ := stable("spam", nil)
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "spam", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())
		_ = countEvents(r, "") // drain any setup events
		for i := 0; i < 5; i++ {
			reconcile1(r, "spam")
		}
		Expect(countEvents(r, eventCandidateRejected)).To(Equal(1), "one Event per transition, not per reconcile")
	})

	It("keeps a stable-side Degraded reason visible when both a stable problem and a bad candidate exist", func() {
		// A stable at replicas 1 that is fully Ready, then scaled to 2 with only one
		// model-ready Pod -> a stable-side Degraded (ReplicasNotModelReady),
		// independent of the candidate. (RWX avoids co-location noise.)
		fake := newFakeEngine()
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, nil)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "both"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Cache: &decisionmodelv1alpha1.CacheSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			},
		})).To(Succeed())
		reconcile1(r, "both")
		revA := RevisionHash(getDM("both").Spec, defaultDigest, fakeImage)
		pr.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJobComplete("both", revA)
		reconcile1(r, "both")
		mkGatedPod("both", revA, "both-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			reconcile1(r, "both")
			return getDM("both").Status.Phase
		}, "5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady), "stable Ready at replicas 1")

		// Scale to 2 with still only one model-ready Pod -> stable-side Degraded.
		Expect(updateDM(ctx, namespace, "both", func(dm *decisionmodelv1alpha1.DecisionModel) {
			two := int32(2)
			dm.Spec.Replicas = &two
		})).To(Succeed())
		Eventually(func() string {
			reconcile1(r, "both")
			d := cond("both", decisionmodelv1alpha1.ConditionDegraded)
			if d == nil || d.Status != metav1.ConditionTrue {
				return ""
			}
			return d.Reason
		}, "5s", "20ms").Should(Equal(reasonReplicasNotModelReady), "stable-side Degraded set")

		// Now also break the candidate spec: the stable-side Degraded must stay.
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "both", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())
		reconcile1(r, "both")
		Expect(cond("both", decisionmodelv1alpha1.ConditionDegraded).Reason).
			To(Equal(reasonReplicasNotModelReady), "stable-side Degraded is not overwritten by the candidate reason")
		Expect(cond("both", decisionmodelv1alpha1.ConditionResolved).Reason).
			To(Equal(reasonModelNotFound), "the candidate failure is on Resolved")
	})

	It("does not roll the stable when its API-key Secret becomes unreadable", func() {
		fake := newFakeEngine()
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newR(fake, pr, nil)
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "key", Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}},
			Data:       map[string][]byte{"token": []byte("s3cr3t")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "keyroll"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key"}, Key: "token",
				}},
			},
		})).To(Succeed())
		reconcile1(r, "keyroll")
		revA := RevisionHash(getDM("keyroll").Spec, defaultDigest, fakeImage)
		pr.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJobComplete("keyroll", revA)
		reconcile1(r, "keyroll")
		mkGatedPod("keyroll", revA, "keyroll-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			reconcile1(r, "keyroll")
			return getDM("keyroll").Status.Phase
		},
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Capture the stable Deployment's Pod template, then make the Secret
		// unreadable (lose its capability label) AND break the candidate model.
		depBefore := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "keyroll-" + revA}, depBefore)).To(Succeed())
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		Expect(func() error {
			s := &corev1.Secret{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key"}, s); err != nil {
				return err
			}
			s.Labels = map[string]string{} // drop the api-key label -> unreadable
			return k8sClient.Update(ctx, s)
		}()).To(Succeed())
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "keyroll", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())

		reconcile1(r, "keyroll")
		depAfter := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "keyroll-" + revA}, depAfter)).To(Succeed())
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable Pod template is unchanged (no roll) when the key is unreadable")
		// The already-gated Pod stays gated.
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "keyroll-a"}, p)).To(Succeed())
		var gated bool
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate && c.Status == corev1.ConditionTrue {
				gated = true
			}
		}
		Expect(gated).To(BeTrue(), "already-gated stable Pod stays gated")
	})

	It("abandons a superseded in-flight candidate when the spec changes to a bad model", func() {
		r, fake, _, _ := stable("supersede", nil)
		// Start a valid candidate (new digest) and get it in flight (Caching).
		fake.mu.Lock()
		fake.digest = "d0d0290000000000000000000000000000000000000000000000000000000000"
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "supersede", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcile1(r, "supersede")
		candA := getDM("supersede").Status.CandidateRevision
		Expect(candA).NotTo(BeNil(), "a candidate is in flight")
		// Its per-revision PVC exists (allocates a volume).
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM("supersede"), candA.Hash)}, &corev1.PersistentVolumeClaim{})).To(Succeed())

		// Now edit the spec to a model that cannot resolve.
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "supersede", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())
		reconcile1(r, "supersede")

		// The superseded candidate is abandoned: candidateRevision cleared and its
		// workloads GCed; a CandidateSuperseded Event is emitted.
		Expect(getDM("supersede").Status.CandidateRevision).To(BeNil(), "superseded candidate abandoned")
		Expect(countEvents(r, eventCandidateSuperseded)).To(BeNumerically(">=", 1))
		// The candidate Deployment is collected (Deployment deletion is reliable in
		// envtest; PVC deletion is not, as the pvc-protection finalizer lingers
		// without the kube-controller-manager, so assert the Delete was issued via
		// a deletionTimestamp instead).
		Eventually(func() bool {
			reconcile1(r, "supersede")
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "supersede-" + candA.Hash}, &appsv1.Deployment{}))
		}, "5s", "50ms").Should(BeTrue(), "the abandoned candidate's Deployment is collected")
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM("supersede"), candA.Hash)}, pvc); err == nil {
			Expect(pvc.DeletionTimestamp).NotTo(BeNil(), "the abandoned candidate's PVC was issued for deletion")
		} else {
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}
	})
})

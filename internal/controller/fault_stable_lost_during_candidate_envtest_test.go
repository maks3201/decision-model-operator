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

// A Ready stable loses its store (PVC deleted, Pod gone) and then spec.model
// changes BEFORE any reconcile starts recovery. The reconcile now takes the
// candidate path (the hash moved). The stable store must still be recovered in
// parallel with the candidate — its PVC recreated (annotated recovering) and a
// recovery prefetch Job created — without blocking the candidate, and it must
// survive a candidate failure.
var _ = Describe("stable store lost then a spec change", func() {
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
	newRec := func(pr Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   pr,
			Recorder: events.NewFakeRecorder(256),
			Now:      func() time.Time { return clock },
		}
	}
	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}
	markJob := func(name, rev string) {
		Eventually(func() error {
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job); err != nil {
				return err
			}
			if jobComplete(job) {
				return nil // already marked (a prior attempt won); fields are immutable
			}
			now := metav1.Now()
			if job.Status.StartTime == nil {
				job.Status.StartTime = &now
			}
			if job.Status.CompletionTime == nil {
				job.Status.CompletionTime = &now
			}
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}
			return k8sClient.Status().Update(ctx, job)
		}, "3s", "20ms").Should(Succeed())
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
		pod.Status.PodIP = "10.0.0.80"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	pvcExists := func(claim string) bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, &corev1.PersistentVolumeClaim{}) == nil
	}
	getPVC := func(claim string) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc)).To(Succeed())
		return pvc
	}
	jobExists := func(name, rev string) bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, &batchv1.Job{}) == nil
	}
	// deletePVCHard deletes a PVC and strips the pvc-protection finalizer so it
	// actually disappears under envtest (which has no kubelet/GC to drop it once no
	// Pod mounts it). This mirrors production, where the PVC becomes NotFound once
	// its last mounting Pod is gone.
	deletePVCHard := func(claim string) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc); err == nil {
			_ = k8sClient.Delete(ctx, pvc)
			cur := &corev1.PersistentVolumeClaim{}
			if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, cur); gerr == nil && len(cur.Finalizers) > 0 {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
			}
		}
	}
	driveReady := func(r *DecisionModelReconciler, name string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		})).To(Succeed())
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		// Reconcile until this revision's prefetch Job exists (resolve + candidate +
		// Caching), then complete it; retrying absorbs a reconcile that returns
		// before the Job is created.
		Eventually(func() bool {
			rec(r, name)
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, &batchv1.Job{}) == nil
		}, "10s", "50ms").Should(BeTrue())
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		clock = time.Now()
		namespace = fmt.Sprintf("stablelost-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("recovers the lost stable store in parallel with the new candidate", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev1 := driveReady(r, "sl1")
		dm := getDM("sl1")
		stableClaim := storeNameRev(dm, rev1)
		Expect(pvcExists(stableClaim)).To(BeTrue(), "the stable store exists while Ready")

		By("the stable store is lost: PVC deleted and the stable Pod removed, before any recovery")
		deletePVCHard(stableClaim)
		Expect(k8sClient.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "sl1-pod-" + rev1}})).To(Succeed())
		Eventually(func() bool { return pvcExists(stableClaim) }, "5s", "50ms").Should(BeFalse())

		By("spec.model changes before any reconcile starts recovery")
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		pr.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		Expect(updateDM(ctx, namespace, "sl1", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM("sl1").Spec, digest2, fakeImage)
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})
		Expect(rev2).NotTo(Equal(rev1))

		By("reconciles drive the candidate AND recover the stable store in parallel")
		Eventually(func() bool {
			rec(r, "sl1")
			return pvcExists(stableClaim)
		}, "10s", "50ms").Should(BeTrue(), "the lost stable store PVC must be recreated during the candidate rollout")
		Expect(getPVC(stableClaim).Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"),
			"the recreated stable store PVC is annotated recovering")
		Eventually(func() bool {
			rec(r, "sl1")
			return jobExists("sl1", rev1)
		}, "10s", "50ms").Should(BeTrue(), "a recovery prefetch Job for the stable revision must be created in parallel")

		By("the candidate is not blocked: it has its own revision and prefetch Job")
		Expect(getDM("sl1").Status.CandidateRevision).NotTo(BeNil())
		Expect(getDM("sl1").Status.CandidateRevision.Hash).To(Equal(rev2))
		Expect(jobExists("sl1", rev2)).To(BeTrue(), "the candidate prefetch Job exists alongside the stable recovery Job")

		By("the stable store returns to serving once its recovery prefetch completes")
		// The original prefetch Job (older than the recreated PVC) is deleted as
		// stale and a fresh recovery Job is created; mark whichever recovery Job is
		// current complete on each pass until the store is refilled.
		Eventually(func() bool {
			rec(r, "sl1")
			if jobExists("sl1", rev1) {
				markJob("sl1", rev1)
				rec(r, "sl1")
			}
			_, recovering := getPVC(stableClaim).Annotations[storeRecoveringAnnotation]
			return !recovering
		}, "15s", "50ms").Should(BeTrue(), "the recovering annotation clears once the stable store is refilled")
	})

	It("keeps the recovered stable store when the candidate fails", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev1 := driveReady(r, "sl2")
		dm := getDM("sl2")
		stableClaim := storeNameRev(dm, rev1)

		By("the stable store is lost, then spec.model changes to a candidate that will fail")
		deletePVCHard(stableClaim)
		Expect(k8sClient.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "sl2-pod-" + rev1}})).To(Succeed())
		Eventually(func() bool { return pvcExists(stableClaim) }, "5s", "50ms").Should(BeFalse())

		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		pr.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		Expect(updateDM(ctx, namespace, "sl2", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = "kev:en"
		})).To(Succeed())
		rev2 := RevisionHash(getDM("sl2").Spec, digest2, fakeImage)
		// The candidate never becomes model-ready (gate mismatch), so it will time
		// out / fail; the stable store recovery must proceed regardless.
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: "deadbeef", Device: "cpu"})

		By("the stable store is recovered in parallel even though the candidate is unhealthy")
		Eventually(func() bool {
			rec(r, "sl2")
			return pvcExists(stableClaim) && jobExists("sl2", rev1)
		}, "10s", "50ms").Should(BeTrue(), "the stable store recovery runs independently of candidate health")
		Expect(getPVC(stableClaim).Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"))
		// The candidate's own revision still exists (not blocked by the recovery).
		Expect(getDM("sl2").Status.CandidateRevision).NotTo(BeNil())
		Expect(getDM("sl2").Status.CandidateRevision.Hash).To(Equal(rev2))
	})

	// --- Visibility: the candidate-path recovery surfaces the stable store health
	// on Degraded (the rollout phase stays the candidate's), deduped, and clears it
	// once the store is serveable again.
	degraded := func(name string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
	}
	drainWarnings := func(r *DecisionModelReconciler, sub string) int {
		fr := r.Recorder.(*events.FakeRecorder)
		n := 0
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, sub) {
					n++
				}
			default:
				return n
			}
		}
	}
	loseStoreThenChangeModel := func(r *DecisionModelReconciler, pr *revProber, name string) (string, string, string) {
		rev1 := driveReady(r, name)
		claim := storeNameRev(getDM(name), rev1)
		deletePVCHard(claim)
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name + "-pod-" + rev1}})).To(Succeed())
		// Delete the original (Complete) prefetch Job too: it predates the recreated
		// PVC, so leaving it would make the kick keep deleting it as stale rather
		// than reaching the recovering/no-Job state the recovery drives from.
		bg := metav1.DeletePropagationBackground
		if j := (&batchv1.Job{}); k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev1)}, j) == nil {
			Expect(k8sClient.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &bg})).To(Succeed())
		}
		Eventually(func() bool { return pvcExists(claim) }, "5s", "50ms").Should(BeFalse())
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		pr.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) { d.Spec.Model = "kev:en" })).To(Succeed())
		rev2 := RevisionHash(getDM(name).Spec, digest2, fakeImage)
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})
		return rev1, rev2, claim
	}

	It("surfaces Degraded=StoreLost while rebuilding, deduped, then clears it when the store is refilled", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev1, _, claim := loseStoreThenChangeModel(r, pr, "vis1")

		By("Degraded=StoreLost appears while the store is being rebuilt")
		Eventually(func() string {
			rec(r, "vis1")
			if d := degraded("vis1"); d != nil {
				return d.Reason
			}
			return ""
		}, "10s", "50ms").Should(Equal(reasonStoreLost))
		Expect(degraded("vis1").Status).To(Equal(metav1.ConditionTrue))
		// The rollout phase stays the candidate's (not forced to a stable phase).
		Expect(getDM("vis1").Status.CandidateRevision).NotTo(BeNil())

		By("the StoreLost Warning is deduped across five reconciles")
		_ = drainWarnings(r, "") // clear accumulated events
		warnings := 0
		for i := 0; i < 5; i++ {
			rec(r, "vis1")
			warnings += drainWarnings(r, eventStoreLost)
		}
		Expect(warnings).To(Equal(0), "StoreLost already set: no repeat Warning while rebuilding")

		By("completing the recovery prefetch clears the Degraded condition")
		Eventually(func() bool {
			rec(r, "vis1")
			if jobExists("vis1", rev1) {
				markJob("vis1", rev1)
				rec(r, "vis1")
			}
			d := degraded("vis1")
			return d == nil || d.Reason != reasonStoreLost
		}, "15s", "50ms").Should(BeTrue(), "once the store is serveable again the recovery Degraded is cleared")
		Expect(getPVC(claim).Annotations).NotTo(HaveKey(storeRecoveringAnnotation))
	})

	It("surfaces Degraded=StorePrefetchFailed when the recovery prefetch Job fails", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev1, _, _ := loseStoreThenChangeModel(r, pr, "vis2")

		By("driving to the recovery Job, then failing it")
		Eventually(func() bool {
			rec(r, "vis2")
			return jobExists("vis2", rev1)
		}, "10s", "50ms").Should(BeTrue())
		failJob := func(name, rev string) {
			Eventually(func() error {
				job := &batchv1.Job{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job); err != nil {
					return err
				}
				if jobFailed(job) {
					return nil
				}
				now := metav1.Now()
				if job.Status.StartTime == nil {
					job.Status.StartTime = &now
				}
				job.Status.Conditions = []batchv1.JobCondition{
					{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
				}
				return k8sClient.Status().Update(ctx, job)
			}, "3s", "20ms").Should(Succeed())
		}
		failJob("vis2", rev1)

		By("the candidate path surfaces Degraded=StorePrefetchFailed, deduped over five reconciles")
		Eventually(func() string {
			rec(r, "vis2")
			if d := degraded("vis2"); d != nil {
				return d.Reason
			}
			return ""
		}, "10s", "50ms").Should(Equal(reasonStorePrefetchFailed))
		_ = drainWarnings(r, "")
		warnings := 0
		for i := 0; i < 5; i++ {
			rec(r, "vis2")
			warnings += drainWarnings(r, eventStorePrefetchFail)
		}
		Expect(warnings).To(Equal(0), "StorePrefetchFailed already set: no repeat Warning")
		Expect(getDM("vis2").Status.CandidateRevision).NotTo(BeNil(), "the rollout phase stays the candidate's")
	})

	It("surfaces Degraded=RegistryNotAllowed when the recovery guard refuses, deduped", func() {
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		// Allow-list refuses the recorded stable registry.
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                 map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                  pr,
			AllowedRegistries:       []string{"ollaya.dev"},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(256),
			Now:                     func() time.Time { return clock },
		}
		rev1 := driveReady(r, "vis3")
		claim := storeNameRev(getDM("vis3"), rev1)
		By("record a now-disallowed registry on the stable, then lose its store and change the model")
		Expect(updateDMStatus(ctx, namespace, "vis3", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Status.StableRevision.Registry = "https://disallowed.example.com"
		})).To(Succeed())
		deletePVCHard(claim)
		Expect(k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "vis3-pod-" + rev1}})).To(Succeed())
		bg := metav1.DeletePropagationBackground
		if j := (&batchv1.Job{}); k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("vis3", rev1)}, j) == nil {
			Expect(k8sClient.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &bg})).To(Succeed())
		}
		Eventually(func() bool { return pvcExists(claim) }, "5s", "50ms").Should(BeFalse())
		fake := r.Engines["ollaya"].(*fakeEngine)
		fake.digest = digest2
		pr.set(rev1, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		Expect(updateDM(ctx, namespace, "vis3", func(d *decisionmodelv1alpha1.DecisionModel) { d.Spec.Model = "kev:en" })).To(Succeed())
		rev2 := RevisionHash(getDM("vis3").Spec, digest2, fakeImage)
		pr.set(rev2, engine.Loaded{Name: "kev:en", Digest: digest2, Device: "cpu"})

		By("the candidate path surfaces Degraded=RegistryNotAllowed and creates no recovery Job")
		Eventually(func() string {
			rec(r, "vis3")
			if d := degraded("vis3"); d != nil {
				return d.Reason
			}
			return ""
		}, "10s", "50ms").Should(Equal(reasonRegistryNotAllowed))
		Expect(jobExists("vis3", rev1)).To(BeFalse(), "a disallowed recovery registry must not create a prefetch Job")
		_ = drainWarnings(r, "")
		warnings := 0
		for i := 0; i < 5; i++ {
			rec(r, "vis3")
			warnings += drainWarnings(r, eventRegistryNotAllowed)
		}
		Expect(warnings).To(Equal(0), "RegistryNotAllowed already set: no repeat Warning")
		Expect(getDM("vis3").Status.CandidateRevision).NotTo(BeNil())
	})
})

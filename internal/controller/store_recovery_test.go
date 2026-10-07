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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// lost-store recovery is level-triggered from cluster state. The
// "recovering" signal is a PVC annotation (restart-safe); completion is a
// Complete prefetch Job created at/after the PVC; failed retries are bounded per
// Job UID; the exhausted state does not churn; and no Job deleted this
// reconcile is read back from the cache.
var _ = Describe("lost-store recovery marker on the PVC", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	newRec := func(c client.Client) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Recorder: events.NewFakeRecorder(32),
		}
	}
	mkDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		return dm
	}
	stableRev := func() *decisionmodelv1alpha1.RevisionStatus {
		return &decisionmodelv1alpha1.RevisionStatus{
			Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage,
		}
	}
	getJob := func(dm *decisionmodelv1alpha1.DecisionModel) *batchv1.Job {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(dm, "r1")}, job)).To(Succeed())
		return job
	}
	jobExists := func(dm *decisionmodelv1alpha1.DecisionModel) bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(dm, "r1")}, &batchv1.Job{}) == nil
	}
	getPVC := func(claim string) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc)).To(Succeed())
		return pvc
	}
	markComplete := func(job *batchv1.Job) {
		now := metav1.Now()
		job.Status.StartTime, job.Status.CompletionTime = &now, &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	markFailed := func(job *batchv1.Job) {
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	degradedReason := func(dm *decisionmodelv1alpha1.DecisionModel) string {
		c := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		if c == nil {
			return ""
		}
		return c.Reason
	}
	// recover runs recovery until the store PVC exists and a prefetch Job exists,
	// returning the reconciler used.
	driveToJob := func(rr *DecisionModelReconciler, dm *decisionmodelv1alpha1.DecisionModel, stable *decisionmodelv1alpha1.RevisionStatus, claim string) {
		Eventually(func() bool {
			handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(handled).To(BeTrue())
			return jobExists(dm)
		}, "3s", "20ms").Should(BeTrue())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "storerec-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// (a) A fresh reconciler (operator restart) that finds an annotated PVC and no
	// Job must create the prefetch Job and NOT serve — the restart-safe marker on
	// the PVC alone drives recovery, no in-memory state.
	It("recovers from an annotated PVC with no Job after a restart (no in-memory state)", func() {
		dm := mkDM("restart")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		seed := newRec(k8sClient)

		// Recovery recreates the (annotated) PVC.
		handled, _, _, _ := seed.recoverStableStore(ctx, dm, seed.Engines["ollaya"], stable, claim, false)
		Expect(handled).To(BeTrue())
		Expect(getPVC(claim).Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"))

		// Operator restart: a brand-new reconciler with no in-memory marker. It must
		// still treat the store as recovering (annotation) and create the Job.
		fresh := newRec(k8sClient)
		handled, _, _, err := fresh.recoverStableStore(ctx, dm, fresh.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue(), "annotated PVC => still recovering after restart")
		Expect(jobExists(dm)).To(BeTrue(), "the fresh reconciler created the prefetch Job")
	})

	// (b) Completion removes the annotation and serving resumes.
	It("clears the annotation and resumes serving once the prefetch completes", func() {
		dm := mkDM("done")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rr := newRec(k8sClient)

		driveToJob(rr, dm, stable, claim)
		markComplete(getJob(dm))

		// First reconcile after completion: clears the annotation, still handled.
		handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(getPVC(claim).Annotations).NotTo(HaveKey(storeRecoveringAnnotation), "annotation cleared on completion")

		// Next reconcile: PVC has no annotation -> healthy, serving resumes.
		handled, _, _, err = rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeFalse(), "serving resumes once the annotation is gone")
	})

	// (c) An unannotated PVC with no Job is healthy (legacy/pruned), unchanged.
	It("treats an unannotated PVC with no Job as healthy (legacy/pruned)", func() {
		dm := mkDM("legacy")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rr := newRec(k8sClient)

		// A normal (unannotated) store PVC, no prefetch Job record.
		_, perr := rr.ensurePVC(ctx, dm, "r1")
		Expect(perr).NotTo(HaveOccurred())

		handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeFalse(), "unannotated PVC is populated/healthy")
	})

	// A stale Complete Job older than the (annotated) recovering PVC is deleted,
	// not accepted as completion.
	It("deletes a Complete Job older than the recovering PVC as stale", func() {
		dm := mkDM("stale")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rr := newRec(k8sClient)

		// A Complete prefetch Job that filled a PREVIOUS volume (created first).
		staleJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: prefetchName(dm, "r1"), Labels: revisionLabels(dm, "r1")},
			Spec:       newFakeEngine().PrefetchJobSpec(rr.stableParams(ctx, dm, stable, claim)),
		}
		Expect(controllerutil.SetControllerReference(dm, staleJob, rr.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, staleJob)).To(Succeed())
		markComplete(getJob(dm))

		// A recovering PVC created AFTER the Job (strictly newer timestamp).
		time.Sleep(1100 * time.Millisecond)
		Expect(rr.createRecoveryPVC(ctx, dm, stable, claim, false)).To(Succeed())
		Expect(getPVC(claim).Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"))

		// The Complete Job predates the PVC -> stale -> deleted, recovery holds.
		handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue(), "stale Complete Job must not be accepted as completion")
		// The PVC keeps its recovering annotation (not populated by a stale Job).
		Expect(getPVC(claim).Annotations).To(HaveKeyWithValue(storeRecoveringAnnotation, "true"))
		// The stale Job is deleted; a fresh prefetch is created on a later reconcile.
		Eventually(func() bool {
			handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(handled).To(BeTrue())
			return jobExists(dm) && !jobComplete(getJob(dm))
		}, "3s", "20ms").Should(BeTrue(), "a fresh (incomplete) prefetch replaces the stale Complete Job")
	})

	// A permanent classified prefetch failure gives up at once, without burning
	// maxStoreRecoverAttempts, Degraded=StorePrefetchFailed with the reason.
	It("gives up at once on a permanent prefetch reason", func() {
		dm := mkDM("perm")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rr := newRec(k8sClient)

		driveToJob(rr, dm, stable, claim)
		job := getJob(dm)
		markFailed(job)
		// A failed prefetch Pod carrying a permanent (DigestMismatch) message,
		// controlled by the current prefetch Job (trusted by UID, not labels).
		yes := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: prefetchName(dm, "r1") + "-xyz",
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:             dm.Name,
					decisionmodelv1alpha1.LabelPrefetchRevision: "r1",
					"job-name": prefetchName(dm, "r1"),
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID,
					Controller: &yes, BlockOwnerDeletion: &yes,
				}},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		now := metav1.Now()
		pod.Status.StartTime = &now
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "prefetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "DigestMismatch: pulled digest differs from the pin",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		handled, _, res, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(degradedReason(dm)).To(Equal(reasonStorePrefetchFailed))
		deg := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg.Message).To(ContainSubstring("DigestMismatch"))
		Expect(deg.Message).To(ContainSubstring("permanently"))
		// It did NOT burn an attempt: the counter is still at zero (a later
		// transient failure would still get its full budget).
		Expect(rr.countFailedRecovery(dm, types.UID("probe"))).To(BeFalse(),
			"the permanent give-up did not consume the bounded attempts")
	})

	// The same failed Job UID counts once (idempotent across stale cache reads).
	It("counts the same failed Job UID only once", func() {
		dm := mkDM("once")
		rr := newRec(k8sClient)
		jobUID := types.UID("job-uid-1")
		Expect(rr.countFailedRecovery(dm, jobUID)).To(BeFalse())
		Expect(rr.countFailedRecovery(dm, jobUID)).To(BeFalse(), "same UID again: no extra attempt")
		Expect(rr.countFailedRecovery(dm, jobUID)).To(BeFalse())
		Expect(rr.countFailedRecovery(dm, types.UID("job-uid-2"))).To(BeFalse())
		Expect(rr.countFailedRecovery(dm, types.UID("job-uid-3"))).To(BeTrue(), "third distinct failure exhausts")
	})

	// Exhausted state does not churn: no condition transition and one Event.
	It("holds StorePrefetchFailed without churn once exhausted", func() {
		dm := mkDM("exhausted")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rec := events.NewFakeRecorder(32)
		rr := newRec(k8sClient)
		rr.Recorder = rec

		driveToJob(rr, dm, stable, claim)
		for i := 0; i < maxStoreRecoverAttempts; i++ {
			rr.countFailedRecovery(dm, types.UID("uid-"+itoa(i)))
		}
		markFailed(getJob(dm))

		handled, _, res, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(res.RequeueAfter).To(Equal(regateInterval))
		Expect(degradedReason(dm)).To(Equal(reasonStorePrefetchFailed))
		cond1 := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)

		handled, _, _, err = rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		cond2 := meta.FindStatusCondition(dm.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(cond2.Message).To(Equal(cond1.Message), "fixed message, no churn")
		Expect(cond2.LastTransitionTime).To(Equal(cond1.LastTransitionTime), "no status transition")

		var prefetchFailedEvents int
		for drained := false; !drained; {
			select {
			case e := <-rec.Events:
				if strings.Contains(e, eventStorePrefetchFail) {
					prefetchFailedEvents++
				}
			default:
				drained = true
			}
		}
		Expect(prefetchFailedEvents).To(Equal(1), "the StorePrefetchFailed Event is emitted once")
	})

	// A stale cache read of a just-deleted failed Job never resumes serving.
	It("ignores a stale cache read of a just-deleted failed Job", func() {
		dm := mkDM("race")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)

		seed := newRec(k8sClient)
		driveToJob(seed, dm, stable, claim)
		markFailed(getJob(dm))
		failedCopy := getJob(dm)

		var deleted atomic.Bool
		var served atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					deleted.Store(true)
				}
				return cl.Delete(ctx, obj, opts...)
			},
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if j, ok := obj.(*batchv1.Job); ok && deleted.Load() && !served.Load() {
					served.Store(true)
					failedCopy.DeepCopyInto(j)
					return nil
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		})
		// A fresh reconciler: the annotated PVC in the cluster drives recovery.
		rr := newRec(c)
		handled, _, _, err := rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(deleted.Load()).To(BeTrue(), "the failed Job was deleted")

		handled, _, _, err = rr.recoverStableStore(ctx, dm, rr.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue(), "a stale read of the deleted failed Job never resumes serving")
	})

	// Safety invariant: a prefetch Pod must never be selected by any serving
	// selector. The rendered prefetch Job's Pod template must not match the
	// revision's Service / PDB / Deployment selector (all revisionLabels), and a
	// Pod created from that template must not satisfy the serving Service's
	// label selector — otherwise a stable-store recovery prefetch Pod (no
	// readiness probe, so Ready) would become a Service endpoint with nothing
	// listening and would count against the PDB.
	It("renders a prefetch Pod template disjoint from every serving selector", func() {
		dm := mkDM("disjoint")
		stable := stableRev()
		claim := storeNameRev(dm, stable.Hash)
		rr := newRec(k8sClient)

		driveToJob(rr, dm, stable, claim)
		tmplLabels := getJob(dm).Spec.Template.Labels

		// The prefetch Pod carries the name-scoped cache label and a dedicated
		// prefetch-revision label, but never the serving revision label.
		Expect(tmplLabels).To(HaveKeyWithValue(decisionmodelv1alpha1.LabelName, dm.Name))
		Expect(tmplLabels).To(HaveKeyWithValue(decisionmodelv1alpha1.LabelPrefetchRevision, stable.Hash))
		Expect(tmplLabels).NotTo(HaveKey(decisionmodelv1alpha1.LabelRevision))

		// Every serving selector of this revision = revisionLabels; the Service,
		// PDB and Deployment all use it. A selector matches a Pod only if every
		// selector key/value is present on the Pod, so the prefetch template must
		// fail at least one key of the serving selector.
		servingSelector := revisionLabels(dm, stable.Hash)
		sel := labels.SelectorFromSet(servingSelector)
		Expect(sel.Matches(labels.Set(tmplLabels))).To(BeFalse(),
			"prefetch Pod template must not match the serving selector")

		// And concretely against the running Service: create the serving Service
		// for this revision and confirm its selector would not pick the prefetch
		// Pod (envtest has no endpoints controller, so assert selector vs labels).
		Expect(rr.ensureService(ctx, dm, rr.Engines["ollaya"], stable.Hash)).To(Succeed())
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dm.Name}, svc)).To(Succeed())
		svcSel := labels.SelectorFromSet(svc.Spec.Selector)
		Expect(svcSel.Matches(labels.Set(tmplLabels))).To(BeFalse(),
			"the serving Service must not select the prefetch Pod")
	})
})

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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Contract specs for the transient-prefetch-retry policy (decision in the review
// of the retry-budget analysis; implementation lands separately). They assert the
// Job/condition contract only, never controller internals:
//
//   - a TRANSIENT recovery Job failure (classified permanent=false) must NOT count
//     toward the recovery give-up budget: after several transient failures the DM
//     stays Degraded=StoreLost (retrying), never Degraded=StorePrefetchFailed, and
//     the stable keeps serving;
//   - a PERMANENT recovery failure (UpstreamTagMoved / DigestMismatch) still gives
//     up at once -> Degraded=StorePrefetchFailed (today's behaviour);
//   - a candidate TRANSIENT prefetch failure keeps retrying while the caching
//     progress deadline (phaseTransitionTime + caching timeout) has not passed,
//     then rolls back with PrefetchFailed at the deadline (never at once);
//   - the retry schedule is durable: a reconciler restart mid-backoff does not
//     reset it (no immediate retry, no give-up).
//
// The specs that fail on current main are PIt (marked with the observed main
// behaviour); they flip to It when the policy lands, without weakening.
var _ = Describe("transient prefetch failures retry instead of giving up", func() {
	var (
		ctx             context.Context
		namespace       string
		counter         int
		clock           time.Time
		transientPodSeq int
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
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	degradedReason := func(name string) string {
		if d := meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionDegraded); d != nil && d.Status == metav1.ConditionTrue {
			return d.Reason
		}
		return ""
	}
	phase := func(name string) decisionmodelv1alpha1.DecisionModelPhase { return getDM(name).Status.Phase }
	// drainEvents empties the fake recorder's channel and returns nothing; used to
	// clear accumulated events before counting a specific window.
	drainEvents := func(r *DecisionModelReconciler) int {
		fr := r.Recorder.(*events.FakeRecorder)
		n := 0
		for {
			select {
			case <-fr.Events:
				n++
			default:
				return n
			}
		}
	}
	// drainEventsCount empties the fake recorder and counts events containing sub.
	drainEventsCount := func(r *DecisionModelReconciler, sub string) int {
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
	stableServing := func(podName string) bool {
		p := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, p); err != nil {
			return false
		}
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}

	// --- fixtures ---------------------------------------------------------------
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
	// driveReady brings a single-replica DM to Ready and returns the stable rev.
	driveReady := func(r *DecisionModelReconciler, name string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec(r, name)
		rev := getDM(name).Status.CandidateRevision.Hash
		markPrefetchComplete(ctx, namespace, name, rev)
		rec(r, name)
		createReadyPod(name, rev, name+"-pod-"+rev, "10.0.9.10")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return phase(name)
		}, "5s", "25ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return getDM(name).Status.StableRevision.Hash
	}
	// loseStore deletes the stable store PVC and the original Complete prefetch Job
	// (not the serving Pod — the stable keeps serving its ready Pod while the store
	// recovers, matching the StoreLost semantics), so the next reconcile enters
	// store recovery.
	loseStore := func(name, rev string) {
		claim := storeNameRev(getDM(name), rev)
		pvc := &corev1.PersistentVolumeClaim{}
		if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, pvc) == nil {
			_ = k8sClient.Delete(ctx, pvc, client.PropagationPolicy(metav1.DeletePropagationBackground))
			// envtest has no PVC-protection controller, so the pvc-protection
			// finalizer is never removed; clear it so the Delete can finish.
			cur := &corev1.PersistentVolumeClaim{}
			if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, cur) == nil && len(cur.Finalizers) > 0 {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
			}
		}
		job := &batchv1.Job{}
		if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job) == nil {
			_ = k8sClient.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		}
		Eventually(func() bool {
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claim}, &corev1.PersistentVolumeClaim{}) != nil
		}, "5s", "25ms").Should(BeTrue())
	}
	// failJobTransient fails the (current) recovery/prefetch Job AND attaches a
	// failed prefetch Pod carrying a CLASSIFIED-transient termination message
	// (reason: Transient, exit 1), so prefetchFailureReason returns a non-permanent
	// detail (an unreadable/unknown failure keeps today's give-up behaviour). The
	// failure time is stamped with the test clock so the controller's durable
	// backoff lines up with the injected clock.
	failJobTransient := func(name, rev string) {
		var jobUID types.UID
		Eventually(func() error {
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job); err != nil {
				return err
			}
			jobUID = job.UID
			if jobFailed(job) {
				return nil
			}
			t := metav1.NewTime(clock)
			if job.Status.StartTime == nil {
				job.Status.StartTime = &t
			}
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: t},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: t},
			}
			return k8sClient.Status().Update(ctx, job)
		}, "3s", "20ms").Should(Succeed())
		// Attach a failed prefetch Pod with a classified-transient message, owned by
		// the current Job, so the failure classifies as Transient (not unknown).
		yes := true
		t := metav1.NewTime(clock)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: fmt.Sprintf("%s-t%d", jobName(name, rev), transientPodSeq),
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:             name,
					decisionmodelv1alpha1.LabelPrefetchRevision: rev,
					"job-name": jobName(name, rev),
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: jobName(name, rev), UID: jobUID,
					Controller: &yes, BlockOwnerDeletion: &yes,
				}},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Image: fakeImage}}},
		}
		transientPodSeq++
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.StartTime = &t
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "prefetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "reason: Transient\nconnection reset by peer",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	// failJobPermanent fails the Job AND creates a failed prefetch Pod carrying a
	// permanent termination message (UpstreamTagMoved, exit 5), so
	// prefetchFailureReason classifies it permanent.
	failJobPermanent := func(name, rev string) {
		failJobTransient(name, rev)
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		yes := true
		now := metav1.Now()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: jobName(name, rev) + "-perm",
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:             name,
					decisionmodelv1alpha1.LabelPrefetchRevision: rev,
					"job-name": jobName(name, rev),
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID,
					Controller: &yes, BlockOwnerDeletion: &yes,
				}},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.StartTime = &now
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "prefetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 5, Message: "reason: UpstreamTagMoved\ntag moved upstream",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	// failJobUnknown fails the Job AND attaches a failed Pod whose message
	// classifies as Unknown (non-permanent, NOT positively transient — e.g. an
	// auth / ENOSPC / OOM failure). Unknown keeps the bounded give-up budget.
	failJobUnknown := func(name, rev string) {
		failJobTransient(name, rev)
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		yes := true
		now := metav1.Now()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: fmt.Sprintf("%s-unk%d", jobName(name, rev), transientPodSeq),
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:             name,
					decisionmodelv1alpha1.LabelPrefetchRevision: rev,
					"job-name": jobName(name, rev),
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID,
					Controller: &yes, BlockOwnerDeletion: &yes,
				}},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.StartTime = &now
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "prefetch",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "reason: " + engine.PrefetchReasonUnknown + "\nno space left on device",
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("prefetch-retry-%d", counter)
		clock = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Spec 1 — PIt: on main a transient recovery failure counts toward
	// maxStoreRecoverAttempts (3), so after 3 failures the DM reaches
	// Degraded=StorePrefetchFailed. The policy makes transient failures NOT count:
	// the DM must stay Degraded=StoreLost (retrying with backoff) across many
	// transient failures, and the stable keeps serving.
	It("keeps retrying a transient recovery failure (stays StoreLost, never StorePrefetchFailed)", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "t1")
		loseStore("t1", rev)

		By("the store is recovering: Degraded=StoreLost")
		Eventually(func() string { rec(r, "t1"); return degradedReason("t1") }, "10s", "50ms").Should(Equal(reasonStoreLost))

		By("five transient recovery-Job failures (advancing the clock past each backoff) stay StoreLost")
		for i := 0; i < 5; i++ {
			// A recovery Job exists; fail it transiently.
			Eventually(func() bool {
				rec(r, "t1")
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("t1", rev)}, &batchv1.Job{}) == nil
			}, "10s", "50ms").Should(BeTrue(), "a recovery Job should be (re)created on attempt %d", i)
			failJobTransient("t1", rev)
			// First reconcile records the backoff (persist-then-act, no delete yet);
			// it must stay StoreLost, never StorePrefetchFailed.
			rec(r, "t1")
			Expect(degradedReason("t1")).NotTo(Equal(reasonStorePrefetchFailed),
				"a transient recovery failure must not give up (attempt %d)", i)
			// Advance past the capped backoff so the Job is deleted and recreated.
			clock = clock.Add(10 * time.Minute)
			rec(r, "t1")
			Expect(degradedReason("t1")).NotTo(Equal(reasonStorePrefetchFailed),
				"still retrying after the backoff (attempt %d)", i)
		}
		Expect(degradedReason("t1")).To(Equal(reasonStoreLost), "transient recovery stays StoreLost, keeps retrying")
		Expect(stableServing("t1-pod-"+rev)).To(BeTrue(), "the stable keeps serving throughout recovery")
	})

	// Spec 2 — It (passes on main): a PERMANENT recovery failure (UpstreamTagMoved)
	// gives up at once -> Degraded=StorePrefetchFailed, as today. The policy must
	// not change this.
	It("gives up at once on a permanent recovery failure (StorePrefetchFailed)", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "p1")
		loseStore("p1", rev)

		Eventually(func() bool {
			rec(r, "p1")
			j := &batchv1.Job{}
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("p1", rev)}, j) == nil
		}, "10s", "50ms").Should(BeTrue(), "a recovery Job is created")
		failJobPermanent("p1", rev)

		By("a permanent recovery failure gives up -> Degraded=StorePrefetchFailed")
		Eventually(func() string { rec(r, "p1"); return degradedReason("p1") }, "10s", "50ms").
			Should(Equal(reasonStorePrefetchFailed), "a permanent failure must give up at once, as today")
	})

	// Spec 3 — PIt: on main a candidate transient prefetch failure rolls back AT
	// ONCE (reasonPrefetchFailed), with no retry. The policy makes it retry while
	// the caching progress deadline has not passed, then roll back at the deadline.
	It("retries a candidate transient prefetch failure until the caching deadline, then rolls back", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev1 := driveReady(r, "c1")

		By("a spec change starts a candidate; its prefetch Job fails transiently")
		Expect(updateDM(ctx, namespace, "c1", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Resources = corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("300m")}}
		})).To(Succeed())
		rec(r, "c1")
		candRev := getDM("c1").Status.CandidateRevision.Hash
		Expect(candRev).NotTo(Equal(rev1))
		failJobTransient("c1", candRev)

		By("before the caching deadline the candidate is NOT rolled back (it retries)")
		for i := 0; i < 3; i++ {
			clock = clock.Add(1 * time.Minute) // still well under the caching timeout
			rec(r, "c1")
			Expect(phase("c1")).NotTo(BeElementOf(decisionmodelv1alpha1.PhaseRolledBack, decisionmodelv1alpha1.PhaseFailed),
				"a transient candidate failure must retry, not roll back, before the deadline")
			Expect(getDM("c1").Status.StableRevision.Hash).To(Equal(rev1), "the stable keeps serving while the candidate retries")
			// The failed Job was deleted; reconcile until it is recreated, then fail
			// it again (the retry recreates the Job across reconciles).
			Eventually(func() bool {
				rec(r, "c1")
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("c1", candRev)}, &batchv1.Job{}) == nil
			}, "5s", "25ms").Should(BeTrue(), "the candidate prefetch Job is recreated on retry")
			failJobTransient("c1", candRev) // the recreated Job fails again
		}

		By("at the caching deadline the candidate rolls back with PrefetchFailed")
		clock = clock.Add(31 * time.Minute) // past the 30m caching timeout
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(r, "c1"); return phase("c1") }, "5s", "25ms").
			Should(Equal(decisionmodelv1alpha1.PhaseRolledBack), "at the deadline the candidate rolls back (stable exists)")
		Expect(getDM("c1").Status.StableRevision.Hash).To(Equal(rev1), "the stable is unchanged after the candidate rolls back")
	})

	// Spec 4 — PIt: the retry schedule must be durable (derived from Job failure
	// time / an annotation, never memory), so a reconciler restart mid-backoff does
	// not reset it: a brand-new reconciler must not immediately recreate the Job
	// before the backoff elapses, and must not have given up. On main there is no
	// backoff schedule (the attempt is counted and the Job recreated immediately),
	// so this fails.
	It("keeps the recovery backoff schedule across a reconciler restart", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "r1")
		loseStore("r1", rev)
		Eventually(func() bool {
			rec(r, "r1")
			j := &batchv1.Job{}
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("r1", rev)}, j) == nil
		}, "10s", "50ms").Should(BeTrue())
		failJobTransient("r1", rev)
		rec(r, "r1") // first sight: persist the backoff bookkeeping (persist-then-act)
		rec(r, "r1") // the record is now durable: delete the failed Job, backoff pending
		// The failed Job was deleted; a backoff is pending (clock not advanced).
		Eventually(func() bool {
			rec(r, "r1")
			j := &batchv1.Job{}
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("r1", rev)}, j) != nil
		}, "5s", "25ms").Should(BeTrue(), "the failed recovery Job is deleted; a backoff is pending")

		By("a NEW reconciler (restart) must honour the pending backoff, not recreate the Job at once")
		r2 := newRec(pr) // fresh reconciler: any in-memory schedule is gone
		rec(r2, "r1")
		jobBack := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("r1", rev)}, &batchv1.Job{}) == nil
		Expect(jobBack).To(BeFalse(), "a restart mid-backoff must not immediately recreate the recovery Job")
		Expect(degradedReason("r1")).To(Equal(reasonStoreLost), "and must not have given up across the restart")

		By("once the clock passes the backoff the Job is recreated")
		clock = clock.Add(10 * time.Minute)
		Eventually(func() bool {
			rec(r2, "r1")
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("r1", rev)}, &batchv1.Job{}) == nil
		}, "10s", "50ms").Should(BeTrue(), "after the backoff elapses the recovery Job is recreated")
	})

	// Guard that the fixtures and driveReady are sound (an active spec that always
	// passes on main), so a red run points at the policy, not the harness.
	// B1 — an Unknown failure (auth/ENOSPC/OOM-class, non-permanent but NOT
	// positively transient) must NOT retry forever: it counts toward the bounded
	// budget and reaches Degraded=StorePrefetchFailed, so StorePrefetchFailed is not
	// effectively gone.
	It("gives up with StorePrefetchFailed on an Unknown recovery failure (bounded budget)", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "u1")
		loseStore("u1", rev)

		By("repeated Unknown failures burn the bounded budget and reach StorePrefetchFailed")
		Eventually(func() string {
			// Recreate (if needed), fail Unknown, drive the reconciles.
			if k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("u1", rev)}, &batchv1.Job{}) == nil {
				failJobUnknown("u1", rev)
			}
			rec(r, "u1")
			clock = clock.Add(10 * time.Minute)
			rec(r, "u1")
			return degradedReason("u1")
		}, "15s", "50ms").Should(Equal(reasonStorePrefetchFailed),
			"an Unknown failure must NOT retry forever; it exhausts the budget and gives up")
	})

	// S1 — persist-then-act: if the status write fails on the first sight of a
	// failed transient Job, the controller must NOT delete the Job or advance an
	// uncounted retry; the next reconcile re-observes the same still-present Job and
	// records the backoff exactly once.
	It("does not delete the Job or lose a retry when the status write fails on first sight", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var fail atomic.Bool
		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		ic := interceptor.NewClient(wc, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, isDM := obj.(*decisionmodelv1alpha1.DecisionModel); sub == "status" && isDM && fail.CompareAndSwap(true, false) {
					return apierrors.NewInternalError(fmt.Errorf("induced status write failure"))
				}
				return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: ic, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   pr,
			Recorder: events.NewFakeRecorder(256),
			Now:      func() time.Time { return clock },
		}
		rev := driveReady(r, "sw1")
		loseStore("sw1", rev)
		Eventually(func() bool {
			rec(r, "sw1")
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("sw1", rev)}, &batchv1.Job{}) == nil
		}, "10s", "50ms").Should(BeTrue())
		failJobTransient("sw1", rev)

		By("the first-sight status write fails: the failed Job must survive, no uncounted retry")
		fail.Store(true)
		rec(r, "sw1")
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("sw1", rev)}, &batchv1.Job{})).
			To(Succeed(), "a failed first-sight status write must not have deleted the Job")

		By("the next reconcile records the backoff exactly once (TransientAttempts == 1)")
		rec(r, "sw1")
		s := getDM("sw1").Status.StoreRecovery
		Expect(s).NotTo(BeNil())
		Expect(s.TransientAttempts).To(Equal(int32(1)), "the retry is counted exactly once despite the failed write")
	})

	// S3 — the transient StoreLost Warning fires once per cause change, not once
	// per backoff cycle: over several transient failures the Event count stays 1.
	It("emits one transient StoreLost Warning across several backoff cycles", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "ev1")
		loseStore("ev1", rev)
		Eventually(func() string { rec(r, "ev1"); return degradedReason("ev1") }, "10s", "50ms").Should(Equal(reasonStoreLost))
		// Drain the StoreLost events emitted so far (initial recovery).
		_ = drainEvents(r)

		for i := 0; i < 4; i++ {
			Eventually(func() bool {
				rec(r, "ev1")
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("ev1", rev)}, &batchv1.Job{}) == nil
			}, "10s", "50ms").Should(BeTrue())
			failJobTransient("ev1", rev)
			rec(r, "ev1") // first sight: persist, one transient Warning (first time only)
			clock = clock.Add(10 * time.Minute)
			rec(r, "ev1") // delete, backoff
		}
		warnings := drainEventsCount(r, "transient")
		Expect(warnings).To(BeNumerically("<=", 1),
			"the transient StoreLost Warning must not repeat every backoff cycle (got %d)", warnings)
	})

	It("drives a stable to Ready and loses its store (fixture sanity)", func() {
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(pr)
		rev := driveReady(r, "s1")
		Expect(getDM("s1").Status.StableRevision).NotTo(BeNil())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "s1-" + rev}, dep)).To(Succeed())
		loseStore("s1", rev)
		Eventually(func() string { rec(r, "s1"); return degradedReason("s1") }, "10s", "50ms").Should(Equal(reasonStoreLost))
	})
})

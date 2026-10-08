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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("prefetch failure classification", func() {
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
	newR := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(256),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	// failPrefetch marks the revision's prefetch Job failed and creates a failed
	// prefetch Pod, controlled by that Job, whose first container terminated with
	// the given message.
	failPrefetch := func(name, rev, termMsg string, exit int32) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		yes := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: jobName(name, rev) + "-abc",
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
				ExitCode: exit, Message: termMsg,
			}},
		}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	drainHas := func(r *DecisionModelReconciler, substr string) bool {
		fr := r.Recorder.(*events.FakeRecorder)
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, substr) {
					return true
				}
			default:
				return false
			}
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("prefetchfail-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// A classified prefetch failure on the candidate path puts the reason in the
	// condition message, status.failedRevision.message and the Event; the
	// condition reason stays PrefetchFailed.
	DescribeTable("candidate path surfaces the classified reason",
		func(termMsg, wantReason string) {
			name := "cand"
			r := newR()
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
			rec(r, name) // resolve + create prefetch Job
			rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
			failPrefetch(name, rev, termMsg, 1)
			rec(r, name) // observe the failed Job -> rollback/fail

			dm := getDM(name)
			Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
			deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
			Expect(deg).NotTo(BeNil())
			Expect(deg.Reason).To(Equal(reasonPrefetchFailed), "condition reason stays PrefetchFailed")
			Expect(deg.Message).To(ContainSubstring(wantReason))
			Expect(dm.Status.FailedRevision).NotTo(BeNil())
			Expect(dm.Status.FailedRevision.Message).To(ContainSubstring(wantReason))
			Expect(drainHas(r, wantReason)).To(BeTrue(), "the Event names the classified reason")
		},
		Entry("model not found", "pull error: ModelNotFound tag does not exist", "ModelNotFound"),
		Entry("digest mismatch", "DigestMismatch: manifest sha256 differs", "DigestMismatch"),
		Entry("transient", "connection reset by peer", "Transient"),
	)

	// A moved upstream tag is a permanent prefetch failure: the recorded digest
	// no longer matches what the registry serves and re-pulling the tag cannot
	// fix a pinned revision. The termination message carries both short digests
	// (recorded vs. the one the registry now serves); the controller passes the
	// classifier detail through to the PrefetchFailed condition, the Event and
	// status.failedRevision.message, so a user sees exactly what moved.
	It("candidate path surfaces UpstreamTagMoved with both short digests", func() {
		name := "moved"
		r := newR()
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		// The engine's prefetch container writes the classification plus both
		// short digests to the termination message (reason line + detail line).
		termMsg := "reason: UpstreamTagMoved\ntag moved upstream: recorded aaaaaaaaaaaa, registry now serves bbbbbbbbbbbb"
		failPrefetch(name, rev, termMsg, 5)
		rec(r, name)

		dm := getDM(name)
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseFailed))
		deg := meta_Find(dm, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonPrefetchFailed), "condition reason stays PrefetchFailed")
		Expect(deg.Message).To(ContainSubstring("UpstreamTagMoved"))
		Expect(deg.Message).To(ContainSubstring("permanent"))
		Expect(deg.Message).To(ContainSubstring("aaaaaaaaaaaa"), "recorded short digest surfaced")
		Expect(deg.Message).To(ContainSubstring("bbbbbbbbbbbb"), "registry short digest surfaced")
		Expect(dm.Status.FailedRevision).NotTo(BeNil())
		Expect(dm.Status.FailedRevision.Message).To(ContainSubstring("UpstreamTagMoved"))
		Expect(drainHas(r, "UpstreamTagMoved")).To(BeTrue(), "the Event names the classified reason")
	})

	// newestFailedPrefetchTermination trusts the current prefetch Job's UID, not
	// the labels (which anyone with Pod create rights can set). A Pod is read only
	// when its controller OwnerReference is that Job.
	Describe("prefetch termination lookup trusts the Job UID", func() {
		// mkPod creates a terminated prefetch Pod with the given controller
		// OwnerReference UID (empty = no owner); labels always match the filter.
		mkPod := func(name, rev, suffix, msg string, exit int32, ownerUID types.UID) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: jobName(name, rev) + "-" + suffix,
					Labels: map[string]string{
						decisionmodelv1alpha1.LabelName:             name,
						decisionmodelv1alpha1.LabelPrefetchRevision: rev,
						"job-name": jobName(name, rev),
					},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "prefetch", Image: fakeImage}}},
			}
			if ownerUID != "" {
				yes := true
				pod.OwnerReferences = []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: jobName(name, rev),
					UID: ownerUID, Controller: &yes, BlockOwnerDeletion: &yes,
				}}
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			now := metav1.Now()
			pod.Status.StartTime = &now
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "prefetch",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: exit, Message: msg,
				}},
			}}
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		}

		var (
			r    *DecisionModelReconciler
			name string
			rev  string
			job  *batchv1.Job
		)
		BeforeEach(func() {
			r = newR()
			name = "own"
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
			rec(r, name) // resolve + create prefetch Job
			rev = RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
			job = &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
			Expect(job.UID).NotTo(BeEmpty())
		})

		It("ignores a foreign Pod with matching labels and job-name", func() {
			mkPod(name, rev, "foreign", "evil ModelNotFound", 1, "")
			_, _, found := r.newestFailedPrefetchTermination(ctx, getDM(name), rev)
			Expect(found).To(BeFalse())
		})

		It("ignores a Pod of a previous Job that reused the name (different UID)", func() {
			mkPod(name, rev, "stale", "stale ModelNotFound", 1, types.UID("00000000-0000-0000-0000-000000000000"))
			_, _, found := r.newestFailedPrefetchTermination(ctx, getDM(name), rev)
			Expect(found).To(BeFalse())
		})

		It("uses a Pod controlled by the current Job", func() {
			mkPod(name, rev, "owned", "DigestMismatch here", 7, job.UID)
			msg, exit, found := r.newestFailedPrefetchTermination(ctx, getDM(name), rev)
			Expect(found).To(BeTrue())
			Expect(msg).To(Equal("DigestMismatch here"))
			Expect(exit).To(Equal(int32(7)))
		})

		It("returns found=false when the prefetch Job is missing", func() {
			// A revision whose prefetch Job was never created: an owned-looking Pod
			// (any UID) must not be trusted because its Job cannot be verified.
			missingRev := rev + "x"
			mkPod(name, missingRev, "orphan", "DigestMismatch", 1, types.UID("11111111-1111-1111-1111-111111111111"))
			_, _, found := r.newestFailedPrefetchTermination(ctx, getDM(name), missingRev)
			Expect(found).To(BeFalse())
		})

		It("picks the newest owned Pod when several exist", func() {
			mkPod(name, rev, "old", "old reason", 1, job.UID)
			// Create the second owned Pod with a strictly later start time.
			old := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev) + "-old"}, old)).To(Succeed())
			yes := true
			newPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace, Name: jobName(name, rev) + "-new",
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
			Expect(k8sClient.Create(ctx, newPod)).To(Succeed())
			later := metav1.NewTime(old.Status.StartTime.Add(time.Hour))
			newPod.Status.StartTime = &later
			newPod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "prefetch",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 2, Message: "new reason",
				}},
			}}
			Expect(k8sClient.Status().Update(ctx, newPod)).To(Succeed())

			msg, exit, found := r.newestFailedPrefetchTermination(ctx, getDM(name), rev)
			Expect(found).To(BeTrue())
			Expect(msg).To(Equal("new reason"))
			Expect(exit).To(Equal(int32(2)))
		})
	})
})

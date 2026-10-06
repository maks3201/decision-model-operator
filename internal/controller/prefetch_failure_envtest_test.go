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
	// prefetch Pod whose first container terminated with the given message.
	failPrefetch := func(name, rev, termMsg string, exit int32) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: name + "-prefetch-" + rev + "-abc",
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:             name,
					decisionmodelv1alpha1.LabelPrefetchRevision: rev,
					"job-name": name + "-prefetch-" + rev,
				},
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
})

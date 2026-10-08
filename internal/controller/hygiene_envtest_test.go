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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("hygiene", func() {
	const model = "laya:en"

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	newR := func(eng *fakeEngine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}
	createReadyPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.40"
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
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
	markJobFailed := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(dmName, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	readyReason := func(name string) (string, metav1.ConditionStatus) {
		c := meta.FindStatusCondition(getDM(name).Status.Conditions, decisionmodelv1alpha1.ConditionReady)
		if c == nil {
			return "", ""
		}
		return c.Reason, c.Status
	}
	createDM := func(name string) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("hygiene-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// bumping the retry annotation triggers a reconcile that clears a
	// failed revision (verified via the reconcile effect; the predicate change is
	// what lets the manager deliver it).
	It("retry annotation clears the failed revision", func() {
		eng := newFakeEngine()
		r := newR(eng, &fakeProber{})
		createDM("h2")
		reconcileOnce(r, "h2")
		rev := RevisionHash(getDM("h2").Spec, defaultDigest, fakeImage)
		markJobFailed("h2", rev)
		reconcileOnce(r, "h2")
		Expect(getDM("h2").Status.FailedRevision).NotTo(BeNil())

		Expect(updateDM(ctx, namespace, "h2", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Annotations = map[string]string{decisionmodelv1alpha1.AnnotationRetry: "go"}
		})).To(Succeed())
		reconcileOnce(r, "h2")
		Expect(getDM("h2").Status.FailedRevision).To(BeNil())
		Expect(getDM("h2").Status.LastRetryToken).To(Equal("go"))
	})

	// a stable revision whose probe starts reporting DeviceMismatch must
	// leave Ready (honest readiness).
	It("stable DeviceMismatch leaves Ready", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}}
		r := newR(eng, prober)
		createDM("h3")

		reconcileOnce(r, "h3")
		rev := RevisionHash(getDM("h3").Spec, defaultDigest, fakeImage)
		markJobComplete("h3", rev)
		reconcileOnce(r, "h3")
		createReadyPod("h3", rev, "h3-pod-0")
		reconcileOnce(r, "h3") // probe -> gate True
		reconcileOnce(r, "h3") // promote
		Expect(getDM("h3").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
		reason, status := readyReason("h3")
		Expect(status).To(Equal(metav1.ConditionTrue))
		Expect(reason).To(Equal(reasonReady))

		// The runtime now reports the wrong device for the loaded model.
		prober.loaded.Device = "cuda"
		// Bump ContainersReady LTT so the gate-True Pod is re-probed.
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "h3-pod-0"}, pod)).To(Succeed())
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.ContainersReady {
				pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(time.Hour))
			}
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		reconcileOnce(r, "h3") // re-probe -> DeviceMismatch -> gate False -> 0 ready

		dm := getDM("h3")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))
		reason, status = readyReason("h3")
		Expect(status).To(Equal(metav1.ConditionFalse))
		Expect(reason).To(Equal(reasonNoModelReadyPods))
	})

	// the failed-revision guard path is authoritative — a stray phase write
	// is self-healed back to RolledBack, and no Job is (re)created.
	It("failed-revision guard restores RolledBack after a stray phase write", func() {
		eng := newFakeEngine()
		prober := &fakeProber{loaded: engine.Loaded{Name: model, Digest: defaultDigest, Device: "cpu"}}
		r := newR(eng, prober)
		createDM("h4")

		// Reach Ready on rev1 (the stable revision).
		reconcileOnce(r, "h4")
		rev1 := RevisionHash(getDM("h4").Spec, defaultDigest, fakeImage)
		markJobComplete("h4", rev1)
		reconcileOnce(r, "h4")
		createReadyPod("h4", rev1, "h4-pod-0")
		reconcileOnce(r, "h4")
		reconcileOnce(r, "h4")
		Expect(getDM("h4").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))

		// New revision whose prefetch fails -> RolledBack, failedRevision=rev2.
		eng.digest = "f0f0000000000000000000000000000000000000000000000000000000000000"
		Expect(updateDM(ctx, namespace, "h4", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())
		reconcileOnce(r, "h4")
		rev2 := RevisionHash(getDM("h4").Spec, eng.digest, fakeImage)
		markJobFailed("h4", rev2)
		reconcileOnce(r, "h4")
		Expect(getDM("h4").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(getDM("h4").Status.FailedRevision).NotTo(BeNil())

		// Simulate a stray/stale write leaving the DM in Caching with no candidate.
		Expect(updateDMStatus(ctx, namespace, "h4", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.Phase = decisionmodelv1alpha1.PhaseCaching
			meta.SetStatusCondition(&dm.Status.Conditions, metav1.Condition{
				Type: decisionmodelv1alpha1.ConditionCached, Status: metav1.ConditionFalse,
				Reason: "Caching", Message: "prefetching model into store",
			})
		})).To(Succeed())

		// Next reconcile: the guard path restores RolledBack and drops the stale
		// Cached condition; no prefetch Job is created for the failed revision.
		reconcileOnce(r, "h4")
		got := getDM("h4")
		Expect(got.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))
		Expect(meta.FindStatusCondition(got.Status.Conditions, decisionmodelv1alpha1.ConditionCached)).To(BeNil())
		deg := meta.FindStatusCondition(got.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Status).To(Equal(metav1.ConditionTrue))

		job := &batchv1.Job{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("h4", rev2)}, job)
		Expect(err).To(HaveOccurred(), "failed revision Job must not be recreated")
	})
})

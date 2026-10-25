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

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// A DecisionModel is a scale target: /scale serves spec.replicas, status.replicas.desired
// and status.selector; the selector names the stable revision's serving Pods only; and
// crossing 1 replica on a ReadWriteOnce store (which changes the co-location term in the
// Pod template under the Recreate strategy) is announced with a Warning Event.
var _ = Describe("DecisionModel as an autoscaler scale target", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }

	newR := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(256),
			// Scope the fleet-budget count to this test's namespace.
			WatchNamespaces: []string{namespace},
		}
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) decisionmodelv1alpha1.DecisionModelPhase {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return getDM(name).Status.Phase
	}
	createDM := func(name string, replicas int32) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(replicas),
			},
		})).To(Succeed())
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
	// gatedPod is a serving Pod of the revision whose readiness gate is True.
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
	// driveReady brings a DM to Ready through its first rollout.
	driveReady := func(r *DecisionModelReconciler, name, rev string) {
		rec(r, name)
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { return rec(r, name) }, "10s", "50ms").
			Should(Equal(decisionmodelv1alpha1.PhaseReady))
	}
	// driveToDeployment reconciles until the serving Deployment of the first revision exists.
	driveToDeployment := func(r *DecisionModelReconciler, name string) (string, *appsv1.Deployment) {
		rec(r, name)
		rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJob(name, rev)
		dep := &appsv1.Deployment{}
		Eventually(func() error {
			rec(r, name)
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)
		}, "5s", "50ms").Should(Succeed())
		return rev, dep
	}
	// drainEvents returns the recorded events and empties the buffer.
	drainEvents := func(r *DecisionModelReconciler) []string {
		fr := r.Recorder.(*events.FakeRecorder)
		var out []string
		for drained := false; !drained; {
			select {
			case e := <-fr.Events:
				out = append(out, e)
			default:
				drained = true
			}
		}
		return out
	}
	countReason := func(evs []string, reason string) int {
		n := 0
		for _, e := range evs {
			if strings.Contains(e, reason) {
				n++
			}
		}
		return n
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "autoscale-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("selects the stable revision's serving Pods only, and is empty before a stable exists", func() {
		r := newR()
		createDM("sel", 1)
		Expect(getDM("sel").Status.Selector).To(BeEmpty(), "no stable revision yet")

		rev := RevisionHash(getDM("sel").Spec, defaultDigest, fakeImage)
		driveReady(r, "sel", rev)
		dm := getDM("sel")
		Expect(dm.Status.Selector).To(Equal(labels.SelectorFromSet(revisionLabels(dm, rev)).String()))

		sel, err := labels.Parse(dm.Status.Selector)
		Expect(err).NotTo(HaveOccurred())
		Expect(sel.Matches(labels.Set{decisionmodelv1alpha1.LabelName: "sel", decisionmodelv1alpha1.LabelRevision: rev})).To(BeTrue(),
			"matches a stable serving Pod")
		Expect(sel.Matches(labels.Set{decisionmodelv1alpha1.LabelName: "sel"})).To(BeFalse(),
			"does not match a prefetch Pod (no revision label)")
		Expect(sel.Matches(labels.Set{decisionmodelv1alpha1.LabelName: "sel", decisionmodelv1alpha1.LabelRevision: "other"})).To(BeFalse(),
			"does not match a candidate Pod of another revision")
	})

	It("serves the scale subresource and rejects a scale write below 1", func() {
		createDM("scale", 1)
		scale := &autoscalingv1.Scale{}
		Expect(k8sClient.SubResource("scale").Get(ctx, getDM("scale"), scale)).To(Succeed())
		Expect(scale.Spec.Replicas).To(Equal(int32(1)))

		Expect(k8sClient.SubResource("scale").Update(ctx, getDM("scale"),
			client.WithSubResourceBody(&autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: 3}}))).To(Succeed())
		Expect(*getDM("scale").Spec.Replicas).To(Equal(int32(3)), "a scale write lands in spec.replicas")

		for _, bad := range []int32{0, -1} {
			err := k8sClient.SubResource("scale").Update(ctx, getDM("scale"),
				client.WithSubResourceBody(&autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: bad}}))
			Expect(err).To(HaveOccurred(), "replicas %d must be rejected", bad)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "replicas %d: %v", bad, err)
		}
		Expect(*getDM("scale").Spec.Replicas).To(Equal(int32(3)), "rejected writes change nothing")
	})

	It("announces 1 <-> N crossings on RWO with a Warning Event, and not the 2 <-> 3 steps", func() {
		r := newR()
		createDM("rwo", 1)
		rev, dep := driveToDeployment(r, "rwo")
		dm := getDM("rwo")
		Expect(dep.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType), "RWO store: Recreate")
		Expect(hasColocationTerm(dep.Spec.Template.Spec.Affinity, revisionLabels(dm, rev))).To(BeFalse())
		drainEvents(r)

		setReplicas := func(n int32) {
			Expect(updateDM(ctx, namespace, "rwo", func(d *decisionmodelv1alpha1.DecisionModel) { d.Spec.Replicas = int32Ptr(n) })).To(Succeed())
			rec(r, "rwo")
		}
		template := func() *appsv1.Deployment {
			d := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rwo-" + rev}, d)).To(Succeed())
			return d
		}

		setReplicas(3) // 1 -> 3: the co-location term enters the Pod template.
		Expect(hasColocationTerm(template().Spec.Template.Spec.Affinity, revisionLabels(getDM("rwo"), rev))).To(BeTrue())
		Expect(countReason(drainEvents(r), eventServingPodsRecreated)).To(Equal(1), "1 -> 3 announced once")
		withColocated := template().Spec.Template.Spec.DeepCopy()

		setReplicas(2) // 3 -> 2: both sides co-locate, the Pod template is unchanged, no crossing.
		Expect(template().Spec.Template.Spec).To(Equal(*withColocated), "2 <-> 3 leaves the Pod template unchanged")
		Expect(countReason(drainEvents(r), eventServingPodsRecreated)).To(Equal(0), "2 <-> 3 is not a crossing")

		setReplicas(1) // N -> 1: the term leaves the Pod template.
		Expect(hasColocationTerm(template().Spec.Template.Spec.Affinity, revisionLabels(getDM("rwo"), rev))).To(BeFalse())
		Expect(countReason(drainEvents(r), eventServingPodsRecreated)).To(Equal(1), "N -> 1 announced once")
		Expect(template().Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
	})

	// markJobFailed fails a prefetch Job so the candidate rolls back.
	markJobFailed := func(name, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName(name, rev)}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue},
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	// changeModel makes the next revision: a new model with its own digest.
	changeModel := func(r *DecisionModelReconciler, name, model, digest string) string {
		r.Engines["ollaya"].(*fakeEngine).digest = digest
		r.Prober = &fakeProber{loaded: engine.Loaded{Name: model, Digest: digest, Device: "cpu"}}
		Expect(updateDM(ctx, namespace, name, func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Model = model
		})).To(Succeed())
		return RevisionHash(getDM(name).Spec, digest, fakeImage)
	}
	// scaleViaSubresource writes spec.replicas through the scale subresource.
	scaleViaSubresource := func(r *DecisionModelReconciler, name string, n int32) {
		Expect(k8sClient.SubResource("scale").Update(ctx, getDM(name),
			client.WithSubResourceBody(&autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: n}}))).To(Succeed())
		rec(r, name)
	}
	// expectInPlace checks that a /scale write changed only the stable Deployment's replica count.
	expectInPlace := func(name, rev string, n int32) {
		dm := getDM(name)
		Expect(*dm.Spec.Replicas).To(Equal(n))
		Expect(dm.Status.CandidateRevision).To(BeNil(), "no candidate revision is started")
		Expect(dm.Status.StableRevision).NotTo(BeNil())
		Expect(dm.Status.StableRevision.Hash).To(Equal(rev), "stable revision unchanged")
		Expect(dm.Status.Replicas.Desired).To(Equal(n), "the field the scale subresource reads")

		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		Expect(*dep.Spec.Replicas).To(Equal(n))
		deps := &appsv1.DeploymentList{}
		Expect(k8sClient.List(ctx, deps, client.InNamespace(namespace))).To(Succeed())
		Expect(deps.Items).To(HaveLen(1), "no new Deployment")
	}

	It("keeps the selector on the new stable after promotion", func() {
		const digestB = "b2b2220000000000000000000000000000000000000000000000000000000000"
		r := newR()
		createDM("promo", 1)
		revA := RevisionHash(getDM("promo").Spec, defaultDigest, fakeImage)
		driveReady(r, "promo", revA)

		revB := changeModel(r, "promo", "kev:en", digestB)
		Expect(revB).NotTo(Equal(revA))
		driveReady(r, "promo", revB)

		dm := getDM("promo")
		Expect(dm.Status.StableRevision.Hash).To(Equal(revB))
		Expect(dm.Status.Selector).To(Equal(labels.SelectorFromSet(revisionLabels(dm, revB)).String()))
		sel, err := labels.Parse(dm.Status.Selector)
		Expect(err).NotTo(HaveOccurred())
		Expect(sel.Matches(labels.Set{decisionmodelv1alpha1.LabelName: "promo", decisionmodelv1alpha1.LabelRevision: revA})).To(BeFalse(),
			"the old stable's Pods are no longer selected")
	})

	It("keeps the selector on the stable after a candidate rollback", func() {
		const digestB = "b2b2220000000000000000000000000000000000000000000000000000000000"
		r := newR()
		createDM("rb", 1)
		revA := RevisionHash(getDM("rb").Spec, defaultDigest, fakeImage)
		driveReady(r, "rb", revA)
		selA := getDM("rb").Status.Selector

		revB := changeModel(r, "rb", "kev:en", digestB)
		rec(r, "rb")
		markJobFailed("rb", revB)
		Expect(rec(r, "rb")).To(Equal(decisionmodelv1alpha1.PhaseRolledBack))

		dm := getDM("rb")
		Expect(dm.Status.StableRevision.Hash).To(Equal(revA), "stable keeps serving")
		Expect(dm.Status.Selector).To(Equal(selA))
		Expect(dm.Status.Selector).To(Equal(labels.SelectorFromSet(revisionLabels(dm, revA)).String()))
	})

	It("applies a /scale write on a Ready model as an in-place update", func() {
		r := newR()
		createDM("inplace", 1)
		rev := RevisionHash(getDM("inplace").Spec, defaultDigest, fakeImage)
		driveReady(r, "inplace", rev)

		scaleViaSubresource(r, "inplace", 3)
		expectInPlace("inplace", rev, 3)
	})

	It("scales a Ready model back down from 3 to 2 in place", func() {
		r := newR()
		createDM("down", 1)
		rev := RevisionHash(getDM("down").Spec, defaultDigest, fakeImage)
		driveReady(r, "down", rev)

		scaleViaSubresource(r, "down", 3)
		expectInPlace("down", rev, 3)

		scaleViaSubresource(r, "down", 2)
		expectInPlace("down", rev, 2)
	})
})

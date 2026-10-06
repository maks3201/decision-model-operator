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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// revProber reports a per-revision Loaded result, so a test can make the stable
// revision's Pod lose the model while the candidate stays healthy.
type revProber struct {
	mu       sync.Mutex
	byRev    map[string]engine.Loaded
	fallback engine.Loaded
}

func (p *revProber) set(rev string, l engine.Loaded) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.byRev == nil {
		p.byRev = map[string]engine.Loaded{}
	}
	p.byRev[rev] = l
}
func (p *revProber) Probe(_ context.Context, pod *corev1.Pod, _ engine.Engine, _, _ string) (engine.Loaded, error) {
	return p.lookup(pod)
}

// Reinspect implements the optional reinspector capability so the periodic regate
// loop re-checks already-True Pods per revision.
func (p *revProber) Reinspect(_ context.Context, pod *corev1.Pod, _ engine.Engine, _, _ string) (engine.Loaded, error) {
	return p.lookup(pod)
}

func (p *revProber) lookup(pod *corev1.Pod) (engine.Loaded, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rev := pod.Labels[decisionmodelv1alpha1.LabelRevision]
	if l, ok := p.byRev[rev]; ok {
		l.Pinned = true
		return l, nil
	}
	f := p.fallback
	f.Pinned = true
	return f, nil
}

// the stable revision is maintained while a candidate is mid-rollout —
// its Deployment is restored if deleted, and its Pods are regated.
var _ = Describe("stable maintenance during a rollout", func() {
	const digestB = "b0b0290000000000000000000000000000000000000000000000000000000000"
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
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
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
		p.Status.PodIP = "10.0.0.137"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	podGate := func(pod string) *corev1.PodCondition {
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pod}, p)).To(Succeed())
		for i := range p.Status.Conditions {
			if string(p.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
				return &p.Status.Conditions[i]
			}
		}
		return nil
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "stablemaint-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Drive a manual-promotion DM to a stable revision A, then start candidate B
	// and park it in AwaitingPromotion. Returns (reconciler, prober, clock, revA, revB).
	setup := func(name string) (*DecisionModelReconciler, *revProber, *safeClock, string, string) {
		fake := newFakeEngine()
		pr := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		clk := newSafeClock()
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": fake}, Prober: pr,
			Recorder: events.NewFakeRecorder(64), Now: clk.now,
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
				Rollout: &decisionmodelv1alpha1.RolloutSpec{Promotion: decisionmodelv1alpha1.PromotionManual},
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

		// Candidate B (new digest) -> park in AwaitingPromotion.
		fake.mu.Lock()
		fake.digest = digestB
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Model = "kev:en" })).To(Succeed())
		rec()
		revB := getDM(name).Status.CandidateRevision.Hash
		pr.set(revB, engine.Loaded{Name: "kev:en", Digest: digestB, Device: "cpu"})
		markJobComplete(name, revB)
		rec()
		mkGatedPod(name, revB, name+"-b")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
		return r, pr, clk, revA, revB
	}

	It("restores a deleted stable Deployment while the candidate is parked", func() {
		r, _, _, revA, _ := setup("maint")
		depKey := types.NamespacedName{Namespace: namespace, Name: "maint-" + revA}
		Expect(k8sClient.Get(ctx, depKey, &appsv1.Deployment{})).To(Succeed())

		// Delete the stable Deployment out of band.
		Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "maint-" + revA}})).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, depKey, &appsv1.Deployment{}))
		}, "5s", "50ms").Should(BeTrue())

		// A reconcile while the candidate is parked must restore the stable Deployment.
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "maint"}})
		Expect(k8sClient.Get(ctx, depKey, &appsv1.Deployment{})).
			To(Succeed(), "stable Deployment recreated during the rollout")
		// Still parked (candidate not promoted by maintenance).
		Expect(getDM("maint").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseAwaitingPromotion))
	})

	It("regates the stable Pod to False when it loses the model during AwaitingPromotion", func() {
		r, pr, clk, revA, _ := setup("regate")
		Expect(podGate("regate-a").Status).To(Equal(corev1.ConditionTrue))

		// The stable Pod loses the model (digest mismatch). The periodic re-inspect
		// fires once regateInterval has elapsed; maintenance must then flip the
		// stable Pod's gate False even though the candidate is parked.
		pr.set(revA, engine.Loaded{Name: "laya:en", Digest: "deadbeefdeadbeef0000000000000000000000000000000000000000deadbeef", Device: "cpu"})
		clk.add(regateInterval + time.Second)
		Eventually(func() corev1.ConditionStatus {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "regate"}})
			g := podGate("regate-a")
			if g == nil {
				return corev1.ConditionUnknown
			}
			return g.Status
		}, "5s", "20ms").Should(Equal(corev1.ConditionFalse), "stable Pod gate flips False on model loss")
	})
})

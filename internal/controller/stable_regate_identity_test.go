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
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// argProber mimics the PRODUCTION prober: it answers each probe from what the Pod
// actually serves, which it looks up by the MODEL STRING the controller passes to
// Probe/Reinspect (exactly as the real engineProber uses that string to query the
// Pod's /api/ps and build the expectation). A controller that probes the stable
// against the recorded stable model gets the stable's Loaded back (match); a
// controller that passes a different (candidate/spec) model gets whatever that
// model maps to — or an error for a model that does not resolve. This is the fake
// the existing revProber is not: revProber ignores the model string and always
// returns the Pod's own recorded data, so it can never catch "the stable was
// probed against a candidate expectation".
type argProber struct {
	mu    sync.Mutex
	byArg map[string]engine.Loaded // model string -> what a Pod serving it reports
	errAt map[string]error         // model string -> probe error (e.g. not loadable)
	asked []string                 // every model string the controller probed with
}

func (p *argProber) set(model string, l engine.Loaded) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.byArg == nil {
		p.byArg = map[string]engine.Loaded{}
	}
	l.Pinned = true
	p.byArg[model] = l
}

func (p *argProber) fail(model string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.errAt == nil {
		p.errAt = map[string]error{}
	}
	p.errAt[model] = err
}

func (p *argProber) answer(model string) (engine.Loaded, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, model)
	if err, ok := p.errAt[model]; ok {
		return engine.Loaded{}, err
	}
	if l, ok := p.byArg[model]; ok {
		return l, nil
	}
	return engine.Loaded{}, fmt.Errorf("argProber: no answer for model %q", model)
}

func (p *argProber) askedModels() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.asked...)
}

func (p *argProber) Probe(_ context.Context, _ *corev1.Pod, _ engine.Engine, _, model string) (engine.Loaded, error) {
	return p.answer(model)
}

func (p *argProber) Reinspect(_ context.Context, _ *corev1.Pod, _ engine.Engine, _, model string) (engine.Loaded, error) {
	return p.answer(model)
}

// This suite reproduces the P0 measured on kind: a Ready
// single-replica DecisionModel whose spec.model is changed to a tag that does not
// resolve must keep its SERVING stable Pod gated True and in the Service. The gate
// must be decided from the stable's recorded identity (status.stableRevision),
// never from the live (now-broken) spec. It uses argProber so the stable Pod is
// only "model-ready" if the controller probes it against the stable's own model.
var _ = Describe("the serving stable gate survives a failing candidate resolve", func() {
	const stableModel = "laya:en"
	var (
		ctx       context.Context
		namespace string
		nsCounter int
		clk       time.Time
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
		p.Status.PodIP = "10.0.0.151"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	podGateTrue := func(pod string) bool {
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pod}, p)).To(Succeed())
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("regate-identity-%d", nsCounter)
		clk = time.Now()
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// drive a single-replica DM to Ready with an argProber, returning (r, fake, pr, revA).
	driveReady := func(name string) (*DecisionModelReconciler, *fakeEngine, *argProber, string) {
		fake := newFakeEngine()
		pr := &argProber{}
		pr.set(stableModel, engine.Loaded{Name: stableModel, Digest: defaultDigest, Device: "cpu"})
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": fake},
			Prober:   pr,
			Recorder: events.NewFakeRecorder(64),
			Now:      func() time.Time { return clk },
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: stableModel, Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		}
		rec()
		revA := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		markJobComplete(name, revA)
		rec()
		mkGatedPod(name, revA, name+"-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM(name).Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return r, fake, pr, revA
	}

	// assertStableGateSurvives runs the common P0 assertions after a candidate
	// failure is injected: the single stable Pod stays gated True and counted
	// model-ready, the DM stays Ready, the stable Deployment is not rolled, and the
	// prober is never asked about badModel (the broken live spec). It advances the
	// clock past the regate interval first, so the already-True Pod is re-inspected
	// (the window measured on kind).
	assertStableGateSurvives := func(r *DecisionModelReconciler, pr *argProber, name, revA, badModel string) {
		depBefore := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + revA}, depBefore)).To(Succeed())
		tmplBefore := depBefore.Spec.Template.DeepCopy()
		if badModel != "" {
			pr.fail(badModel, fmt.Errorf("model not loaded"))
		}

		clk = clk.Add(2 * regateInterval)
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})

		Expect(podGateTrue(name+"-a")).To(BeTrue(),
			"the serving stable Pod must stay model-ready when only the candidate fails")
		dm := getDM(name)
		Expect(dm.Status.Replicas.ModelReady).To(Equal(int32(1)), "the stable replica stays model-ready")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady), "the DM stays Ready")
		Expect(meta_Find(dm, decisionmodelv1alpha1.ConditionReady).Status).To(Equal(metav1.ConditionTrue),
			"Ready stays True")
		if badModel != "" {
			Expect(pr.askedModels()).NotTo(ContainElement(badModel),
				"the stable must be probed from its recorded identity, never the live spec model")
		}
		depAfter := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + revA}, depAfter)).To(Succeed())
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore),
			"the stable Pod template must be unchanged (no roll) when only the candidate fails")
	}

	It("keeps the stable Pod gated and Ready when spec.model is changed to an unresolvable tag", func() {
		r, fake, pr, revA := driveReady("flap")
		fake.mu.Lock()
		fake.resolveErr = engine.ErrNotFound
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "flap", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:does-not-exist"
		})).To(Succeed())
		assertStableGateSurvives(r, pr, "flap", revA, "laya:does-not-exist")
	})

	It("keeps the stable gated and Ready on a transient resolve error (registry 5xx)", func() {
		r, fake, pr, revA := driveReady("trans")
		fake.mu.Lock()
		fake.resolveErr = fmt.Errorf("registry 503 service unavailable")
		fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, "trans", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:flaky"
		})).To(Succeed())
		assertStableGateSurvives(r, pr, "trans", revA, "laya:flaky")
	})

	It("keeps the stable gated and Ready when the new spec.runtimeVersion is invalid", func() {
		r, _, pr, revA := driveReady("rtver")
		Expect(updateDM(ctx, namespace, "rtver", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.RuntimeVersion = "0.0.1" // below the engine minimum -> invalid
		})).To(Succeed())
		// runtimeVersion does not change the model string, so no badModel to fail;
		// the assertion still proves the stable stays gated and is not rolled.
		assertStableGateSurvives(r, pr, "rtver", revA, "")
	})

	It("keeps the stable gated and Ready when the API-key Secret loses its opt-in label", func() {
		fake := newFakeEngine()
		pr := &argProber{}
		pr.set(stableModel, engine.Loaded{Name: stableModel, Digest: defaultDigest, Device: "cpu"})
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": fake}, Prober: pr,
			Recorder: events.NewFakeRecorder(64), Now: func() time.Time { return clk },
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "key", Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"}},
			Data:       map[string][]byte{"token": []byte("s3cr3t")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "sec"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: stableModel, Device: "cpu", Replicas: int32Ptr(1),
				Auth: &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "key"}, Key: "token",
				}},
			},
		})).To(Succeed())
		rec := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "sec"}})
		}
		rec()
		revA := RevisionHash(getDM("sec").Spec, defaultDigest, fakeImage)
		markJobComplete("sec", revA)
		rec()
		mkGatedPod("sec", revA, "sec-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec(); return getDM("sec").Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// Drop the opt-in label: the key becomes unreadable (confused-deputy guard).
		Expect(func() error {
			s := &corev1.Secret{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "key"}, s); err != nil {
				return err
			}
			s.Labels = map[string]string{}
			return k8sClient.Update(ctx, s)
		}()).To(Succeed())
		assertStableGateSurvives(r, pr, "sec", revA, "")
	})

	It("keeps the stable gated from recorded identity while a candidate is RolloutQueued", func() {
		// Occupy the only rollout slot with a holder DM, so a second DM's valid
		// candidate (a different model) queues; its stable must keep being probed
		// against its OWN recorded model, never the queued candidate's model.
		fake := newFakeEngine()
		pr := &argProber{}
		pr.set(stableModel, engine.Loaded{Name: stableModel, Digest: defaultDigest, Device: "cpu"})
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": fake}, Prober: pr,
			Recorder: events.NewFakeRecorder(64), Now: func() time.Time { return clk },
			// Drive "q" to Ready with the budget disabled so its first rollout is
			// never queued behind another spec's in-flight rollout sharing the
			// envtest apiserver (the budget List is cluster-wide). The budget is
			// enabled below, only for the holder+queue phase this spec is about.
			MaxConcurrentRollouts: 0,
		}
		rec := func(n string) {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: n}})
		}
		mk := func(n string) {
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: n},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: stableModel, Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
		}
		// Drive "q" to Ready (its stable serves stableModel).
		mk("q")
		rec("q")
		revQ := RevisionHash(getDM("q").Spec, defaultDigest, fakeImage)
		// Reconcile until the prefetch Job exists, then complete it in the same
		// Eventually so the test never reads it before the controller created it
		// (CI flake #116: the Job can be created a reconcile after the candidate is
		// first recorded). A transient NotFound just retries.
		Eventually(func() error {
			rec("q")
			job := &batchv1.Job{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("q", revQ)}, job); err != nil {
				return err
			}
			now := metav1.Now()
			job.Status.StartTime, job.Status.CompletionTime = &now, &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			}
			return k8sClient.Status().Update(ctx, job)
		}, "5s", "20ms").Should(Succeed())
		rec("q")
		mkGatedPod("q", revQ, "q-a")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase { rec("q"); return getDM("q").Status.Phase },
			"5s", "20ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		// A holder DM takes the single rollout slot (records a candidate).
		r.MaxConcurrentRollouts = 1
		mk("holder")
		rec("holder")
		pr.set("kev:en", engine.Loaded{Name: "kev:en", Digest: defaultDigest, Device: "cpu"})

		// Now q asks for a new candidate (different model) -> it must queue behind
		// the holder's slot. The queued-path stable maintenance must probe q's stable
		// against q's OWN recorded model (stableModel), never the queued candidate's
		// model (kev:en).
		Expect(updateDM(ctx, namespace, "q", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "kev:en"
		})).To(Succeed())

		clk = clk.Add(2 * regateInterval)
		// Reset the record of probed models so we only assert on this reconcile.
		pr.mu.Lock()
		pr.asked = nil
		pr.mu.Unlock()
		rec("q")

		// q's stable Pod must stay model-ready, probed from q's recorded model only.
		Expect(podGateTrue("q-a")).To(BeTrue(), "the queued DM's stable Pod stays model-ready")
		Expect(getDM("q").Status.Replicas.ModelReady).To(Equal(int32(1)))
		// The stable (revQ) was only ever probed against stableModel this reconcile.
		for _, m := range pr.askedModels() {
			Expect(m).To(Equal(stableModel),
				"a queued candidate must never cause the stable to be probed against another model")
		}
	})
})

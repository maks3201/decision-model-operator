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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
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

// ---------------------------------------------------------------- unit tests

// evalPolicyHash must fold the EFFECTIVE score tolerance into the policy
// identity in canonical form: unset, "0.5" and "0.50" are the same policy;
// a real change ("1") is a different policy (so a parked result/approval is
// stale and the candidate re-evaluates).
func TestEvalPolicyHashScoreTolerance(t *testing.T) {
	base := func(tol string) *decisionmodelv1alpha1.EvaluationSpec {
		return &decisionmodelv1alpha1.EvaluationSpec{
			DatasetRef:     decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
			MinAccuracy:    "0.90",
			ScoreTolerance: tol,
		}
	}
	unset := evalPolicyHash(base(""))
	for _, same := range []string{"0.5", "0.50", "0.500"} {
		if got := evalPolicyHash(base(same)); got != unset {
			t.Errorf("tolerance %q: hash %q != unset-default hash %q (effective default must match)", same, got, unset)
		}
	}
	for _, diff := range []string{"1", "0.25", "0.75"} {
		if got := evalPolicyHash(base(diff)); got == unset {
			t.Errorf("tolerance %q: hash equals the default; a real change must shift the policy hash", diff)
		}
	}
	// An invalid/negative tolerance falls back to the default, so it hashes as unset.
	for _, bad := range []string{"-1", "abc"} {
		if got := evalPolicyHash(base(bad)); got != unset {
			t.Errorf("tolerance %q: invalid value must fall back to the default hash", bad)
		}
	}
}

// Two eval cache keys that differ only by the score tolerance must not share a
// result: a result computed under one tolerance is not valid under another.
func TestEvalKeyToleranceSeparatesCacheEntries(t *testing.T) {
	s := newEvalStore()
	k1 := evalKey{"ns", "n", "rev", "dshash", 0, 0.5}
	k2 := k1
	k2.tolerance = 0.75
	s.start(k1, func() {})
	s.finish(k1, evalResult{done: true, accuracy: 0.9})
	if _, ok := s.get(k2); ok {
		t.Fatal("a key differing only by tolerance must not hit the cached result")
	}
	if _, ok := s.get(k1); !ok {
		t.Fatal("the original key must still resolve its own result")
	}
}

// forgetMismatched cancels and drops an in-flight evaluation for the same
// revision whose dataset/maxCases/tolerance differs from the current key (an
// in-place dataset edit or tolerance change mid-Evaluating), and keeps the
// current entry.
func TestForgetMismatchedCancelsStaleSameRevisionRun(t *testing.T) {
	s := newEvalStore()
	cur := evalKey{"ns", "n", "rev", "ds-new", 0, 0.5}
	stale := cur
	stale.dataset = "ds-old"
	other := evalKey{"ns", "n", "rev2", "ds-old", 0, 0.5} // different revision: untouched
	cancelled := false
	s.start(stale, func() { cancelled = true })
	s.start(cur, func() {})
	s.start(other, func() {})

	s.forgetMismatched(cur)

	if !cancelled {
		t.Error("the stale same-revision run was not cancelled")
	}
	if _, ok := s.get(stale); ok {
		t.Error("the stale entry was not dropped")
	}
	if _, ok := s.get(cur); !ok {
		t.Error("the current entry must be kept")
	}
	if _, ok := s.get(other); !ok {
		t.Error("a different revision's entry must be untouched")
	}
}

func TestApplyCUDAArch(t *testing.T) {
	archIn := func(key string) *corev1.Affinity {
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{"arm64"}}},
				}},
			},
		}}
	}
	tests := []struct {
		name     string
		device   string
		selector map[string]string
		aff      *corev1.Affinity
		want     map[string]string
	}{
		{"cpu untouched", engine.DeviceCPU, nil, nil, nil},
		{"cuda adds amd64", engine.DeviceCUDA, nil, nil, map[string]string{archLabel: "amd64"}},
		{"cuda keeps other selectors", engine.DeviceCUDA, map[string]string{"pool": "gpu"}, nil,
			map[string]string{"pool": "gpu", archLabel: "amd64"}},
		{"user arch selector wins", engine.DeviceCUDA, map[string]string{archLabel: "arm64"}, nil,
			map[string]string{archLabel: "arm64"}},
		{"required affinity on arch wins", engine.DeviceCUDA, nil, archIn(archLabel), nil},
		{"affinity on another key does not", engine.DeviceCUDA, nil, archIn("topology.kubernetes.io/zone"),
			map[string]string{archLabel: "amd64"}},
		{"preferred affinity on arch wins", engine.DeviceCUDA, nil, &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
				Weight: 1,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: archLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{"arm64"}}},
				},
			}},
		}}, nil},
		{"matchFields on arch wins", engine.DeviceCUDA, nil, &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchFields: []corev1.NodeSelectorRequirement{{Key: archLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{"arm64"}}},
			}}},
		}}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := map[string]string(nil)
			if tt.selector != nil {
				input = map[string]string{}
				for k, v := range tt.selector {
					input[k] = v
				}
			}
			spec := &corev1.PodSpec{NodeSelector: input, Affinity: tt.aff}
			applyCUDAArch(spec, tt.device)
			if len(spec.NodeSelector) != len(tt.want) {
				t.Fatalf("nodeSelector = %v, want %v", spec.NodeSelector, tt.want)
			}
			for k, v := range tt.want {
				if spec.NodeSelector[k] != v {
					t.Errorf("nodeSelector[%s] = %q, want %q", k, spec.NodeSelector[k], v)
				}
			}
			// The caller's map (aliased from the DecisionModel spec) is never mutated.
			if _, leaked := input[archLabel]; leaked && tt.selector[archLabel] == "" {
				t.Errorf("input selector map was mutated: %v", input)
			}
		})
	}
}

// applyGPUToleration must not write into the backing array of a slice that
// aliases the DecisionModel's own spec.scheduling.tolerations.
func TestApplyGPUTolerationDoesNotMutateInput(t *testing.T) {
	backing := make([]corev1.Toleration, 1, 4) // spare capacity would be appended into
	backing[0] = corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpExists}
	spec := &corev1.PodSpec{Tolerations: backing}
	applyGPUToleration(spec, engine.DeviceCUDA)
	if len(spec.Tolerations) != 2 {
		t.Fatalf("tolerations = %v, want 2 entries", spec.Tolerations)
	}
	if got := backing[:cap(backing)][1]; got.Key != "" {
		t.Errorf("append wrote into the caller's backing array: %v", got)
	}
}

func TestEffectiveTimeouts(t *testing.T) {
	d := func(v time.Duration) *metav1.Duration { return &metav1.Duration{Duration: v} }
	withTimeouts := func(to *decisionmodelv1alpha1.RolloutTimeouts) *decisionmodelv1alpha1.DecisionModel {
		return &decisionmodelv1alpha1.DecisionModel{Spec: decisionmodelv1alpha1.DecisionModelSpec{
			Rollout: &decisionmodelv1alpha1.RolloutSpec{Timeouts: to}}}
	}
	tests := []struct {
		name                      string
		dm                        *decisionmodelv1alpha1.DecisionModel
		caching, starting, evalTO time.Duration
		warmup                    time.Duration
	}{
		{"no rollout", &decisionmodelv1alpha1.DecisionModel{}, cacheTimeout, startTimeout, evalDeadline, warmupBaseTimeout},
		{"rollout without timeouts", withTimeouts(nil), cacheTimeout, startTimeout, evalDeadline, warmupBaseTimeout},
		{"empty timeouts", withTimeouts(&decisionmodelv1alpha1.RolloutTimeouts{}), cacheTimeout, startTimeout, evalDeadline, warmupBaseTimeout},
		{"all set", withTimeouts(&decisionmodelv1alpha1.RolloutTimeouts{Caching: d(3 * time.Hour), Starting: d(40 * time.Minute), Evaluating: d(20 * time.Minute)}),
			3 * time.Hour, 40 * time.Minute, 20 * time.Minute, 40 * time.Minute},
		{"only caching", withTimeouts(&decisionmodelv1alpha1.RolloutTimeouts{Caching: d(time.Hour)}),
			time.Hour, startTimeout, evalDeadline, warmupBaseTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cachingTimeout(tt.dm); got != tt.caching {
				t.Errorf("caching = %v, want %v", got, tt.caching)
			}
			if got := startingTimeout(tt.dm); got != tt.starting {
				t.Errorf("starting = %v, want %v", got, tt.starting)
			}
			if got := evaluatingTimeout(tt.dm); got != tt.evalTO {
				t.Errorf("evaluating = %v, want %v", got, tt.evalTO)
			}
			if got := warmupTimeoutFrom(withWarmupTimeout(context.Background(), tt.dm)); got != tt.warmup {
				t.Errorf("warmup = %v, want %v", got, tt.warmup)
			}
		})
	}
}

func TestRegateDueAndMark(t *testing.T) {
	r := &DecisionModelReconciler{}
	t0 := time.Now()
	if !r.regateDue("a", t0) {
		t.Error("a Pod never seen (e.g. after an operator restart) must be due at once")
	}
	r.regateMark("a", t0)
	if r.regateDue("a", t0.Add(regateInterval-time.Second)) {
		t.Error("not due inside the interval")
	}
	if !r.regateDue("a", t0.Add(regateInterval)) {
		t.Error("due once the interval elapsed")
	}
	// Entries of Pods that disappeared are swept so the map stays bounded.
	r.regateMark("gone", t0)
	later := t0.Add(regateInterval*regateSweepFactor + time.Second)
	r.regateMark("alive", later)
	r.regateMu.Lock()
	_, goneKept := r.regate.last["gone"]
	_, aliveKept := r.regate.last["alive"]
	r.regateMu.Unlock()
	if goneKept || !aliveKept {
		t.Errorf("sweep: gone kept=%v alive kept=%v, want false/true", goneKept, aliveKept)
	}
}

// warmCountingEngine records Warmup calls and the deadline each one received.
type warmCountingEngine struct {
	proberFakeEngine
	mu        sync.Mutex
	warmups   int
	remaining []time.Duration
}

func (e *warmCountingEngine) Warmup(ctx context.Context, _, _, _ string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.warmups++
	if dl, ok := ctx.Deadline(); ok {
		e.remaining = append(e.remaining, time.Until(dl))
	}
	return nil
}

func (e *warmCountingEngine) warmupCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.warmups
}

func TestReinspectForgetsWarmStateOnlyWhenModelIsGone(t *testing.T) {
	eng := &warmCountingEngine{proberFakeEngine: proberFakeEngine{loaded: []engine.Loaded{
		{Name: "laya:en", Digest: "d1", Device: "cpu"}}}}
	p := NewProber(nil).(*engineProber)
	pod := readyPod()
	pod.UID = "uid-1"

	if _, err := probeUntilReady(p, pod, eng, "laya:en"); err != nil {
		t.Fatalf("initial probe: %v", err)
	}
	if eng.warmupCount() != 1 {
		t.Fatalf("warmups after first probe = %d, want 1", eng.warmupCount())
	}

	// Still loaded: Inspect only, no new warmup, warm state kept.
	if _, err := p.Reinspect(context.Background(), pod, eng, "", "laya:en"); err != nil {
		t.Fatalf("Reinspect with model present: %v", err)
	}
	if _, err := p.Probe(context.Background(), pod, eng, "", "laya:en"); err != nil {
		t.Fatalf("Probe after Reinspect: %v", err)
	}
	if eng.warmupCount() != 1 {
		t.Errorf("Reinspect must not warm up; warmups = %d, want 1", eng.warmupCount())
	}

	// The model vanished: Reinspect fails and the warm state is dropped, so the
	// next Probe re-runs the normal warmup.
	eng.loaded = nil
	if _, err := p.Reinspect(context.Background(), pod, eng, "", "laya:en"); err == nil {
		t.Fatal("Reinspect with the model gone must fail")
	}
	eng.loaded = []engine.Loaded{{Name: "laya:en", Digest: "d1", Device: "cpu"}}
	if _, err := probeUntilReady(p, pod, eng, "laya:en"); err != nil {
		t.Fatalf("probe after reload: %v", err)
	}
	if eng.warmupCount() != 2 {
		t.Errorf("warmups after the model vanished = %d, want 2 (re-warm)", eng.warmupCount())
	}
}

func TestWarmupUsesStartingTimeout(t *testing.T) {
	eng := &warmCountingEngine{proberFakeEngine: proberFakeEngine{loaded: []engine.Loaded{
		{Name: "laya:en", Digest: "d1", Device: "cpu"}}}}
	dm := &decisionmodelv1alpha1.DecisionModel{Spec: decisionmodelv1alpha1.DecisionModelSpec{
		Rollout: &decisionmodelv1alpha1.RolloutSpec{Timeouts: &decisionmodelv1alpha1.RolloutTimeouts{
			Starting: &metav1.Duration{Duration: 45 * time.Minute}}}}}
	p := NewProber(nil)
	pod := readyPod()
	pod.UID = "uid-2"
	ctx := withWarmupTimeout(context.Background(), dm)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := p.Probe(ctx, pod, eng, "", "laya:en")
		if err == nil {
			break
		}
		if !errors.Is(err, errWarmupInProgress) || time.Now().After(deadline) {
			t.Fatalf("probe: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.remaining) != 1 || eng.remaining[0] < 44*time.Minute || eng.remaining[0] > 45*time.Minute {
		t.Errorf("warmup deadline remaining = %v, want ~45m (the starting timeout, not the 2m default)", eng.remaining)
	}
}

// reinspectProber is a fakeProber that also supports periodic re-inspection, so
// only the specs below exercise the re-check path. missing makes the model
// disappear from Reinspect.
type reinspectProber struct {
	*fakeProber
	missing   atomic.Bool
	authErr   atomic.Bool
	reinspect atomic.Int32
}

func (p *reinspectProber) Reinspect(_ context.Context, _ *corev1.Pod, _ engine.Engine, _, _ string) (engine.Loaded, error) {
	p.reinspect.Add(1)
	if p.authErr.Load() {
		// A transport/auth error (e.g. a 401 during an API-key rotation): transient.
		return engine.Loaded{}, fmt.Errorf("401 unauthorized")
	}
	if p.missing.Load() {
		return engine.Loaded{}, fmt.Errorf("%w (fake)", errModelNotLoaded)
	}
	return p.result(), nil
}

// ------------------------------------------------------------ envtest specs

var _ = Describe("robustness", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
		clock     *safeClock
	)

	int32Ptr := func(v int32) *int32 { return &v }
	dur := func(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

	newR := func(eng engine.Engine, p Prober) *DecisionModelReconciler {
		clock = newSafeClock()
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   p,
			Recorder: events.NewFakeRecorder(256),
			Now:      clock.now,
		}
	}

	reconcileRes := func(r *DecisionModelReconciler, name string) (reconcile.Result, error) {
		return r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := reconcileRes(r, name)
		Expect(err).NotTo(HaveOccurred())
	}
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	getPod := func(name string) *corev1.Pod {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, pod)).To(Succeed())
		return pod
	}
	gateOf := func(pod *corev1.Pod) *corev1.PodCondition {
		for i := range pod.Status.Conditions {
			if string(pod.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
				return &pod.Status.Conditions[i]
			}
		}
		return nil
	}
	markJobComplete := func(dmName, rev string) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}
	createGatedPod := func(dmName, rev, podName string) {
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
		pod.Status.PodIP = "10.0.0.60"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	newDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}
	phaseOf := func(name string) decisionmodelv1alpha1.DecisionModelPhase { return getDM(name).Status.Phase }

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("robust-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// ------------------------------------------------------------- item 1
	Describe("re-inspecting gated Pods", func() {
		loaded := engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}

		// stableWithPod drives a DM to Ready with one gated Pod and a
		// re-inspecting prober, then settles the re-check clock.
		stableWithPod := func(name string) (*DecisionModelReconciler, *reinspectProber, string) {
			p := &reinspectProber{fakeProber: &fakeProber{loaded: loaded}}
			r := newR(newFakeEngine(), p)
			newDM(name, nil)
			reconcileOnce(r, name)
			rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
			markJobComplete(name, rev)
			reconcileOnce(r, name)
			createGatedPod(name, rev, name+"-pod-0")
			Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
				_, _ = reconcileRes(r, name)
				return phaseOf(name)
			}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
			reconcileOnce(r, name) // stable path; the Pod's clock starts here
			return r, p, rev
		}

		It("does nothing while the model is still loaded: no Pod patch, no status write, requeue at the interval", func() {
			r, p, _ := stableWithPod("rg1")
			podRV := getPod("rg1-pod-0").ResourceVersion
			dmRV := getDM("rg1").ResourceVersion
			calls := p.reinspect.Load()

			// Inside the interval: no Inspect at all.
			clock.add(regateInterval / 2)
			res, err := reconcileRes(r, "rg1")
			Expect(err).NotTo(HaveOccurred())
			Expect(p.reinspect.Load()).To(Equal(calls), "no re-inspect inside the interval")
			Expect(res.RequeueAfter).To(Equal(regateInterval), "a Ready DM keeps coming back to re-inspect")

			// Interval elapsed: exactly one Inspect, nothing written.
			clock.add(regateInterval)
			reconcileOnce(r, "rg1")
			Expect(p.reinspect.Load()).To(Equal(calls+1), "one re-inspect once the interval elapsed")
			Expect(getPod("rg1-pod-0").ResourceVersion).To(Equal(podRV), "gate untouched: no Pod patch")
			Expect(getDM("rg1").ResourceVersion).To(Equal(dmRV), "no status write when nothing changed")
			Expect(gateOf(getPod("rg1-pod-0")).Status).To(Equal(corev1.ConditionTrue))
		})

		It("flips the gate False within one interval when the model disappears, then re-warms", func() {
			r, p, _ := stableWithPod("rg2")
			probesBefore := p.callCount()

			p.missing.Store(true)
			clock.add(regateInterval + time.Second)
			reconcileOnce(r, "rg2")
			gate := gateOf(getPod("rg2-pod-0"))
			Expect(gate).NotTo(BeNil())
			Expect(gate.Status).To(Equal(corev1.ConditionFalse))
			Expect(gate.Reason).To(Equal(reasonProbeError), "same reason as a failed probe")
			Expect(getDM("rg2").Status.Replicas.ModelReady).To(BeZero())
			Expect(getDM("rg2").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseDegraded))

			// Runtime has the model again: the normal probe path (Probe, which warms)
			// runs for the now-ungated Pod and the gate recovers.
			p.missing.Store(false)
			Eventually(func() corev1.ConditionStatus {
				_, _ = reconcileRes(r, "rg2")
				return gateOf(getPod("rg2-pod-0")).Status
			}, "10s", "50ms").Should(Equal(corev1.ConditionTrue))
			Expect(p.callCount()).To(BeNumerically(">", probesBefore), "recovery goes through Probe (warmup path), not Reinspect")
		})

		It("keeps a healthy Pod's gate True on a transient re-inspect auth error (key rotation)", func() {
			r, p, _ := stableWithPod("rg2a")
			// A 401 during an API-key rotation window: the re-inspect fails with a
			// transport/auth error, not a model loss. The gate must stay True and
			// the Pod must keep counting ready — dropping it would take down serving.
			p.authErr.Store(true)
			clock.add(regateInterval + time.Second)
			reconcileOnce(r, "rg2a")
			gate := gateOf(getPod("rg2a-pod-0"))
			Expect(gate).NotTo(BeNil())
			Expect(gate.Status).To(Equal(corev1.ConditionTrue), "transient auth error must not flip the gate")
			Expect(getDM("rg2a").Status.Replicas.ModelReady).To(Equal(int32(1)))

			// But the keep-True window is bounded: a runtime that stays unreachable
			// past maxTransientReinspectFailures consecutive re-inspects is flipped
			// False, so a permanently hung/mis-keyed runtime cannot stay model-ready
			// forever.
			for i := 0; i < maxTransientReinspectFailures; i++ {
				clock.add(regateInterval + time.Second)
				reconcileOnce(r, "rg2a")
			}
			gate = gateOf(getPod("rg2a-pod-0"))
			Expect(gate.Status).To(Equal(corev1.ConditionFalse), "gate flips False once the transient window is exhausted")
			Expect(gate.Reason).To(Equal(reasonProbeError))
			Expect(getDM("rg2a").Status.Replicas.ModelReady).To(BeZero())

			// Recovery: the key is accepted again and the Pod re-warms to True.
			p.authErr.Store(false)
			Eventually(func() corev1.ConditionStatus {
				_, _ = reconcileRes(r, "rg2a")
				return gateOf(getPod("rg2a-pod-0")).Status
			}, "10s", "50ms").Should(Equal(corev1.ConditionTrue))
		})

		It("adds no requeue and no re-check for a Prober without the capability (unchanged behaviour)", func() {
			plain := &fakeProber{loaded: loaded}
			r := newR(newFakeEngine(), plain)
			newDM("rg3", nil)
			reconcileOnce(r, "rg3")
			rev := RevisionHash(getDM("rg3").Spec, defaultDigest, fakeImage)
			markJobComplete("rg3", rev)
			reconcileOnce(r, "rg3")
			createGatedPod("rg3", rev, "rg3-pod-0")
			Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
				_, _ = reconcileRes(r, "rg3")
				return phaseOf("rg3")
			}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
			clock.add(10 * regateInterval)
			res, err := reconcileRes(r, "rg3")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
		})
	})

	// ------------------------------------------------------------- item 2
	Describe("CUDA arch nodeSelector", func() {
		drive := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) (*appsv1.Deployment, *batchv1.Job) {
			r := newR(newFakeEngine(), &fakeProber{})
			newDM(name, mutate)
			reconcileOnce(r, name)
			dm := getDM(name)
			img := fakeImage
			if dm.Spec.Device == "cuda" {
				img = fakeImageCUDA
			}
			rev := RevisionHash(dm.Spec, defaultDigest, img)
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
			markJobComplete(name, rev)
			reconcileOnce(r, name)
			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
			return dep, job
		}

		It("pins cuda serving Pods to amd64 but not the prefetch Job", func() {
			dep, job := drive("ar1", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Device = "cuda" })
			Expect(dep.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue(archLabel, "amd64"))
			Expect(job.Spec.Template.Spec.NodeSelector).NotTo(HaveKey(archLabel))
		})

		It("does not pin cpu serving Pods", func() {
			dep, _ := drive("ar2", nil)
			Expect(dep.Spec.Template.Spec.NodeSelector).NotTo(HaveKey(archLabel))
		})

		It("respects a user arch nodeSelector and keeps the other selectors", func() {
			dep, _ := drive("ar3", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Device = "cuda"
				dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{
					NodeSelector: map[string]string{archLabel: "arm64", "pool": "gpu"},
				}
			})
			Expect(dep.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{archLabel: "arm64", "pool": "gpu"}))
		})
	})

	// ------------------------------------------------------------- item 3
	Describe("configurable timeouts", func() {
		It("sets the prefetch Job deadline from an explicit caching timeout, and leaves it alone otherwise", func() {
			r := newR(newFakeEngine(), &fakeProber{})
			newDM("to1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
					Timeouts: &decisionmodelv1alpha1.RolloutTimeouts{Caching: dur(2 * time.Hour)}}
			})
			newDM("to2", nil)
			reconcileOnce(r, "to1")
			reconcileOnce(r, "to2")
			for name, want := range map[string]*int64{"to1": ptrTo(int64(7200)), "to2": nil} {
				rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
				job := &batchv1.Job{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-prefetch-" + rev}, job)).To(Succeed())
				if want == nil {
					Expect(job.Spec.ActiveDeadlineSeconds).To(BeNil(), "%s: unset timeout leaves the engine's default", name)
				} else {
					Expect(job.Spec.ActiveDeadlineSeconds).To(Equal(want), name)
				}
			}
		})

		// Phase timeouts: defaults unchanged, custom values honoured. First
		// revision, so an overrun is a Failed DecisionModel with the reason below.
		DescribeTable("Caching and Starting phase timeouts",
			func(toStarting bool, timeouts *decisionmodelv1alpha1.RolloutTimeouts, advance time.Duration, wantPhase decisionmodelv1alpha1.DecisionModelPhase, wantReason string) {
				name := "ph"
				r := newR(newFakeEngine(), &fakeProber{})
				newDM(name, func(dm *decisionmodelv1alpha1.DecisionModel) {
					if timeouts != nil {
						dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{Timeouts: timeouts}
					}
				})
				reconcileOnce(r, name) // Caching
				if toStarting {
					markJobComplete(name, RevisionHash(getDM(name).Spec, defaultDigest, fakeImage))
					reconcileOnce(r, name) // Starting (no Pods)
					Expect(phaseOf(name)).To(Equal(decisionmodelv1alpha1.PhaseStarting))
				}
				clock.add(advance)
				reconcileOnce(r, name)
				Expect(phaseOf(name)).To(Equal(wantPhase))
				if wantReason != "" {
					deg := meta_Find(getDM(name), decisionmodelv1alpha1.ConditionDegraded)
					Expect(deg).NotTo(BeNil())
					Expect(deg.Reason).To(Equal(wantReason))
				}
			},
			Entry("caching default: 29m is fine", false, nil, 29*time.Minute, decisionmodelv1alpha1.PhaseCaching, ""),
			Entry("caching default: 31m times out", false, nil, 31*time.Minute, decisionmodelv1alpha1.PhaseFailed, reasonCacheTimeout),
			Entry("caching 5m: 6m times out", false, &decisionmodelv1alpha1.RolloutTimeouts{Caching: dur(5 * time.Minute)},
				6*time.Minute, decisionmodelv1alpha1.PhaseFailed, reasonCacheTimeout),
			Entry("caching 2h: 31m is still fine", false, &decisionmodelv1alpha1.RolloutTimeouts{Caching: dur(2 * time.Hour)},
				31*time.Minute, decisionmodelv1alpha1.PhaseCaching, ""),
			Entry("starting default: 9m is fine", true, nil, 9*time.Minute, decisionmodelv1alpha1.PhaseStarting, ""),
			Entry("starting default: 11m times out", true, nil, 11*time.Minute, decisionmodelv1alpha1.PhaseFailed, reasonStartTimeout),
			Entry("starting 3m: 4m times out", true, &decisionmodelv1alpha1.RolloutTimeouts{Starting: dur(3 * time.Minute)},
				4*time.Minute, decisionmodelv1alpha1.PhaseFailed, reasonStartTimeout),
			Entry("starting 1h: 11m is still fine", true, &decisionmodelv1alpha1.RolloutTimeouts{Starting: dur(time.Hour)},
				11*time.Minute, decisionmodelv1alpha1.PhaseStarting, ""),
		)

		DescribeTable("Evaluating phase timeout",
			func(evalTimeout time.Duration, advance time.Duration, wantPhase decisionmodelv1alpha1.DecisionModelPhase) {
				name := "ev"
				eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing", delay: time.Hour}
				r := newR(eng, &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
				Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "golden"},
					Data: map[string]string{"cases.jsonl": strings.Repeat(
						`{"state":{},"questions":{"q1":{"type":"choice"}},"expected":{"q1":"billing"}}`+"\n", 3)},
				})).To(Succeed())
				newDM(name, func(dm *decisionmodelv1alpha1.DecisionModel) {
					dm.Spec.Rollout = &decisionmodelv1alpha1.RolloutSpec{
						Evaluation: &decisionmodelv1alpha1.EvaluationSpec{
							DatasetRef:  decisionmodelv1alpha1.DatasetRef{ConfigMapRef: &decisionmodelv1alpha1.DatasetKeyRef{Name: "golden", Key: "cases.jsonl"}},
							MinAccuracy: "0.90",
						},
						Timeouts: &decisionmodelv1alpha1.RolloutTimeouts{Evaluating: dur(evalTimeout)},
					}
				})
				reconcileOnce(r, name)
				rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
				markJobComplete(name, rev)
				reconcileOnce(r, name)
				createGatedPod(name, rev, name+"-pod-0")
				reconcileOnce(r, name) // starts the (slow) evaluation
				Expect(phaseOf(name)).To(Equal(decisionmodelv1alpha1.PhaseEvaluating))

				clock.add(advance)
				Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
					_, _ = reconcileRes(r, name)
					return phaseOf(name)
				}, "5s", "50ms").Should(Equal(wantPhase))
				if wantPhase == decisionmodelv1alpha1.PhaseFailed {
					deg := meta_Find(getDM(name), decisionmodelv1alpha1.ConditionDegraded)
					Expect(deg).NotTo(BeNil())
					Expect(deg.Reason).To(Equal(reasonEvaluationTimeout))
				}
			},
			Entry("2m: 3m later it times out", 2*time.Minute, 3*time.Minute, decisionmodelv1alpha1.PhaseFailed),
			Entry("1h: 11m later it is still evaluating", time.Hour, 11*time.Minute, decisionmodelv1alpha1.PhaseEvaluating),
		)
	})

	Describe("timeouts validation (CEL)", func() {
		DescribeTable("bounds are 1m to 24h",
			func(value string, valid bool) {
				dm := &decisionmodelv1alpha1.DecisionModel{
					ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cel"},
					Spec: decisionmodelv1alpha1.DecisionModelSpec{
						Engine: "ollaya", Model: "laya:en",
						Rollout: &decisionmodelv1alpha1.RolloutSpec{},
					},
				}
				d, err := time.ParseDuration(value)
				if err == nil {
					dm.Spec.Rollout.Timeouts = &decisionmodelv1alpha1.RolloutTimeouts{Starting: &metav1.Duration{Duration: d}}
				}
				cerr := k8sClient.Create(ctx, dm)
				if valid {
					Expect(cerr).NotTo(HaveOccurred())
					return
				}
				Expect(cerr).To(HaveOccurred())
				Expect(apierrors.IsInvalid(cerr)).To(BeTrue(), "got %v", cerr)
				Expect(cerr.Error()).To(ContainSubstring("between 1m and 24h"))
			},
			Entry("1m", "1m", true),
			Entry("90m", "90m", true),
			Entry("24h", "24h", true),
			Entry("59s", "59s", false),
			Entry("25h", "25h", false),
			Entry("zero", "0s", false),
		)
	})
})

func ptrTo[T any](v T) *T { return &v }

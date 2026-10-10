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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Fault-injection coverage for transition seams the main suite does not pin yet:
// a status write lost right after a Pod gate patch, a prefetch Job create whose
// response is lost, and a same-phase re-entry that must not reset the phase clock.
// Faults are injected with sigs.k8s.io/controller-runtime/pkg/client/interceptor
// and an injectable clock; no production code is touched.
var _ = Describe("fault seams: gate, promotion, and phase timing", func() {
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
	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModelSpec)) {
		spec := decisionmodelv1alpha1.DecisionModelSpec{
			Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
		}
		if mutate != nil {
			mutate(&spec)
		}
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Spec: spec,
		})).To(Succeed())
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
	// mkPod creates a bare serving Pod (containers ready, no gate) for a revision,
	// so the operator's probePods patches the gate; the simulated kubelet then sets
	// PodReady once the gate is True.
	mkPod := func(name, rev, pod string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = "10.0.0.200"
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	gateStatus := func(pod string) corev1.ConditionStatus {
		p := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pod}, p)).To(Succeed())
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status
			}
		}
		return ""
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("faultseam-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Gap: the readiness gate is patched on the Pod, then the DecisionModel status
	// write is lost (a crash/timeout after the Pod patch landed). The next reconcile
	// must converge without flapping the gate back to False and without promoting
	// twice.
	It("keeps the gate True and promotes once when the status write is lost right after the gate patch", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rec := events.NewFakeRecorder(256)

		// Fail the FIRST DecisionModel status patch that happens after a gate patch
		// has landed on the Pod. The gate patch is a Pod status subresource patch;
		// the DM status patch is a DecisionModel status subresource patch.
		var gatePatched atomic.Bool
		var dropped atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*corev1.Pod); ok && sub == "status" {
					if perr := cl.Status().Patch(ctx, obj, patch, opts...); perr != nil {
						return perr
					}
					gatePatched.Store(true)
					return nil
				}
				if _, ok := obj.(*decisionmodelv1alpha1.DecisionModel); ok && sub == "status" &&
					gatePatched.Load() && dropped.CompareAndSwap(false, true) {
					// Let the DM status write LAND, then drop the response (crash seam).
					if perr := cl.Status().Patch(ctx, obj, patch, opts...); perr != nil {
						return perr
					}
					return fmt.Errorf("induced lost response after the DecisionModel status write landed")
				}
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: rec,
		}
		recon := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g3"}})
		}
		createDM("g3", nil)
		recon()
		rev := getDM("g3").Status.CandidateRevision.Hash
		prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		markJobComplete("g3", rev)
		recon()
		mkPod("g3", rev, "g3-a")

		// Reconcile until the DM reaches Ready, tolerating the one induced lost
		// response. The gate must never leave True once set.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			recon()
			if gateStatus("g3-a") != "" {
				Expect(gateStatus("g3-a")).To(Equal(corev1.ConditionTrue),
					"the gate must not flap back to False after the lost status write")
			}
			return getDM("g3").Status.Phase
		}, "10s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

		Expect(dropped.Load()).To(BeTrue(), "the induced lost-response seam must have fired")
		Expect(getDM("g3").Status.StableRevision).NotTo(BeNil())
		Expect(getDM("g3").Status.StableRevision.Hash).To(Equal(rev))
		Expect(gateStatus("g3-a")).To(Equal(corev1.ConditionTrue))

		// Promoted must never be emitted twice across the lost write. (It may be 0:
		// Events are buffered and flushed only on a successful status write, so the
		// promote write that landed-but-errored does not flush its Promoted, and the
		// convergence reconcile does not re-emit it because the switch already
		// happened. The invariant that matters is no DOUBLE promotion.)
		promoted := 0
		for drained := false; !drained; {
			select {
			case ev := <-rec.Events:
				if strings.Contains(ev, eventPromoted) {
					promoted++
				}
			default:
				drained = true
			}
		}
		Expect(promoted).To(BeNumerically("<=", 1), "Promoted is never emitted twice across the lost status write")
	})

	// Gap: the initial candidate prefetch Job is created but the client never sees
	// the response (the Job landed). The next reconcile must not create a second
	// Job.
	It("creates no duplicate prefetch Job when the Job create lands but the client errors", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var firedOnce atomic.Bool
		firedOnce.Store(true)
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*batchv1.Job); ok && firedOnce.CompareAndSwap(true, false) {
					if cerr := cl.Create(ctx, obj, opts...); cerr != nil {
						return cerr
					}
					return fmt.Errorf("induced lost response after the prefetch Job create landed")
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(128),
		}
		recon := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g12"}})
		}
		createDM("g12", nil)
		recon() // admission + the Job create whose response is lost
		rev := getDM("g12").Status.CandidateRevision.Hash

		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			recon()
			return getDM("g12").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseCaching), "converges to Caching after the lost response")

		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(namespace))).To(Succeed())
		n := 0
		for i := range jobs.Items {
			if jobs.Items[i].Name == jobName("g12", rev) {
				n++
			}
		}
		Expect(n).To(Equal(1), "exactly one prefetch Job exists (no duplicate from the retried create)")
	})

	// Gap: a same-phase re-entry (Caching -> Caching) with a changed condition
	// reason/message must NOT move phaseTransitionTime, so the caching timeout still
	// fires measured from the FIRST entry into Caching.
	It("does not reset the phase clock on a same-phase re-entry, so the caching timeout fires from first entry", func() {
		clock := newSafeClock()
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:   prober,
			Recorder: events.NewFakeRecorder(256),
			Now:      clock.now,
		}
		recon := func() {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "g15"}})
		}
		// Caching timeout well under the 30m default so the frozen clock can cross it.
		createDM("g15", func(s *decisionmodelv1alpha1.DecisionModelSpec) {
			s.Rollout = &decisionmodelv1alpha1.RolloutSpec{
				Timeouts: &decisionmodelv1alpha1.RolloutTimeouts{Caching: &metav1.Duration{Duration: 5 * time.Minute}},
			}
		})
		recon() // admission; the prefetch Job is created, DM enters Caching
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			recon()
			return getDM("g15").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhaseCaching))
		firstEntry := getDM("g15").Status.PhaseTransitionTime
		Expect(firstEntry).NotTo(BeNil())

		// Re-enter Caching several times while advancing the clock below the
		// timeout; the Cached condition message may churn, but the phase stays
		// Caching, so phaseTransitionTime must not move.
		for i := 0; i < 4; i++ {
			clock.add(1 * time.Minute)
			recon()
			Expect(getDM("g15").Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseCaching))
			Expect(getDM("g15").Status.PhaseTransitionTime.Time).To(Equal(firstEntry.Time),
				"a same-phase re-entry must not move phaseTransitionTime")
		}

		// Cross the caching timeout measured from the FIRST entry (now +~4m, add 2m
		// more => >5m). The DM must roll back / fail on the caching timeout, proving
		// the deadline was anchored to the first entry, not reset by the re-entries.
		clock.add(2 * time.Minute)
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			recon()
			return getDM("g15").Status.Phase
		}, "5s", "50ms").Should(Or(
			Equal(decisionmodelv1alpha1.PhaseFailed),
			Equal(decisionmodelv1alpha1.PhaseRolledBack),
		), "the caching timeout fires measured from the first entry into Caching")
	})
})

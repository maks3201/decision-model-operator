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
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Rollout admission is durable (status.candidateRevision persisted before any
// allocation) and counted through an uncached read, so --max-concurrent-rollouts
// holds across leader changes and a lagging cache.
var _ = Describe("durable rollout admission", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
		clock     time.Time
	)
	int32Ptr := func(v int32) *int32 { return &v }

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	newRec := func(cl client.Client, prober *revProber, maxRollouts int) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: cl, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:               map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:                prober,
			Recorder:              events.NewFakeRecorder(256),
			MaxConcurrentRollouts: maxRollouts,
			WatchNamespaces:       []string{namespace},
			Now:                   func() time.Time { return clock },
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	createDM := func(name, model string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
	}
	pvcExists := func(name, rev string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM(name), rev)}, &corev1.PersistentVolumeClaim{})
		return err == nil
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("admission-%d", counter)
		clock = time.Now()
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("persists status.candidateRevision before creating the candidate store PVC", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var pvcHadAdmission atomic.Bool
		var pvcCreated atomic.Bool
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					pvcCreated.Store(true)
					// Read the DM straight from the API server: the admission must
					// already be durable before this PVC create.
					dm := &decisionmodelv1alpha1.DecisionModel{}
					if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: pvc.Namespace, Name: namespaceDMName(pvc.Name)}, dm); gerr == nil {
						if dm.Status.CandidateRevision != nil {
							pvcHadAdmission.Store(true)
						}
					}
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := newRec(c, prober, 2)
		createDM("one", "laya:en")
		// Reconcile until the candidate PVC is created.
		Eventually(func() bool {
			rec(r, "one")
			return pvcCreated.Load()
		}, "5s", "50ms").Should(BeTrue())
		Expect(pvcHadAdmission.Load()).To(BeTrue(),
			"status.candidateRevision must be persisted before the candidate PVC is created")
	})

	It("does not over-admit from a second reconciler with an empty reservation map", func() {
		proberA := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rA := newRec(k8sClient, proberA, 1)
		createDM("a", "laya:en")
		revA := RevisionHash(getDM("a").Spec, defaultDigest, fakeImage)
		proberA.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		// Admit A: its candidateRevision is persisted (the slot is taken).
		rec(rA, "a")
		Expect(getDM("a").Status.CandidateRevision).NotTo(BeNil(), "A admitted and persisted")

		// A brand-new reconciler (as after a leader change) with an empty RAM
		// reservation map must still see A's slot via the uncached count, so B is
		// queued, not admitted — no over-admission.
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(k8sClient, proberB, 1)
		Expect(rB.budgetReservations).To(BeEmpty())
		createDM("b", "kev:en")
		revB := RevisionHash(getDM("b").Spec, defaultDigest, fakeImage)
		proberB.set(revB, engine.Loaded{Name: "kev:en", Digest: defaultDigest, Device: "cpu"})
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(rB, "b")
			return getDM("b").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhasePending), "B queued behind A's durable admission")
		Expect(getDM("b").Status.CandidateRevision).To(BeNil(), "B not admitted")
		Expect(pvcExists("b", revB)).To(BeFalse(), "B created no candidate PVC while queued")
	})

	It("creates nothing when the admission status write conflicts", func() {
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var failOnce atomic.Bool
		failOnce.Store(true)
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if dm, ok := obj.(*decisionmodelv1alpha1.DecisionModel); ok &&
					dm.Status.CandidateRevision != nil && failOnce.CompareAndSwap(true, false) {
					return apierrors.NewConflict(
						decisionmodelv1alpha1.GroupVersion.WithResource("decisionmodels").GroupResource(),
						dm.Name, fmt.Errorf("simulated admission conflict"))
				}
				return cl.Status().Patch(ctx, obj, patch, opts...)
			},
		})
		r := newRec(c, prober, 2)
		createDM("conf", "laya:en")
		rev := RevisionHash(getDM("conf").Spec, defaultDigest, fakeImage)
		prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		rec(r, "conf") // admission status write conflicts

		// Nothing allocated: no candidate PVC, and candidateRevision not durably set.
		Expect(pvcExists("conf", rev)).To(BeFalse(), "no PVC created when the admission write conflicted")
		// A later reconcile (no conflict) admits and then allocates.
		Eventually(func() bool {
			rec(r, "conf")
			return getDM("conf").Status.CandidateRevision != nil
		}, "5s", "50ms").Should(BeTrue(), "admission succeeds on retry")
	})

	It("counts an upgraded in-flight candidate (candidateRevision already set) without a new admission", func() {
		// A DM carrying a candidateRevision as if left in flight across an operator
		// upgrade occupies a slot and must not be re-admitted.
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rA := newRec(k8sClient, prober, 1)
		createDM("up-a", "laya:en")
		revA := RevisionHash(getDM("up-a").Spec, defaultDigest, fakeImage)
		prober.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		rec(rA, "up-a")
		Expect(getDM("up-a").Status.CandidateRevision).NotTo(BeNil())
		// up-a's candidate occupies the only slot. Its reservation is irrelevant
		// (durable candidateRevision counts), so a fresh reconciler must queue B.
		rB := newRec(k8sClient, prober, 1)
		createDM("up-b", "kev:en")
		revB := RevisionHash(getDM("up-b").Spec, defaultDigest, fakeImage)
		prober.set(revB, engine.Loaded{Name: "kev:en", Digest: defaultDigest, Device: "cpu"})
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(rB, "up-b")
			return getDM("up-b").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhasePending),
			"the upgraded in-flight candidate counts; B queues without a new admission")
	})

	// Items 5-6: a crash (interceptor error) right AFTER the candidate PVC create,
	// then after the prefetch Job create. Admission is persisted before either, so
	// a brand-new reconciler (empty reservation map) must still see A's durable
	// slot and queue B — the post-allocation crash does not release the slot.
	crashAfterKindThenNoOverAdmit := func(dmName, otherName string, failKind string) {
		proberA := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		var failed atomic.Bool
		failed.Store(true)
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				// Let the write LAND, then crash the reconcile: the object exists but
				// the reconcile returns an error (process dies right after the create).
				if cerr := cl.Create(ctx, obj, opts...); cerr != nil {
					return cerr
				}
				switch failKind {
				case "pvc":
					if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && failed.CompareAndSwap(true, false) {
						return fmt.Errorf("induced crash right after the candidate PVC create")
					}
				case "job":
					if _, ok := obj.(*batchv1.Job); ok && failed.CompareAndSwap(true, false) {
						return fmt.Errorf("induced crash right after the prefetch Job create")
					}
				}
				return nil
			},
		})
		rA := newRec(c, proberA, 1)
		createDM(dmName, "laya:en")
		revA := RevisionHash(getDM(dmName).Spec, defaultDigest, fakeImage)
		proberA.set(revA, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		// Drive A until the induced crash fires; admission is already durable.
		Eventually(func() bool {
			rec(rA, dmName)
			return getDM(dmName).Status.CandidateRevision != nil
		}, "5s", "50ms").Should(BeTrue(), "A admitted (durable) despite the post-allocation crash")

		// A fresh reconciler with an empty reservation map must still queue B.
		proberB := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		rB := newRec(k8sClient, proberB, 1)
		Expect(rB.budgetReservations).To(BeEmpty())
		createDM(otherName, "kev:en")
		revB := RevisionHash(getDM(otherName).Spec, defaultDigest, fakeImage)
		proberB.set(revB, engine.Loaded{Name: "kev:en", Digest: defaultDigest, Device: "cpu"})
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(rB, otherName)
			return getDM(otherName).Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhasePending),
			"B queued behind A's durable slot after a post-allocation crash")
		Expect(getDM(otherName).Status.CandidateRevision).To(BeNil(), "B not admitted")
	}

	It("does not over-admit after a crash right after the candidate PVC create", func() {
		crashAfterKindThenNoOverAdmit("pvc-a", "pvc-other", "pvc")
	})

	It("does not over-admit after a crash right after the prefetch Job create", func() {
		crashAfterKindThenNoOverAdmit("job-a", "job-other", "job")
	})

	// Item 7: a stress invariant — 15 DecisionModels, limit 2, reconciled in
	// parallel goroutines over 20 iterations through ONE reconciler (as the manager
	// does with MaxConcurrentReconciles > 1). The count+decide+reserve is serialized
	// under budgetMu, so no matter the interleaving, at most 2 DMs may ever carry a
	// durable candidateRevision at once. Run under -race.
	It("never admits more than the budget under heavy parallel reconciles", func() {
		const (
			n     = 15
			limit = 2
			iters = 20
		)
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := newRec(k8sClient, prober, limit)
		names := make([]string, n)
		for i := 0; i < n; i++ {
			names[i] = fmt.Sprintf("st-%d", i)
			createDM(names[i], "laya:en")
			rev := RevisionHash(getDM(names[i]).Spec, defaultDigest, fakeImage)
			prober.set(rev, engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"})
		}
		admitted := func() int {
			c := 0
			for _, nm := range names {
				if getDM(nm).Status.CandidateRevision != nil {
					c++
				}
			}
			return c
		}
		for it := 0; it < iters; it++ {
			var wg sync.WaitGroup
			for _, nm := range names {
				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					defer GinkgoRecover()
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
				}(nm)
			}
			wg.Wait()
			Expect(admitted()).To(BeNumerically("<=", limit),
				"iteration %d: never more than %d DMs admitted at once", it, limit)
		}
		// Steady state: exactly the budget is in flight (the rest queued), none lost.
		Expect(admitted()).To(Equal(limit), "exactly the budget is admitted once things settle")
	})

	// Item 13: budget scoped across two watched namespaces, limit 1. One admission
	// across the pair holds the only slot; a DM in the other watched namespace
	// queues. A DM in a THIRD, unwatched namespace is neither counted nor
	// reconciled (so it cannot occupy or be blocked by the budget).
	It("scopes the budget across watched namespaces and ignores unwatched ones", func() {
		nsA := namespace + "-a"
		nsB := namespace + "-b"
		nsOut := namespace + "-out"
		for _, n := range []string{nsA, nsB, nsOut} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}})).To(Succeed())
		}
		prober := &revProber{fallback: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Prober: prober,
			Recorder: events.NewFakeRecorder(128), MaxConcurrentRollouts: 1,
			WatchNamespaces: []string{nsA, nsB},
			Now:             func() time.Time { return clock },
		}
		mk := func(ns, name, model string) {
			Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
				Spec: decisionmodelv1alpha1.DecisionModelSpec{
					Engine: "ollaya", Model: model, Device: "cpu", Replicas: int32Ptr(1),
				},
			})).To(Succeed())
		}
		get := func(ns, name string) *decisionmodelv1alpha1.DecisionModel {
			dm := &decisionmodelv1alpha1.DecisionModel{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, dm)).To(Succeed())
			return dm
		}
		recNS := func(ns, name string) {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
		}

		mk(nsA, "a", "laya:en")
		mk(nsB, "b", "kev:en")
		mk(nsOut, "c", "laya:en")
		for _, nm := range []struct{ ns, n, model string }{{nsA, "a", "laya:en"}, {nsB, "b", "kev:en"}} {
			rev := RevisionHash(get(nm.ns, nm.n).Spec, defaultDigest, fakeImage)
			prober.set(rev, engine.Loaded{Name: nm.model, Digest: defaultDigest, Device: "cpu"})
		}

		// A (nsA) is admitted and holds the only cross-namespace slot.
		recNS(nsA, "a")
		Expect(get(nsA, "a").Status.CandidateRevision).NotTo(BeNil(), "A admitted")

		// B (nsB) must queue behind A even though it is a different namespace.
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			recNS(nsB, "b")
			return get(nsB, "b").Status.Phase
		}, "5s", "50ms").Should(Equal(decisionmodelv1alpha1.PhasePending), "B queued behind A across namespaces")

		// C (unwatched nsOut) is ignored: a reconcile is a no-op (no status written),
		// and it is not counted against the budget.
		recNS(nsOut, "c")
		Expect(get(nsOut, "c").Status.Phase).To(BeEmpty(), "an unwatched DM is not reconciled")
	})
})

// namespaceDMName maps a store PVC name (<dm>-store-<rev>) back to the DM name by
// stripping the "-store-<rev>" suffix. Used by the PVC-create interceptor to find
// the owning DecisionModel. It assumes the test DM names have no "-store-" in them.
func namespaceDMName(pvcName string) string {
	for i := 0; i+len("-store-") <= len(pvcName); i++ {
		if pvcName[i:i+len("-store-")] == "-store-" {
			return pvcName[:i]
		}
	}
	return pvcName
}

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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// Each revision's model manifest is persisted as an owned, revision-labelled
// ConfigMap before the prefetch Job, so a lost store can be rebuilt to the
// recorded digest from the recorded bytes rather than re-resolving the tag. If
// the upstream tag has MOVED, `ollaya pull <tag>` overwrites the seeded store
// with the moved bytes (ollaya-dev/ollaya#64), so the rebuild does not reach the
// recorded digest — it surfaces as UpstreamTagMoved. The persisted manifest is
// the recorded intent and the seed for engines/registries that honour it.
var _ = Describe("per-revision manifest persistence", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
	)
	// A synthetic manifest; the fake engine derives the digest from it.
	manifestBody := []byte(`{"schemaVersion":2,"layers":[{"digest":"sha256:deadbeef"}]}`)
	wantDigest := digestOf(manifestBody)

	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	newR := func(eng engine.Engine) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(128),
		}
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
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
	gatedPod := func(name, rev, pod, ip string) {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: pod,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		p.Status.PodIP = ip
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
	}
	manifestCM := func(name, rev string) (*corev1.ConfigMap, error) {
		cm := &corev1.ConfigMap{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-manifest-" + rev}, cm)
		return cm, err
	}
	createDM := func(name string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
	}
	// driveStable brings a first rollout to a Ready stable, returning the rev hash.
	driveStable := func(r *DecisionModelReconciler, name string) string {
		createDM(name)
		rec(r, name)
		rev := getDM(name).Status.CandidateRevision.Hash
		markJob(name, rev)
		rec(r, name)
		gatedPod(name, rev, name+"-a", "10.0.0.80")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return getDM(name).Status.Phase
		}, "5s", "30ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		return rev
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("manifest-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	It("persists the candidate manifest ConfigMap before the prefetch Job and seeds the Job from it", func() {
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("cand")
		rec(r, "cand")
		rev := getDM("cand").Status.CandidateRevision.Hash

		// The owned manifest ConfigMap exists with the exact bytes, verified by digest.
		cm, err := manifestCM("cand", rev)
		Expect(err).NotTo(HaveOccurred(), "manifest ConfigMap created before/with the prefetch Job")
		Expect(cm.BinaryData[manifestKey]).To(Equal(manifestBody))
		Expect(digestOf(cm.BinaryData[manifestKey])).To(Equal(wantDigest))
		Expect(ownedBy(cm, getDM("cand"))).To(BeTrue(), "owned by the DecisionModel")
		Expect(cm.Labels[decisionmodelv1alpha1.LabelRevision]).To(Equal(rev))

		// The prefetch Job was seeded from the manifest (fake surfaces the digest).
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("cand", rev)}, job)).To(Succeed())
		var seeded string
		for _, e := range job.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "MANIFEST_SEED_DIGEST" {
				seeded = e.Value
			}
		}
		Expect(seeded).To(Equal(wantDigest), "the prefetch Job carries the seeded manifest")
	})

	It("renders the prefetch Job with the recorded manifest seed after the tag moved (runtime outcome is UpstreamTagMoved)", func() {
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		rev := driveStable(r, "lost")
		stable := getDM("lost").Status.StableRevision
		Expect(stable.Digest).To(Equal(wantDigest))
		resolvesAfterStable := eng.resolveCount()

		// The tag moves upstream: Resolve would now return a DIFFERENT digest.
		eng.mu.Lock()
		eng.manifest = []byte(`{"schemaVersion":2,"layers":[{"digest":"sha256:moved"}]}`)
		eng.mu.Unlock()

		// The store-recovery path seeds the prefetch params from the recorded
		// manifest (the stable's pinned digest), NOT by re-resolving the moved tag.
		// NOTE: this asserts only that the Job is RENDERED with the recorded seed.
		// It does not assert the model is rebuilt to the recorded digest at runtime:
		// `ollaya pull <tag>` overwrites the seeded store with the moved tag's bytes
		// (ollaya-dev/ollaya#64), so the real runtime outcome is a digest mismatch
		// surfaced as UpstreamTagMoved (see prefetch_failure / store_recovery specs).
		// The seed still matters as the recorded intent and for engines/registries
		// that honour it.
		base := r.stableParams(ctx, getDM("lost"), stable, "lost-store-"+rev)
		seeded := r.seedManifest(ctx, getDM("lost"), base, stable.Hash, stable.Digest)
		Expect(seeded.Model.Manifest).To(Equal(manifestBody),
			"recovery seeds the recorded manifest, not the moved tag's bytes")
		Expect(digestOf(seeded.Model.Manifest)).To(Equal(wantDigest))
		Expect(eng.resolveCount()).To(Equal(resolvesAfterStable),
			"recovery did not re-resolve: it used the persisted manifest ConfigMap")

		// The rendered prefetch Job carries the recorded seed digest.
		job := eng.PrefetchJobSpec(seeded)
		var found string
		for _, e := range job.Template.Spec.Containers[0].Env {
			if e.Name == "MANIFEST_SEED_DIGEST" {
				found = e.Value
			}
		}
		Expect(found).To(Equal(wantDigest))
	})

	It("ignores a foreign ConfigMap with the manifest name (not owned)", func() {
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("foreign")
		// Pre-create a FOREIGN ConfigMap at the manifest name (no owner ref, bogus
		// bytes) before the first reconcile computes the rev. We cannot know the rev
		// name up front, so drive to a candidate first, then overwrite its CM with a
		// foreign one and assert manifestBytes rejects it.
		rec(r, "foreign")
		rev := getDM("foreign").Status.CandidateRevision.Hash

		// Replace the owned CM with a foreign one (strip the owner reference).
		cm, err := manifestCM("foreign", rev)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Delete(ctx, cm)).To(Succeed())
		foreign := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "foreign-manifest-" + rev,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: "foreign", decisionmodelv1alpha1.LabelRevision: rev},
			},
			BinaryData: map[string][]byte{manifestKey: []byte("not the manifest")},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

		// manifestBytes must reject the foreign object (owner UID mismatch), so the
		// prefetch would fall back to pull-by-tag (seed empty).
		Expect(r.manifestBytes(ctx, getDM("foreign"), rev, wantDigest)).To(BeNil(),
			"a foreign ConfigMap with our name is not trusted")
	})

	It("garbage-collects the manifest ConfigMap with its revision", func() {
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		revA := driveStable(r, "gc")

		// revA's manifest ConfigMap exists and is kept while revA is the stable.
		_, err := manifestCM("gc", revA)
		Expect(err).NotTo(HaveOccurred())
		Expect(r.gcRevisions(ctx, getDM("gc"))).To(Succeed())
		_, err = manifestCM("gc", revA)
		Expect(err).NotTo(HaveOccurred(), "the stable revision's manifest ConfigMap is kept by GC")

		// Make revA stale: record a different stable revision in status so revA is
		// neither stable nor candidate nor Service-selected. gcRevisions must then
		// collect revA's manifest ConfigMap along with its other workloads.
		Expect(updateDMStatus(ctx, namespace, "gc", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
				Hash: "ffffffffffffffff", Engine: "ollaya", Model: "kev:en",
				Digest: wantDigest, Device: "cpu", Image: fakeImage, Placement: placementNone,
			}
			dm.Status.CandidateRevision = nil
		})).To(Succeed())
		// Delete the Service so its selector does not keep revA.
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "gc"}})

		Expect(r.gcRevisions(ctx, getDM("gc"))).To(Succeed())
		Eventually(func() bool {
			cm, gerr := manifestCM("gc", revA)
			return apierrors.IsNotFound(gerr) || (gerr == nil && cm.DeletionTimestamp != nil)
		}, "3s", "50ms").Should(BeTrue(), "the stale revision's manifest ConfigMap is GC'd")
	})

	It("falls back to pull-by-tag for a pre-existing revision without a manifest ConfigMap", func() {
		// A revision resolved by an engine that returns no manifest bytes (the
		// pre-manifest behaviour): no ConfigMap is created, the Job is not seeded,
		// and nothing fails.
		eng := newFakeEngine() // manifest nil -> Resolve returns no Manifest
		r := newR(eng)
		r.Prober = &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		createDM("legacy")
		rec(r, "legacy")
		rev := getDM("legacy").Status.CandidateRevision.Hash

		_, err := manifestCM("legacy", rev)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no manifest ConfigMap without resolvable bytes")
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("legacy", rev)}, job)).To(Succeed())
		for _, e := range job.Spec.Template.Spec.Containers[0].Env {
			Expect(e.Name).NotTo(Equal("MANIFEST_SEED_DIGEST"), "no seed -> pull-by-tag")
		}
	})

	It("re-resolves and writes the manifest on a later reconcile when the first write fails", func() {
		// The ConfigMap write happens in the SAME reconcile as Resolve, before the
		// digest is recorded in status. If the write fails, the reconcile returns
		// an error WITHOUT recording the digest, so the next reconcile resolves
		// again and retries — the bytes are never lost (there is no window where a
		// recorded digest has no durable manifest).
		eng := newFakeEngine()
		eng.manifest = manifestBody

		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		var failWrite atomic.Bool
		failWrite.Store(true)
		failing := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if cm, ok := obj.(*corev1.ConfigMap); ok &&
					cm.Name == "retry-manifest-"+RevisionHash(getDM("retry").Spec, wantDigest, fakeImage) &&
					failWrite.CompareAndSwap(true, false) {
					return fmt.Errorf("induced manifest ConfigMap write failure")
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: failing, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(128),
		}
		createDM("retry")

		// First reconcile: the manifest write fails BEFORE the admission status
		// write, so the reconcile errors and NO candidate digest is recorded
		// (status.candidateRevision stays nil) — the recorded digest can never
		// outlive its durable manifest.
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "retry"}})
		Expect(err).To(HaveOccurred(), "a failed manifest write fails the reconcile")
		rev := RevisionHash(getDM("retry").Spec, wantDigest, fakeImage)
		_, cmErr := manifestCM("retry", rev)
		Expect(apierrors.IsNotFound(cmErr)).To(BeTrue(), "no ConfigMap after the failed write")
		Expect(getDM("retry").Status.CandidateRevision).To(BeNil(),
			"the digest is not recorded in status when the manifest could not be persisted")

		// Later reconcile (write now succeeds): the controller re-resolves and
		// persists the manifest, then records the candidate.
		rec(r, "retry")
		cm, err := manifestCM("retry", rev)
		Expect(err).NotTo(HaveOccurred(), "the retry re-resolves and writes the manifest")
		Expect(cm.BinaryData[manifestKey]).To(Equal(manifestBody))
		Expect(eng.resolveCount()).To(BeNumerically(">=", 2), "it resolved again rather than losing the bytes")
		Expect(getDM("retry").Status.CandidateRevision).NotTo(BeNil())
	})

	It("releases the rollout-budget reservation when the admission manifest write fails", func() {
		// A reservation is taken when a candidate is admitted (budget slot). If the
		// manifest write then fails before the admission is durable, the reservation
		// must be released: otherwise a persistent manifest-write failure would pin a
		// rollout slot forever (syncReservations only drops it once a durable
		// candidate is visible, which never happens on this failure).
		eng := newFakeEngine()
		eng.manifest = manifestBody

		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		failing := interceptor.NewClient(wc, interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if cm, ok := obj.(*corev1.ConfigMap); ok &&
					cm.Name == "resv-manifest-"+RevisionHash(getDM("resv").Spec, wantDigest, fakeImage) {
					return fmt.Errorf("induced manifest ConfigMap write failure")
				}
				return cl.Create(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: failing, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(128),
			// A single slot, free: this DM is admitted (so it takes a reservation)
			// and then hits the failing manifest write.
			MaxConcurrentRollouts: 1,
			WatchNamespaces:       []string{namespace},
		}
		createDM("resv")

		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "resv"}})
		Expect(err).To(HaveOccurred(), "the failed manifest write fails the reconcile")

		// The reservation for this DM must have been released on the error return.
		r.budgetMu.Lock()
		_, held := r.budgetReservations[namespace+"/resv"]
		r.budgetMu.Unlock()
		Expect(held).To(BeFalse(), "reservation released after the failed admission manifest write")
	})

	It("persists no manifest ConfigMap while the candidate is queued, so repeated spec changes do not leak", func() {
		// The manifest is persisted only AFTER admission, so a candidate queued
		// behind the fleet budget writes no manifest ConfigMap. Changing spec.model
		// three times while queued therefore leaves no orphaned manifests (the leak
		// the old pre-gate persist caused — one ConfigMap per queued revision until
		// the DM was deleted).
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		r.MaxConcurrentRollouts = 1
		r.WatchNamespaces = []string{namespace}
		// Hold the only slot so this DM's candidate is always queued.
		r.budgetReservations = map[string]string{namespace + "/other": "someother"}

		createDM("queued")
		models := []string{"laya:en", "kev:en", "jevk5:en"}
		revs := map[string]struct{}{}
		for _, m := range models {
			Expect(updateDM(ctx, namespace, "queued", func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Spec.Model = m
			})).To(Succeed())
			rec(r, "queued")
			dm := getDM("queued")
			Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "queued behind the budget")
			Expect(dm.Status.CandidateRevision).To(BeNil(), "not admitted while queued")
			revs[RevisionHash(dm.Spec, eng.digest, fakeImage)] = struct{}{}
		}

		// No manifest ConfigMap exists for ANY of the queued revisions: nothing leaked.
		for rev := range revs {
			_, err := manifestCM("queued", rev)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"a queued candidate must not persist a manifest ConfigMap (no leak), rev %s", rev)
		}
	})

	It("leaves exactly the stable and the admitted candidate manifests after A->B->C->D while queued", func() {
		// With a serving stable and a candidate repeatedly re-specced (A->B->C->D)
		// while queued behind the full budget, no manifest ConfigMap is written for
		// any queued revision. Once the slot frees and the final revision (D) is
		// admitted, exactly two manifest ConfigMaps exist for the DM: the stable's
		// and D's — the intermediates never leaked and are not resurrected.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		stableRev := driveStable(r, "abcd")

		// Fill the only slot so the DM's next candidate queues.
		r.MaxConcurrentRollouts = 1
		r.WatchNamespaces = []string{namespace}
		r.budgetReservations = map[string]string{namespace + "/holder": "held"}

		// A->B->C->D: four spec.model changes, all queued, none persisting a manifest.
		models := []string{"kev:en", "jevk5:en", "winnow:en", "decider:en"}
		queuedRevs := map[string]struct{}{}
		for _, m := range models {
			Expect(updateDM(ctx, namespace, "abcd", func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Spec.Model = m
			})).To(Succeed())
			rec(r, "abcd")
			dm := getDM("abcd")
			Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "queued behind the budget for %s", m)
			rev := RevisionHash(dm.Spec, wantDigest, fakeImage)
			if rev != stableRev {
				queuedRevs[rev] = struct{}{}
				_, err := manifestCM("abcd", rev)
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no manifest for queued revision %s", rev)
			}
		}

		// Free the slot: the final revision (D) is admitted and its manifest written.
		r.budgetReservations = map[string]string{}
		dRev := RevisionHash(getDM("abcd").Spec, wantDigest, fakeImage)
		Eventually(func() bool {
			rec(r, "abcd")
			cr := getDM("abcd").Status.CandidateRevision
			return cr != nil && cr.Hash == dRev
		}, "5s", "50ms").Should(BeTrue(), "D admitted once the slot frees")

		// Exactly the stable and D manifests exist; no intermediate leaked.
		all := &corev1.ConfigMapList{}
		Expect(k8sClient.List(ctx, all, client.InNamespace(namespace))).To(Succeed())
		have := map[string]struct{}{}
		prefix := "abcd-manifest-"
		for i := range all.Items {
			if n := all.Items[i].Name; len(n) > len(prefix) && n[:len(prefix)] == prefix {
				have[n[len(prefix):]] = struct{}{}
			}
		}
		Expect(have).To(HaveKey(stableRev), "the stable manifest is kept")
		Expect(have).To(HaveKey(dRev), "the admitted candidate (D) manifest is written")
		Expect(have).To(HaveLen(2), "exactly the stable + D manifests exist, no queued intermediate leaked")
		for rev := range queuedRevs {
			if rev == dRev {
				continue
			}
			Expect(have).NotTo(HaveKey(rev), "intermediate queued revision %s never persisted a manifest", rev)
		}
	})

	It("persists and mounts the manifest once the candidate is admitted", func() {
		// Admitted (no budget limit): the manifest is persisted and the prefetch
		// Job references the ConfigMap (mounted, not an env var).
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("admitted")
		rec(r, "admitted")
		dm := getDM("admitted")
		Expect(dm.Status.CandidateRevision).NotTo(BeNil(), "admitted")
		rev := dm.Status.CandidateRevision.Hash
		cm, err := manifestCM("admitted", rev)
		Expect(err).NotTo(HaveOccurred(), "manifest persisted after admission")
		Expect(cm.BinaryData[manifestKey]).To(Equal(manifestBody))

		// The prefetch Job mounts the ConfigMap: a volume references it by name.
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("admitted", rev)}, job)).To(Succeed())
		mountsCM := false
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.ConfigMap != nil && v.ConfigMap.Name == "admitted-manifest-"+rev {
				mountsCM = true
			}
		}
		Expect(mountsCM).To(BeTrue(), "the prefetch Job mounts the manifest ConfigMap")
	})

	It("deletes the manifest ConfigMap of a superseded candidate that never got a PVC", func() {
		// A candidate queued by the rollout budget persists its manifest ConfigMap
		// but never creates a store PVC. When it is later abandoned, GC cannot find
		// it through a stale PVC (there is none) and cannot List ConfigMaps
		// (get-only RBAC), so the abandon path records the hash on the reconcile
		// state and GC deletes <dm>-manifest-<rev> by name.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("abandon")
		dmObj := getDM("abandon")
		// A candidate ConfigMap for a revision that has NO store PVC (queued by the
		// budget, never prefetched).
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "abandon-manifest-deadbeef01",
				Labels: revisionLabels(dmObj, "deadbeef01"),
			},
			BinaryData: map[string][]byte{manifestKey: manifestBody},
		}
		Expect(controllerutil.SetControllerReference(dmObj, cm, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		_, err := manifestCM("abandon", "deadbeef01")
		Expect(err).NotTo(HaveOccurred(), "precondition: the orphan ConfigMap exists")

		// Record the hash as abandoned on the reconcile state (as the abandon paths
		// do before clearing status.candidateRevision), then run GC. No PVC named
		// abandon-store-deadbeef01 exists.
		st := &reconcileState{base: getDM("abandon").DeepCopy()}
		gctx := context.WithValue(ctx, reconcileStateKey{}, st)
		markManifestAbandoned(gctx, "deadbeef01")
		Expect(r.gcRevisions(gctx, getDM("abandon"))).To(Succeed())

		Eventually(func() bool {
			c, gerr := manifestCM("abandon", "deadbeef01")
			return apierrors.IsNotFound(gerr) || (gerr == nil && c.DeletionTimestamp != nil)
		}, "3s", "50ms").Should(BeTrue(),
			"the abandoned candidate's manifest ConfigMap is deleted by name even without a PVC")
	})

	It("deletes the manifest ConfigMap of a stale revision whose PVC is still terminating", func() {
		// A crash between the PVC delete and the ConfigMap delete leaves the PVC
		// terminating (deletionTimestamp set, finalizer lingering) with the
		// ConfigMap still present. The next GC pass still sees the terminating PVC
		// in the List, so it re-derives the revision and deletes the ConfigMap.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("term")
		dmObj := getDM("term")

		// A stale revision (neither stable nor candidate): a store PVC with a
		// finalizer so a Delete leaves it terminating, and its manifest ConfigMap.
		staleRev := "cafef00d01"
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "term-store-" + staleRev,
				Labels:     revisionLabels(dmObj, staleRev),
				Finalizers: []string{"decisionmodel.io/test-hold"},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}
		Expect(controllerutil.SetControllerReference(dmObj, pvc, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		// Put it into terminating state (deletionTimestamp set, finalizer holds it).
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		Eventually(func() bool {
			live := &corev1.PersistentVolumeClaim{}
			if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "term-store-" + staleRev}, live); gerr != nil {
				return false
			}
			return live.DeletionTimestamp != nil
		}, "3s", "50ms").Should(BeTrue(), "precondition: the PVC is terminating")

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "term-manifest-" + staleRev,
				Labels: revisionLabels(dmObj, staleRev),
			},
			BinaryData: map[string][]byte{manifestKey: manifestBody},
		}
		Expect(controllerutil.SetControllerReference(dmObj, cm, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())

		// GC: the terminating PVC is still listed, so its revision is collected and
		// the ConfigMap is deleted.
		Expect(r.gcRevisions(ctx, getDM("term"))).To(Succeed())
		Eventually(func() bool {
			c, gerr := manifestCM("term", staleRev)
			return apierrors.IsNotFound(gerr) || (gerr == nil && c.DeletionTimestamp != nil)
		}, "3s", "50ms").Should(BeTrue(),
			"a terminating PVC still drives deletion of its manifest ConfigMap")

		// Release the finalizer so the PVC can finish deleting (test cleanup).
		live := &corev1.PersistentVolumeClaim{}
		if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "term-store-" + staleRev}, live); gerr == nil {
			live.Finalizers = nil
			_ = k8sClient.Update(ctx, live)
		}
	})

	It("deletes the manifest ConfigMap before the store PVC (order), so a failed PVC delete loses nothing", func() {
		// The manifest ConfigMap must be deleted BEFORE its PVC: if the PVC delete
		// fails/crashes, the manifest is already gone (not orphaned, since GC cannot
		// list ConfigMaps). Intercept the PVC delete to fail it and assert the
		// manifest was deleted first.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		wc, werr := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(werr).NotTo(HaveOccurred())
		var pvcDeleteFailed atomic.Bool
		failing := interceptor.NewClient(wc, interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					pvcDeleteFailed.Store(true)
					return fmt.Errorf("induced PVC delete failure")
				}
				return cl.Delete(ctx, obj, opts...)
			},
		})
		r := &DecisionModelReconciler{
			Client: failing, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:  map[string]engine.Engine{"ollaya": eng},
			Prober:   &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: wantDigest, Device: "cpu"}},
			Recorder: events.NewFakeRecorder(64),
		}
		createDM("order")
		dmObj := getDM("order")
		staleRev := "0rder5tale1"
		// A stale revision's store PVC (no finalizer; the interceptor fails its delete)
		// and its manifest ConfigMap.
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "order-store-" + staleRev, Labels: revisionLabels(dmObj, staleRev),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			},
		}
		Expect(controllerutil.SetControllerReference(dmObj, pvc, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "order-manifest-" + staleRev, Labels: revisionLabels(dmObj, staleRev),
			},
			BinaryData: map[string][]byte{manifestKey: manifestBody},
		}
		Expect(controllerutil.SetControllerReference(dmObj, cm, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())

		// GC errors on the PVC delete, but the manifest ConfigMap was deleted first.
		err := r.gcRevisions(ctx, getDM("order"))
		Expect(err).To(HaveOccurred(), "the induced PVC delete failure surfaces")
		Expect(pvcDeleteFailed.Load()).To(BeTrue())
		_, cmErr := manifestCM("order", staleRev)
		Expect(apierrors.IsNotFound(cmErr)).To(BeTrue(),
			"the manifest ConfigMap is deleted before the PVC, so a failed PVC delete leaves no orphan")
	})

	It("repairs a corrupt owned manifest ConfigMap from the verified manifest", func() {
		// An owned ConfigMap whose stored bytes fail the digest check is deleted and
		// recreated from this reconcile's verified resolved manifest, restoring
		// exact-digest recovery instead of silently degrading to pull-by-tag.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		createDM("repair")
		rec(r, "repair")
		dm := getDM("repair")
		Expect(dm.Status.CandidateRevision).NotTo(BeNil())
		rev := dm.Status.CandidateRevision.Hash
		cm, err := manifestCM("repair", rev)
		Expect(err).NotTo(HaveOccurred())
		Expect(cm.BinaryData[manifestKey]).To(Equal(manifestBody))

		// Corrupt it (still owned).
		cm.BinaryData[manifestKey] = []byte("corrupt-not-the-manifest")
		Expect(k8sClient.Update(ctx, cm)).To(Succeed())

		// ensureManifestConfigMap on a reconcile that HAS the verified manifest
		// (resolvedManifest stashed, as the resolve reconcile does) repairs it:
		// delete + recreate back to the correct bytes.
		st := &reconcileState{base: dm.DeepCopy(), resolvedManifest: manifestBody}
		gctx := context.WithValue(ctx, reconcileStateKey{}, st)
		Expect(r.ensureManifestConfigMap(gctx, dm, rev, wantDigest)).To(Succeed())
		Eventually(func() []byte {
			c, gerr := manifestCM("repair", rev)
			if gerr != nil {
				return nil
			}
			return c.BinaryData[manifestKey]
		}, "3s", "50ms").Should(Equal(manifestBody), "the corrupt manifest is repaired from the verified bytes")
	})

	It("leaves a corrupt manifest and warns once when this reconcile has no verified bytes", func() {
		// With no verified manifest this reconcile (digest reused / hard pin / moved
		// tag), a corrupt owned ConfigMap is left as is and a single Warning is
		// emitted — no delete loop.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		rr := events.NewFakeRecorder(32)
		r := newR(eng)
		r.Recorder = rr
		createDM("warn")
		rec(r, "warn")
		rev := getDM("warn").Status.CandidateRevision.Hash
		cm, err := manifestCM("warn", rev)
		Expect(err).NotTo(HaveOccurred())
		cm.BinaryData[manifestKey] = []byte("corrupt")
		Expect(k8sClient.Update(ctx, cm)).To(Succeed())

		// No reconcile state (so no verified resolvedManifest, and events emit
		// immediately): warn + leave, do not delete.
		Expect(r.ensureManifestConfigMap(ctx, getDM("warn"), rev, wantDigest)).To(Succeed())
		// The ConfigMap is still present (not deleted).
		c, gerr := manifestCM("warn", rev)
		Expect(gerr).NotTo(HaveOccurred(), "corrupt ConfigMap left in place (no delete loop)")
		Expect(c.BinaryData[manifestKey]).To(Equal([]byte("corrupt")))
		// Exactly one ManifestInvalid Warning was emitted.
		warned := 0
		for drained := false; !drained; {
			select {
			case e := <-rr.Events:
				if strings.Contains(e, eventManifestInvalid) {
					warned++
				}
			default:
				drained = true
			}
		}
		Expect(warned).To(Equal(1), "one ManifestInvalid Warning, no loop")
	})
})

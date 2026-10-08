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
	"sync/atomic"

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

// Each revision's model manifest is persisted as an owned, revision-labelled
// ConfigMap before the prefetch Job, so a lost store (or a tag that moved
// upstream) can be rebuilt to EXACTLY the recorded digest from the recorded
// bytes rather than re-resolving the (now different) tag.
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

	It("seeds store recovery from the recorded manifest even after the tag moved", func() {
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

		// First reconcile: the manifest write fails, so the reconcile errors and
		// NO candidate digest is recorded (status.candidateRevision stays nil).
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

	It("persists the manifest ConfigMap even while the candidate is queued by the rollout budget", func() {
		// persistCandidateManifest runs before the fleet rollout-budget gate, so a
		// candidate that is queued (no slot free) still gets its durable manifest.
		// Its prefetch Job is not created while queued, but the bytes are safe.
		eng := newFakeEngine()
		eng.manifest = manifestBody
		r := newR(eng)
		// Budget of zero free slots: this DM is the only one and MaxConcurrentRollouts=1
		// with a reservation already held would queue it; simplest: set the budget to a
		// value already consumed by a reserved slot for another name.
		r.MaxConcurrentRollouts = 1
		r.WatchNamespaces = []string{namespace}
		// Hold the only slot with a reservation for another DM so this candidate
		// is queued behind the fleet budget (same mechanism restart_safety uses).
		r.budgetReservations = map[string]string{namespace + "/other": "someother"}

		createDM("queued")
		rec(r, "queued") // reaches persist, then the budget gate queues it

		dm := getDM("queued")
		Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhasePending), "queued behind the budget")
		Expect(dm.Status.CandidateRevision).To(BeNil(), "not admitted while queued")
		rev := RevisionHash(dm.Spec, wantDigest, fakeImage)
		cm, err := manifestCM("queued", rev)
		Expect(err).NotTo(HaveOccurred(), "the manifest is persisted before the budget gate")
		Expect(cm.BinaryData[manifestKey]).To(Equal(manifestBody))
		// No prefetch Job yet: the candidate is still queued.
		job := &batchv1.Job{}
		Expect(apierrors.IsNotFound(
			k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName("queued", rev)}, job),
		)).To(BeTrue(), "no prefetch Job while queued")
	})
})

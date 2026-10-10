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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// This suite covers the harsher registry seam: a Ready stable whose OWN model
// host leaves the allow-list after an operator restart. Unlike a candidate on a
// disallowed host (fault_flag_flip_envtest_test.go item 2), here spec.model still
// resolves to the stable's own revision, so the guard fires on the spec==stable
// path: preflightSecurity returns bad=true (decisionmodel_controller.go:1010),
// the object is NOT failed because a stable is serving, and maintainStableAfterPreflight
// (decisionmodel_controller.go:1294) records Resolved=False/RegistryNotAllowed,
// emits one CandidateRejected Warning (deduped by the `changed` check), and runs
// the stable path, which renders the stable from status.stableRevision so its
// template is byte-identical and the Deployment is not rolled.
var _ = Describe("the allow-list drops the stable's own registry host", func() {
	const (
		// H is the stable's own model host.
		candHost  = "custom.registry.io"
		candModel = candHost + "/ns/laya:en"
	)

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
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	cond := func(name, typ string) *metav1.Condition {
		return meta.FindStatusCondition(getDM(name).Status.Conditions, typ)
	}
	stableDep := func(name, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, dep)).To(Succeed())
		return dep
	}
	createReadyPod := func(dmName, rev, podName, ip string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{decisionmodelv1alpha1.LabelName: dmName, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = ip
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	podGateTrue := func(podName string) bool {
		p := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, p); err != nil {
			return false
		}
		for _, c := range p.Status.Conditions {
			if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
				return c.Status == corev1.ConditionTrue
			}
		}
		return false
	}
	// countEvents drains the recorder and counts Events whose text contains sub.
	countEvents := func(r *DecisionModelReconciler, sub string) int {
		fr := r.Recorder.(*events.FakeRecorder)
		n := 0
		for {
			select {
			case e := <-fr.Events:
				if strings.Contains(e, sub) {
					n++
				}
			default:
				return n
			}
		}
	}

	newReconciler := func(allowedRegistries []string) *DecisionModelReconciler {
		eng := &deciderFakeEngine{fakeEngine: newFakeEngine(), choice: "billing"}
		return &DecisionModelReconciler{
			Client:            k8sClient,
			APIReader:         k8sClient,
			Scheme:            k8sClient.Scheme(),
			Engines:           map[string]engine.Engine{"ollaya": eng},
			Prober:            &fakeProber{loaded: engine.Loaded{Name: candModel, Digest: defaultDigest, Device: "cpu"}},
			Recorder:          events.NewFakeRecorder(256),
			AllowedRegistries: allowedRegistries,
		}
	}

	// driveReady brings a single-replica DM on host H to Ready and returns the
	// stable revision hash. The allow-list must contain H.
	driveReady := func(r *DecisionModelReconciler, name string) string {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: candModel, Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		rec(r, name)
		rev := getDM(name).Status.CandidateRevision.Hash
		markPrefetchComplete(ctx, namespace, name, rev)
		rec(r, name)
		createReadyPod(name, rev, name+"-stable-0", "10.0.2.10")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(r, name)
			return getDM(name).Status.Phase
		}, "5s", "25ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		return getDM(name).Status.StableRevision.Hash
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("stable-host-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// steady state. The stable's host leaves the allow-list; a plain
	// reconcile must keep the stable serving with no roll, surface the refusal on
	// Resolved=False/RegistryNotAllowed, and emit EXACTLY ONE Warning across five
	// reconciles (the Event is deduped by maintainStableAfterPreflight's `changed`
	// check, decisionmodel_controller.go:1256-1273).
	It("keeps the stable serving with no roll and one deduped Warning across five reconciles", func() {
		name := "steady"
		rPermissive := newReconciler([]string{defaultAllowedRegistry, candHost})
		stableRev := driveReady(rPermissive, name)
		depBefore := stableDep(name, stableRev)
		genBefore := depBefore.Generation
		tmplBefore := depBefore.Spec.Template.DeepCopy()

		By("the operator restarts with an allow-list that no longer contains H")
		rStrict := newReconciler([]string{defaultAllowedRegistry})
		// Drain the startup noise so the Warning count below is only the refusal.
		_ = countEvents(rStrict, "")

		warnings := 0
		for i := 0; i < 5; i++ {
			rec(rStrict, name)
			warnings += countEvents(rStrict, eventCandidateRejected)
		}

		By("Resolved=False/RegistryNotAllowed; the object is not Failed")
		dm := getDM(name)
		Expect(dm.Status.Phase).NotTo(Equal(decisionmodelv1alpha1.PhaseFailed),
			"a serving stable must never be failed by its host leaving the allow-list")
		rc := cond(name, decisionmodelv1alpha1.ConditionResolved)
		Expect(rc).NotTo(BeNil())
		Expect(rc.Status).To(Equal(metav1.ConditionFalse))
		Expect(rc.Reason).To(Equal(reasonRegistryNotAllowed))

		By("exactly one Warning across five reconciles (deduped)")
		Expect(warnings).To(Equal(1), "the refusal Warning must not repeat every reconcile")

		By("the stable Deployment is not rolled and its template is byte-identical")
		depAfter := stableDep(name, stableRev)
		Expect(depAfter.Generation).To(Equal(genBefore), "the stable must not roll")
		Expect(depAfter.Spec.Template).To(Equal(*tmplBefore), "the stable Pod template must be unchanged")
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(stableRev))
		Expect(podGateTrue(name+"-stable-0")).To(BeTrue(), "the serving stable Pod stays model-ready")
	})

	// the stable store is lost while H is disallowed. Store recovery
	// re-checks the recorded registry and the model's own host against the current
	// allow-list: the recovery prefetch is NOT created, Degraded=RegistryNotAllowed
	// with one deduped Warning, and re-allowing H lets recovery proceed and the
	// stable return Ready on the same hash.
	It("refuses store recovery for a disallowed host and recovers once the host is re-allowed", func() {
		name := "lost"
		rPermissive := newReconciler([]string{defaultAllowedRegistry, candHost})
		stableRev := driveReady(rPermissive, name)

		By("the operator restarts with H removed, then the stable store PVC and Pod are deleted")
		rStrict := newReconciler([]string{defaultAllowedRegistry})
		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM(name), stableRev)}, pvc)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		// Strip the pvc-protection finalizer so the PVC actually disappears under
		// envtest (no kubelet/GC to drop it); production reaches NotFound the same
		// way once the last mounting Pod is gone.
		cur := &corev1.PersistentVolumeClaim{}
		if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: storeNameRev(getDM(name), stableRev)}, cur); gerr == nil && len(cur.Finalizers) > 0 {
			cur.Finalizers = nil
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		}
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-stable-0"}, pod)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
		// The original prefetch Job (Complete, from driveReady) is part of the lost
		// store: delete it so the assertion below checks that no NEW recovery Job is
		// created while the host is disallowed.
		oldJob := &batchv1.Job{}
		if gerr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(getDM(name), stableRev)}, oldJob); gerr == nil {
			bg := metav1.DeletePropagationBackground
			Expect(k8sClient.Delete(ctx, oldJob, &client.DeleteOptions{PropagationPolicy: &bg})).To(Succeed())
		}

		By("recovery prefetch is NOT created and Degraded=RegistryNotAllowed with one Warning")
		warnings := 0
		for i := 0; i < 5; i++ {
			rec(rStrict, name)
			warnings += countEvents(rStrict, "store recovery for revision")
		}
		job := &batchv1.Job{}
		recoveryJobErr := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(getDM(name), stableRev)}, job)
		Expect(recoveryJobErr).To(HaveOccurred(), "no recovery prefetch Job must be created for a disallowed host")
		deg := cond(name, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonRegistryNotAllowed))
		Expect(warnings).To(Equal(1), "the refusal Warning must not repeat every reconcile")

		By("re-allowing H lets recovery proceed and the stable returns Ready on the same hash")
		rBack := newReconciler([]string{defaultAllowedRegistry, candHost})
		rev2 := stableRev
		// Reconcile until the recovery prefetch Job exists (re-allow recreates the
		// PVC on one pass, then the Job on the next), then complete it.
		Eventually(func() bool {
			rec(rBack, name)
			return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(getDM(name), rev2)}, &batchv1.Job{}) == nil
		}, "5s", "25ms").Should(BeTrue(), "re-allowing the host lets the recovery prefetch Job be created")
		markPrefetchComplete(ctx, namespace, name, rev2)
		rec(rBack, name)
		createReadyPod(name, rev2, name+"-stable-1", "10.0.2.11")
		Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
			rec(rBack, name)
			return getDM(name).Status.Phase
		}, "5s", "25ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(stableRev), "recovery keeps the same revision hash")
	})

	// with H disallowed, spec.model is changed to an allowed host. The new
	// candidate proceeds normally (its host IS allowed), and promotion retires H:
	// the new revision becomes the stable.
	It("lets a spec.model change to an allowed host roll forward and retire the disallowed host", func() {
		name := "migrate"
		rPermissive := newReconciler([]string{defaultAllowedRegistry, candHost})
		stableRev := driveReady(rPermissive, name)

		By("the operator restarts with H removed (the stable keeps serving)")
		rStrict := newReconciler([]string{defaultAllowedRegistry})
		rec(rStrict, name)
		Expect(getDM(name).Status.StableRevision.Hash).To(Equal(stableRev))
		Expect(cond(name, decisionmodelv1alpha1.ConditionResolved).Reason).To(Equal(reasonRegistryNotAllowed))

		By("spec.model is changed to a model on the still-allowed default host")
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = "laya:multilingual" // bare name -> default host (ollaya.dev), allowed
		})).To(Succeed())
		rec(rStrict, name)
		candRev := getDM(name).Status.CandidateRevision.Hash
		Expect(candRev).NotTo(Equal(stableRev), "the host migration is a new revision")

		By("the candidate proceeds: Resolved recovers to True for the allowed host")
		Expect(cond(name, decisionmodelv1alpha1.ConditionResolved).Status).To(Equal(metav1.ConditionTrue),
			"an allowed-host candidate clears the RegistryNotAllowed refusal")

		By("drive the candidate through to promotion; the new revision retires H")
		markPrefetchComplete(ctx, namespace, name, candRev)
		rec(rStrict, name)
		createReadyPod(name, candRev, name+"-cand-0", "10.0.2.20")
		Eventually(func() string {
			rec(rStrict, name)
			if sr := getDM(name).Status.StableRevision; sr != nil {
				return sr.Hash
			}
			return ""
		}, "6s", "25ms").Should(Equal(candRev), "promotion must retire the disallowed-host revision")
		Expect(getDM(name).Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseReady))
	})

	// a host-less name recorded on a mirror. The stable name is "laya:en"
	// (no explicit host); it was prefetched from mirror M (recorded on the
	// revision). After an operator restart with the default --ollaya-registry flag
	// but an allow-list of exactly [M], a lost-store recovery must PROCEED: the
	// recorded registry M is allowed, and the model-name host check must NOT run
	// for a host-less name (it would resolve to the default flag host, which is not
	// in [M], and wrongly refuse). The recovery recreates the annotated PVC and a
	// prefetch Job.
	It("recovers a host-less stable recorded on a mirror that is still allowed", func() {
		name := "mirror"
		const mirrorURL = "https://mirror.internal:5000"
		const mirrorHost = "mirror.internal:5000"
		// Reconciler with the DEFAULT registry flag but an allow-list of only the
		// mirror host; insecure allowed is irrelevant (https).
		r := &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:           map[string]engine.Engine{"ollaya": newFakeEngine()},
			Prober:            &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}},
			AllowedRegistries: []string{mirrorHost},
			Recorder:          events.NewFakeRecorder(128),
		}
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		stable := &decisionmodelv1alpha1.RevisionStatus{
			Hash: "r1", Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu",
			Image: fakeImage, Registry: mirrorURL, // host-less name, pulled from the mirror
		}
		dm.Status.StableRevision = stable
		claim := storeNameRev(dm, stable.Hash)

		By("recovery recreates the annotated PVC and then a prefetch Job (mirror still allowed)")
		// First pass: PVC missing -> recreate (annotated), handled.
		handled, _, _, err := r.recoverStableStore(ctx, dm, r.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		// Next pass: PVC present + recovering, no Job -> a prefetch Job is created
		// (the recorded mirror host is allowed; the host-less name does not trigger
		// the model-host check).
		handled, _, _, err = r.recoverStableStore(ctx, dm, r.Engines["ollaya"], stable, claim, false)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())

		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: prefetchName(dm, "r1")}, &batchv1.Job{})).
			To(Succeed(), "recovery must proceed for a host-less name whose recorded mirror is allowed")
		deg := cond(name, decisionmodelv1alpha1.ConditionDegraded)
		if deg != nil {
			Expect(deg.Reason).NotTo(Equal(reasonRegistryNotAllowed),
				"a host-less name on an allowed mirror must not be refused")
		}
	})
})

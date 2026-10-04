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
	"sync/atomic"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

var _ = Describe("crash-safe promotion and real readiness", func() {
	const (
		digest2 = "d2d2290000000000000000000000000000000000000000000000000000000000"
		digest3 = "d3d3290000000000000000000000000000000000000000000000000000000000"
	)

	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	one := int32(1)

	// failDMStatus, when > 0, makes that many DecisionModel status patches fail
	// with a server error (Pod gate patches, which also use Status().Patch, pass).
	var failDMStatus atomic.Int32
	var dmStatusPatches atomic.Int32

	type harness struct {
		r    *DecisionModelReconciler
		fake *fakeEngine
		pr   *fakeProber
	}
	newH := func() *harness {
		fake := newFakeEngine()
		pr := &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
		wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		c := interceptor.NewClient(wc, interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object,
				patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, isDM := obj.(*decisionmodelv1alpha1.DecisionModel); isDM && sub == "status" {
					dmStatusPatches.Add(1)
					if failDMStatus.Load() > 0 {
						failDMStatus.Add(-1)
						return apierrors.NewInternalError(errors.New("injected status write failure"))
					}
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
		return &harness{
			r: &DecisionModelReconciler{
				Client: c, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
				Engines:  map[string]engine.Engine{"ollaya": fake},
				Prober:   pr,
				Recorder: events.NewFakeRecorder(256),
			},
			fake: fake, pr: pr,
		}
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(h *harness, name string) error {
		_, err := h.r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
		return err
	}
	mustRec := func(h *harness, name string) {
		Expect(rec(h, name)).To(Succeed())
	}
	serviceRev := func(name string) string {
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, svc)).To(Succeed())
		return svc.Spec.Selector[decisionmodelv1alpha1.LabelRevision]
	}
	depExists := func(name, rev string) bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, &appsv1.Deployment{})
		return err == nil
	}
	jobDone := func(name, rev string) {
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
	// mkPod creates a serving Pod. gate/ready control the two conditions
	// independently so a spec can build "gate True but not Ready".
	mkPod := func(name, rev, podName string, gate, ready bool) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName: name, decisionmodelv1alpha1.LabelRevision: rev},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.95"
		conds := []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionTrue}}
		if gate {
			conds = append(conds, corev1.PodCondition{
				Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue})
		}
		if ready {
			conds = append(conds, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
		}
		pod.Status.Conditions = conds
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		return pod
	}
	newDM := func(name string) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: &one},
		})).To(Succeed())
	}
	// stableA brings a DM to a stable revision A with a ready Pod.
	stableA := func(h *harness, name string) string {
		mustRec(h, name)
		revA := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
		jobDone(name, revA)
		mustRec(h, name)
		mkPod(name, revA, name+"-pod-a", true, true)
		mustRec(h, name)
		mustRec(h, name)
		Expect(getDM(name).Status.StableRevision).NotTo(BeNil())
		Expect(serviceRev(name)).To(Equal(revA))
		return revA
	}
	// switchModel changes the model (new digest) and returns the new revision hash.
	switchModel := func(h *harness, name, model, digest string) string {
		h.fake.mu.Lock()
		h.fake.digest = digest
		h.fake.mu.Unlock()
		Expect(updateDM(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Spec.Model = model
		})).To(Succeed())
		h.pr.loaded = engine.Loaded{Name: model, Digest: digest, Device: "cpu"}
		return RevisionHash(func() decisionmodelv1alpha1.DecisionModelSpec {
			s := getDM(name).Spec
			return s
		}(), digest, fakeImage)
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("promosafety-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		failDMStatus.Store(0)
		dmStatusPatches.Store(0)
	})

	// (a) The blocker: a failed status write around a promotion, then churn.
	Describe("Service follows persisted status (item 1)", func() {
		It("never moves the Service when the status write fails, and never points it at a deleted revision after churn", func() {
			h := newH()
			newDM("sv")
			revA := stableA(h, "sv")

			// B becomes a ready candidate.
			revB := switchModel(h, "sv", "kev:en", digest2)
			mustRec(h, "sv")
			jobDone("sv", revB)
			mustRec(h, "sv")
			mkPod("sv", revB, "sv-pod-b", true, true)

			// The reconcile that would promote B loses its status write.
			failDMStatus.Store(1)
			Expect(rec(h, "sv")).NotTo(Succeed(), "the injected status failure surfaces")
			Expect(failDMStatus.Load()).To(BeZero(), "the injected failure was consumed by the promoting reconcile")
			Expect(getDM("sv").Status.StableRevision.Hash).To(Equal(revA), "promotion was not persisted")
			Expect(serviceRev("sv")).To(Equal(revA),
				"the Service must NOT move to B before status records B as stable")

			// Now the spec changes again, B -> C, before anyone retried the promotion.
			revC := switchModel(h, "sv", "jevk5:en", digest3)
			Expect(revC).NotTo(Equal(revB))
			for i := 0; i < 3; i++ {
				mustRec(h, "sv")
			}
			// The Service still sits on A, which is persisted as stable and ready; the
			// revision it selects was never garbage-collected.
			Expect(serviceRev("sv")).To(Equal(revA))
			Expect(depExists("sv", revA)).To(BeTrue(), "the revision the Service selects still exists")
			Expect(getDM("sv").Status.StableRevision.Hash).To(Equal(revA))
			// And the Service never pointed at a revision that does not exist.
			Expect(depExists("sv", serviceRev("sv"))).To(BeTrue())
		})

		It("re-asserts the Service to the persisted stable on the candidate path", func() {
			h := newH()
			newDM("ra")
			revA := stableA(h, "ra")
			// Someone (or a half-finished earlier reconcile) left the Service elsewhere.
			Expect(updateService(ctx, namespace, "ra", func(s *corev1.Service) {
				s.Spec.Selector = map[string]string{
					decisionmodelv1alpha1.LabelName: "ra", decisionmodelv1alpha1.LabelRevision: "ghost"}
			})).To(Succeed())
			// A spec change puts the DM on the candidate path.
			switchModel(h, "ra", "kev:en", digest2)
			mustRec(h, "ra")
			Expect(getDM("ra").Status.CandidateRevision).NotTo(BeNil(), "we are on the candidate path")
			Expect(serviceRev("ra")).To(Equal(revA), "the candidate path puts the Service back on the stable")
		})

		It("GC keeps the revision the live Service selects even if status disagrees", func() {
			h := newH()
			newDM("gk")
			revA := stableA(h, "gk")
			// Forge the bad state the blocker described: status names another stable
			// while the Service still selects A (and A's Deployment still exists).
			Expect(updateDMStatus(ctx, namespace, "gk", func(d *decisionmodelv1alpha1.DecisionModel) {
				d.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{Hash: "zzzzzzzzzz",
					Engine: "ollaya", Model: "laya:en", Digest: defaultDigest, Device: "cpu", Image: fakeImage,
					Placement: placementNone}
			})).To(Succeed())
			dm := getDM("gk")
			Expect(h.r.gcRevisions(ctx, dm)).To(Succeed())
			Expect(depExists("gk", revA)).To(BeTrue(), "the revision the Service selects is never collected")
		})

		It("still collects the demoted revision once the Service has moved and the grace elapsed", func() {
			h := newH()
			newDM("gc")
			revA := stableA(h, "gc")
			revB := switchModel(h, "gc", "kev:en", digest2)
			mustRec(h, "gc")
			jobDone("gc", revB)
			mustRec(h, "gc")
			mkPod("gc", revB, "gc-pod-b", true, true)
			mustRec(h, "gc") // promote
			Expect(serviceRev("gc")).To(Equal(revB))
			Expect(depExists("gc", revA)).To(BeTrue(), "within the grace window")
			h.r.Now = func() time.Time { return time.Now().Add(2 * promoteGrace) }
			mustRec(h, "gc")
			Expect(depExists("gc", revA)).To(BeFalse(), "collected after the grace")
		})
	})

	// (b) PodReady before switching.
	Describe("PodReady before switching traffic (item 2)", func() {
		It("does not promote onto a Pod whose gate is True but PodReady is False", func() {
			withoutKubelet()
			h := newH()
			newDM("nr")
			mustRec(h, "nr")
			rev := RevisionHash(getDM("nr").Spec, defaultDigest, fakeImage)
			jobDone("nr", rev)
			mustRec(h, "nr")
			mkPod("nr", rev, "nr-pod", true, false) // gate True, PodReady False
			for i := 0; i < 3; i++ {
				mustRec(h, "nr")
			}
			dm := getDM("nr")
			Expect(dm.Status.StableRevision).To(BeNil(), "no promotion while the Pod is not Ready")
			Expect(dm.Status.Phase).To(Equal(decisionmodelv1alpha1.PhaseStarting))
			Expect(dm.Status.Replicas.ModelReady).To(BeZero())
			var svc corev1.Service
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "nr"}, &svc)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the Service is not created/moved for a Pod that is not Ready")

			// Kubernetes sets PodReady: the next reconcile promotes.
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "nr-pod"}, pod)).To(Succeed())
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
			Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
			mustRec(h, "nr")
			Expect(getDM("nr").Status.StableRevision).NotTo(BeNil())
			Expect(serviceRev("nr")).To(Equal(rev))
		})

		It("does not count a Pod whose gate was patched in the same reconcile", func() {
			withoutKubelet()
			h := newH()
			newDM("sr")
			mustRec(h, "sr")
			rev := RevisionHash(getDM("sr").Spec, defaultDigest, fakeImage)
			jobDone("sr", rev)
			mustRec(h, "sr")
			mkPod("sr", rev, "sr-pod", false, false) // containers ready, no gate yet
			mustRec(h, "sr")                         // probe patches the gate True
			Expect(getDM("sr").Status.Replicas.ModelReady).To(BeZero(), "patching the gate does not count the Pod")
			Expect(getDM("sr").Status.StableRevision).To(BeNil())
		})
	})

	// (c) Terminating Pods.
	Describe("terminating Pods (item 2)", func() {
		It("does not count a gated, Ready Pod that is terminating", func() {
			h := newH()
			newDM("tp")
			revA := stableA(h, "tp")
			Expect(getDM("tp").Status.Replicas.ModelReady).To(Equal(int32(1)))
			// Terminate the only Pod; a finalizer keeps it visible with a deletionTimestamp.
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "tp-pod-a"}, pod)).To(Succeed())
			pod.Finalizers = []string{"test/keep"}
			Expect(k8sClient.Update(ctx, pod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				p := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "tp-pod-a"}, p); err == nil {
					p.Finalizers = nil
					_ = k8sClient.Update(ctx, p)
				}
			})
			mustRec(h, "tp")
			Expect(getDM("tp").Status.Replicas.ModelReady).To(BeZero(), "a terminating Pod is not model-ready")
			_ = revA
		})
	})

	// (d) Pinned.
	Describe("pinned (item 3)", func() {
		It("gates a loaded-but-unpinned model False with reason NotPinned and emits an Event", func() {
			h := newH()
			h.pr.unpinned = true
			newDM("np")
			mustRec(h, "np")
			rev := RevisionHash(getDM("np").Spec, defaultDigest, fakeImage)
			jobDone("np", rev)
			mustRec(h, "np")
			mkPod("np", rev, "np-pod", false, false)
			mustRec(h, "np")
			pod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "np-pod"}, pod)).To(Succeed())
			var gate *corev1.PodCondition
			for i := range pod.Status.Conditions {
				if string(pod.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
					gate = &pod.Status.Conditions[i]
				}
			}
			Expect(gate).NotTo(BeNil())
			Expect(gate.Status).To(Equal(corev1.ConditionFalse))
			Expect(gate.Reason).To(Equal(reasonNotPinned))
			Expect(getDM("np").Status.StableRevision).To(BeNil(), "an unpinned model is never promoted")

			fr := h.r.Recorder.(*events.FakeRecorder)
			found := false
			for len(fr.Events) > 0 {
				if e := <-fr.Events; strings.Contains(e, reasonNotPinned) {
					found = true
				}
			}
			Expect(found).To(BeTrue(), "a NotPinned Event is emitted")

			// Once the runtime pins it, the gate recovers.
			h.pr.unpinned = false
			Eventually(func() corev1.ConditionStatus {
				_ = rec(h, "np")
				p := &corev1.Pod{}
				_ = k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "np-pod"}, p)
				for _, c := range p.Status.Conditions {
					if string(c.Type) == decisionmodelv1alpha1.ModelReadyGate {
						return c.Status
					}
				}
				return ""
			}, "5s", "50ms").Should(Equal(corev1.ConditionTrue))
		})

		It("a pin lost later flips an already-True gate through the periodic re-inspect", func() {
			pr := &reinspectProber{fakeProber: &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}}
			h := newH()
			h.r.Prober = pr
			clock := time.Now()
			h.r.Now = func() time.Time { return clock }
			newDM("lp")
			revA := stableA(h, "lp")
			_ = revA
			// The runtime unpins the model (keep_alive expiry) without a restart.
			pr.unpinned = true
			clock = clock.Add(regateInterval + time.Second)
			mustRec(h, "lp")
			p := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "lp-pod-a"}, p)).To(Succeed())
			var gate *corev1.PodCondition
			for i := range p.Status.Conditions {
				if string(p.Status.Conditions[i].Type) == decisionmodelv1alpha1.ModelReadyGate {
					gate = &p.Status.Conditions[i]
				}
			}
			Expect(gate).NotTo(BeNil())
			Expect(gate.Status).To(Equal(corev1.ConditionFalse))
			Expect(gate.Reason).To(Equal(reasonNotPinned))
		})
	})

	// (e) Terminating PVC.
	Describe("terminating store PVC (item 4)", func() {
		It("does not reuse a PVC that is being deleted, and creates a fresh one once it is gone", func() {
			h := newH()
			newDM("tv")
			mustRec(h, "tv")
			dm := getDM("tv")
			rev := RevisionHash(dm.Spec, defaultDigest, fakeImage)
			pvcKey := types.NamespacedName{Namespace: namespace, Name: storeNameRev(dm, rev)}
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
			uid := pvc.UID

			// Make it Terminating: the kubernetes.io/pvc-protection finalizer that
			// envtest adds keeps it around with a deletionTimestamp.
			Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
			Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
			Expect(pvc.DeletionTimestamp).NotTo(BeNil(), "the PVC is Terminating")

			res, err := h.r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "tv"}})
			Expect(err).NotTo(HaveOccurred(), "a terminating store is a wait, not an error")
			Expect(res.RequeueAfter).To(Equal(storeTerminatingRequeue))
			// Nothing was built on top of the dying claim: the Job must not have
			// been recreated against it, and the PVC is still the old one.
			Expect(k8sClient.Get(ctx, pvcKey, pvc)).To(Succeed())
			Expect(pvc.UID).To(Equal(uid))

			// The claim goes away: the next reconcile creates a NEW one.
			pvc.Finalizers = nil
			Expect(k8sClient.Update(ctx, pvc)).To(Succeed())
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
			}, "5s", "50ms").Should(BeTrue())
			mustRec(h, "tv")
			fresh := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, pvcKey, fresh)).To(Succeed())
			Expect(fresh.UID).NotTo(Equal(uid), "a fresh claim, not the terminated one")
			Expect(fresh.DeletionTimestamp).To(BeNil())
		})
	})
})

func updateService(ctx context.Context, ns, name string, mutate func(*corev1.Service)) error {
	svc := &corev1.Service{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, svc); err != nil {
		return err
	}
	mutate(svc)
	return k8sClient.Update(ctx, svc)
}

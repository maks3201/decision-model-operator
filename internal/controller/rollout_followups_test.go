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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// The default GPU toleration is skipped exactly when an existing toleration
// tolerates the nvidia.com/gpu:NoSchedule taint (Kubernetes' own matching).
func TestApplyGPUTolerationDedupe(t *testing.T) {
	tol := func(key string, op corev1.TolerationOperator, value string, effect corev1.TaintEffect) corev1.Toleration {
		return corev1.Toleration{Key: key, Operator: op, Value: value, Effect: effect}
	}
	tests := []struct {
		name      string
		device    string
		existing  []corev1.Toleration
		wantAdded bool
	}{
		{"cpu never adds", engine.DeviceCPU, nil, false},
		{"none present: added", engine.DeviceCUDA, nil, true},
		{"NoExecute-only for the key does not tolerate NoSchedule: added", engine.DeviceCUDA,
			[]corev1.Toleration{tol(gpuTaintKey, corev1.TolerationOpExists, "", corev1.TaintEffectNoExecute)}, true},
		{"Equal with a value never matches the empty-value taint: added", engine.DeviceCUDA,
			[]corev1.Toleration{tol(gpuTaintKey, corev1.TolerationOpEqual, "present", corev1.TaintEffectNoSchedule)}, true},
		{"other key: added", engine.DeviceCUDA,
			[]corev1.Toleration{tol("dedicated", corev1.TolerationOpExists, "", corev1.TaintEffectNoSchedule)}, true},
		{"wildcard (empty key, Exists): not added", engine.DeviceCUDA,
			[]corev1.Toleration{tol("", corev1.TolerationOpExists, "", "")}, false},
		{"exact match: not added", engine.DeviceCUDA,
			[]corev1.Toleration{tol(gpuTaintKey, corev1.TolerationOpExists, "", corev1.TaintEffectNoSchedule)}, false},
		{"key with empty effect tolerates all effects: not added", engine.DeviceCUDA,
			[]corev1.Toleration{tol(gpuTaintKey, corev1.TolerationOpExists, "", "")}, false},
		{"Equal with empty value matches the empty-value taint: not added", engine.DeviceCUDA,
			[]corev1.Toleration{tol(gpuTaintKey, corev1.TolerationOpEqual, "", corev1.TaintEffectNoSchedule)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &corev1.PodSpec{Tolerations: append([]corev1.Toleration(nil), tt.existing...)}
			applyGPUToleration(spec, tt.device)
			wantLen := len(tt.existing)
			if tt.wantAdded {
				wantLen++
			}
			if len(spec.Tolerations) != wantLen {
				t.Fatalf("tolerations = %v, want %d entries", spec.Tolerations, wantLen)
			}
			for i, e := range tt.existing { // the user's tolerations are kept, in order
				if spec.Tolerations[i] != e {
					t.Errorf("user toleration %d changed: %v -> %v", i, e, spec.Tolerations[i])
				}
			}
			if tt.wantAdded {
				got := spec.Tolerations[len(spec.Tolerations)-1]
				want := tol(gpuTaintKey, corev1.TolerationOpExists, "", corev1.TaintEffectNoSchedule)
				if got != want {
					t.Errorf("added toleration = %v, want %v", got, want)
				}
			}
		})
	}
}

var _ = Describe("rollout follow-ups", func() {
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
	markJobComplete := func(dmName, rev string) error {
		job := &batchv1.Job{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job); err != nil {
			return err
		}
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		return k8sClient.Status().Update(ctx, job)
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
		pod.Status.PodIP = "10.0.0.70"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	newDM := func(name string, replicas int32) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(replicas),
				Cache: &decisionmodelv1alpha1.CacheSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}},
			},
		})).To(Succeed())
	}
	fakeProberOK := func() *fakeProber {
		return &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}}
	}
	pdbGet := func(name, rev string) (*policyv1.PodDisruptionBudget, error) {
		pdb := &policyv1.PodDisruptionBudget{}
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name + "-" + rev}, pdb)
		return pdb, err
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("followups-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// Item 3: a steady-state single-replica DM must not send PDB deletes.
	Describe("PDB delete only when it exists", func() {
		It("issues no PDB Delete at replicas=1, and exactly one when a PDB has to be removed", func() {
			var pdbDeletes atomic.Int32
			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			counting := interceptor.NewClient(watchClient, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*policyv1.PodDisruptionBudget); ok {
						pdbDeletes.Add(1)
					}
					return c.Delete(ctx, obj, opts...)
				},
			})
			r := &DecisionModelReconciler{
				Client: counting, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
				Engines:  map[string]engine.Engine{"ollaya": newFakeEngine()},
				Prober:   fakeProberOK(),
				Recorder: events.NewFakeRecorder(64),
			}
			rec := func(name string) {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
				Expect(err).NotTo(HaveOccurred())
			}
			drive := func(name string, replicas int) string {
				rec(name)
				rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
				Expect(markJobComplete(name, rev)).To(Succeed())
				rec(name)
				for i := 0; i < replicas; i++ {
					createGatedPod(name, rev, fmt.Sprintf("%s-pod-%d", name, i))
				}
				rec(name)
				rec(name)
				return rev
			}

			// replicas=1: five stable reconciles, no PDB exists, no Delete is sent.
			newDM("one", 1)
			rev1 := drive("one", 1)
			for i := 0; i < 5; i++ {
				rec("one")
			}
			Expect(pdbDeletes.Load()).To(BeZero(), "no Delete call when there is no PDB")
			_, err = pdbGet("one", rev1)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			// replicas 2 -> 1 with a PDB present: exactly one Delete, then none.
			newDM("two", 2)
			rev2 := drive("two", 2)
			_, err = pdbGet("two", rev2)
			Expect(err).NotTo(HaveOccurred(), "replicas=2 has a PDB")
			Expect(updateDM(ctx, namespace, "two", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Replicas = int32Ptr(1)
			})).To(Succeed())
			for i := 0; i < 4; i++ {
				rec("two")
			}
			Expect(pdbDeletes.Load()).To(Equal(int32(1)), "one Delete for the one PDB that existed")
			_, err = pdbGet("two", rev2)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})

	// Item 1: only a running manager can show that SetupWithManager registers the
	// PDB watch; a direct Reconcile call cannot. The manager is scoped to this
	// spec's namespace and stopped afterwards so it never touches other specs.
	Describe("PDB watch (Owns)", func() {
		It("recreates a deleted PDB without any other trigger", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:  k8sClient.Scheme(),
				Metrics: metricsserver.Options{BindAddress: "0"},
				Cache:   cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
				// Another suite may have registered a controller named "decisionmodel".
				Controller: ctrlconfig.Controller{SkipNameValidation: func() *bool { b := true; return &b }()},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect((&DecisionModelReconciler{
				Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
				Engines: map[string]engine.Engine{"ollaya": newFakeEngine()},
				Prober:  fakeProberOK(),
			}).SetupWithManager(mgr)).To(Succeed())

			mgrCtx, cancelMgr := context.WithCancel(ctx)
			defer cancelMgr()
			done := make(chan struct{})
			go func() {
				defer GinkgoRecover()
				defer close(done)
				_ = mgr.Start(mgrCtx)
			}()
			DeferCleanup(func() {
				cancelMgr()
				Eventually(done, "10s").Should(BeClosed())
			})

			newDM("w", 2)
			rev := RevisionHash(getDM("w").Spec, defaultDigest, fakeImage)

			// Drive the rollout by changing the world; the manager reconciles.
			Eventually(func() error { return markJobComplete("w", rev) }, "20s", "100ms").Should(Succeed())
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "w-" + rev}, &appsv1.Deployment{})
			}, "20s", "100ms").Should(Succeed())
			createGatedPod("w", rev, "w-pod-0")
			createGatedPod("w", rev, "w-pod-1")
			Eventually(func() decisionmodelv1alpha1.DecisionModelPhase {
				return getDM("w").Status.Phase
			}, "20s", "100ms").Should(Equal(decisionmodelv1alpha1.PhaseReady))

			// The PDB is created by the stable-path reconcile. envtest has no
			// Deployment controller, so nothing re-triggers the DM after promotion:
			// nudge it with an annotation until the PDB exists.
			var pdb *policyv1.PodDisruptionBudget
			nudge := 0
			Eventually(func() error {
				nudge++
				_ = updateDM(ctx, namespace, "w", func(dm *decisionmodelv1alpha1.DecisionModel) {
					if dm.Annotations == nil {
						dm.Annotations = map[string]string{}
					}
					dm.Annotations["test/nudge"] = fmt.Sprint(nudge)
				})
				var gerr error
				pdb, gerr = pdbGet("w", rev)
				return gerr
			}, "20s", "250ms").Should(Succeed())
			firstUID := pdb.UID

			// Let in-flight reconciles drain, so a recreate cannot be one of them.
			time.Sleep(time.Second)

			// From here on nothing touches the DM, Pods, Jobs or Deployments: only
			// the PDB delete event can bring the PDB back.
			Expect(k8sClient.Delete(ctx, pdb)).To(Succeed())
			Eventually(func() types.UID {
				got, gerr := pdbGet("w", rev)
				if gerr != nil {
					return ""
				}
				return got.UID
			}, "15s", "100ms").Should(And(Not(BeEmpty()), Not(Equal(firstUID))),
				"the PDB was not recreated: SetupWithManager must Own PodDisruptionBudgets")
		})
	})
})

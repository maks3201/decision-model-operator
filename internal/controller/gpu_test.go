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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// GPU usability fixes (prefetch image, GPU taint toleration, PDB).
var _ = Describe("GPU usability", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)

	int32Ptr := func(v int32) *int32 { return &v }

	newReconciler := func(eng engine.Engine, prober Prober) *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
			Engines:   map[string]engine.Engine{"ollaya": eng},
			Prober:    prober,
			Recorder:  events.NewFakeRecorder(64),
		}
	}

	reconcileOnce := func(r *DecisionModelReconciler, name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}

	getDeployment := func(dmName, rev string) *appsv1.Deployment {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-" + rev}, dep)).To(Succeed())
		return dep
	}

	getJob := func(dmName, rev string) *batchv1.Job {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-prefetch-" + rev}, job)).To(Succeed())
		return job
	}

	markJobComplete := func(dmName, rev string) {
		job := getJob(dmName, rev)
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	// createGatedPod makes a serving Pod gated model-ready so the stable path can
	// settle a revision to Ready.
	createGatedPod := func(dmName, rev, podName string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      podName,
				Labels: map[string]string{
					decisionmodelv1alpha1.LabelName:     dmName,
					decisionmodelv1alpha1.LabelRevision: rev,
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ollaya", Image: fakeImage}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.PodIP = "10.0.0.30"
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue}, // what the kubelet sets once containers and gate are ready
			{Type: corev1.PodConditionType(decisionmodelv1alpha1.ModelReadyGate), Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}

	createDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine:   "ollaya",
				Model:    "laya:en",
				Device:   "cpu",
				Replicas: int32Ptr(1),
			},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
	}

	// cudaRev computes the revision hash for a cuda DM. The resolved serving
	// image is always the engine's CUDA default (spec.image override does not
	// flow into the serving image via paramsFor), so the revision hash uses
	// fakeImageCUDA regardless of spec.image.
	cudaRev := func(dm *decisionmodelv1alpha1.DecisionModel) string {
		return RevisionHash(dm.Spec, defaultDigest, fakeImageCUDA)
	}

	hasToleration := func(spec corev1.PodSpec, key string) bool {
		for _, t := range spec.Tolerations {
			if t.Key == key {
				return true
			}
		}
		return false
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = fmt.Sprintf("gpu-test-%d", nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// prefetch must not use the resolved serving (CUDA) image.
	Describe("prefetch image", func() {
		It("prefetches a cuda DM with the CPU default, not the CUDA serving image", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			createDM("p1", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Device = "cuda" })

			reconcileOnce(r, "p1")
			rev := cudaRev(getDM("p1"))

			job := getJob("p1", rev)
			jobImg := job.Spec.Template.Spec.Containers[0].Image
			Expect(jobImg).To(Equal(fakeImage), "prefetch uses the CPU default image")
			Expect(jobImg).NotTo(Equal(fakeImageCUDA), "prefetch must not use the CUDA serving image")

			// The serving Deployment is created once caching completes; it keeps
			// the resolved CUDA image.
			markJobComplete("p1", rev)
			reconcileOnce(r, "p1")
			dep := getDeployment("p1", rev)
			Expect(dep.Spec.Template.Spec.Containers[0].Image).To(Equal(fakeImageCUDA))
		})

		It("prefetches with spec.image when the user overrides it", func() {
			const override = "registry.example.com/ollaya:pinned"
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			createDM("p2", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Device = "cuda"
				dm.Spec.Image = override
			})
			// spec.image requires the operator to allow overrides.
			r.AllowImageOverride = true

			reconcileOnce(r, "p2")
			rev := cudaRev(getDM("p2"))
			Expect(getJob("p2", rev).Spec.Template.Spec.Containers[0].Image).To(Equal(override))
		})
	})

	// GPU taint toleration for serving Pods only.
	Describe("GPU taint toleration", func() {
		It("adds the nvidia.com/gpu toleration to serving Pods for cuda, not the Job", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			createDM("t1", func(dm *decisionmodelv1alpha1.DecisionModel) { dm.Spec.Device = "cuda" })

			reconcileOnce(r, "t1")
			rev := cudaRev(getDM("t1"))
			// Prefetch Job must NOT carry the GPU toleration (it uses the CPU image).
			Expect(hasToleration(getJob("t1", rev).Spec.Template.Spec, gpuTaintKey)).To(BeFalse())

			markJobComplete("t1", rev)
			reconcileOnce(r, "t1")
			Expect(hasToleration(getDeployment("t1", rev).Spec.Template.Spec, gpuTaintKey)).To(BeTrue())
		})

		It("does not add the GPU toleration for cpu", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			createDM("t2", nil) // cpu
			reconcileOnce(r, "t2")
			rev := RevisionHash(getDM("t2").Spec, defaultDigest, fakeImage)
			markJobComplete("t2", rev)
			reconcileOnce(r, "t2")
			Expect(hasToleration(getDeployment("t2", rev).Spec.Template.Spec, gpuTaintKey)).To(BeFalse())
		})

		It("keeps a user-provided toleration that already tolerates the GPU taint rather than adding another", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{})
			// Empty effect tolerates every effect, including NoSchedule; it differs
			// from the operator's default (which sets Effect: NoSchedule), so the
			// assertion below can tell them apart.
			userTol := corev1.Toleration{
				Key:      gpuTaintKey,
				Operator: corev1.TolerationOpExists,
			}
			createDM("t3", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Device = "cuda"
				dm.Spec.Scheduling = &decisionmodelv1alpha1.SchedulingSpec{
					Tolerations: []corev1.Toleration{userTol},
				}
			})
			reconcileOnce(r, "t3")
			rev := cudaRev(getDM("t3"))
			markJobComplete("t3", rev)
			reconcileOnce(r, "t3")

			tols := getDeployment("t3", rev).Spec.Template.Spec.Tolerations
			var gpuTols []corev1.Toleration
			for _, t := range tols {
				if t.Key == gpuTaintKey {
					gpuTols = append(gpuTols, t)
				}
			}
			Expect(gpuTols).To(HaveLen(1), "exactly one toleration for the GPU key")
			Expect(gpuTols[0]).To(Equal(userTol), "the user's toleration is kept, not overridden")
		})
	})

	// PodDisruptionBudget per stable revision for replicas > 1.
	Describe("PodDisruptionBudget", func() {
		getPDB := func(dmName, rev string) (*policyv1.PodDisruptionBudget, error) {
			pdb := &policyv1.PodDisruptionBudget{}
			err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: dmName + "-" + rev}, pdb)
			return pdb, err
		}

		// driveToStable brings a replicas=N DM to a stable revision with gated
		// Pods, returning the revision hash. Shared RWX cache so replicas>1 is not
		// Degraded for cache-sharing.
		driveToStable := func(r *DecisionModelReconciler, name string, replicas int32) string {
			reconcileOnce(r, name)
			rev := RevisionHash(getDM(name).Spec, defaultDigest, fakeImage)
			markJobComplete(name, rev)
			reconcileOnce(r, name)
			for i := int32(0); i < replicas; i++ {
				createGatedPod(name, rev, fmt.Sprintf("%s-pod-%d", name, i))
			}
			reconcileOnce(r, name) // probe -> ready -> promote
			reconcileOnce(r, name) // stable path
			return rev
		}

		It("creates a PDB with maxUnavailable=1 for replicas>1 and none for replicas=1", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
			createDM("pdb1", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Replicas = int32Ptr(2)
				dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				}
			})
			rev := driveToStable(r, "pdb1", 2)

			pdb, err := getPDB("pdb1", rev)
			Expect(err).NotTo(HaveOccurred())
			Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(pdb.Spec.MinAvailable).To(BeNil())
			Expect(pdb.Spec.Selector.MatchLabels).To(Equal(map[string]string{
				decisionmodelv1alpha1.LabelName:     "pdb1",
				decisionmodelv1alpha1.LabelRevision: rev,
			}))

			// replicas=1 DM: no PDB.
			eng2 := newFakeEngine()
			r2 := newReconciler(eng2, &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
			createDM("pdb2", nil) // replicas 1
			rev2 := driveToStable(r2, "pdb2", 1)
			_, err = getPDB("pdb2", rev2)
			Expect(err).To(HaveOccurred(), "no PDB for replicas=1")
		})

		It("deletes the PDB when replicas drops to 1", func() {
			eng := newFakeEngine()
			r := newReconciler(eng, &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: defaultDigest, Device: "cpu"}})
			createDM("pdb3", func(dm *decisionmodelv1alpha1.DecisionModel) {
				dm.Spec.Replicas = int32Ptr(2)
				dm.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				}
			})
			rev := driveToStable(r, "pdb3", 2)
			_, err := getPDB("pdb3", rev)
			Expect(err).NotTo(HaveOccurred())

			// Scale to 1 replica (in-place update, same revision).
			dm := getDM("pdb3")
			dm.Spec.Replicas = int32Ptr(1)
			Expect(k8sClient.Update(ctx, dm)).To(Succeed())
			reconcileOnce(r, "pdb3")

			Eventually(func() bool {
				_, err := getPDB("pdb3", rev)
				return err != nil
			}, "2s", "50ms").Should(BeTrue(), "PDB removed once replicas<=1")
		})
	})
})

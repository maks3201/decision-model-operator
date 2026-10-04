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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
)

// §1 checksum, §2 drift, §3 cache size.
var _ = Describe("rotation / drift / cache", func() {
	var (
		ctx       context.Context
		namespace string
		nsCounter int
	)
	int32Ptr := func(v int32) *int32 { return &v }
	r := func() *DecisionModelReconciler {
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines: map[string]engine.Engine{"ollaya": newFakeEngine()}, Recorder: events.NewFakeRecorder(32),
		}
	}
	mkDM := func(name string, mutate func(*decisionmodelv1alpha1.DecisionModel)) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1)},
		}
		if mutate != nil {
			mutate(dm)
		}
		Expect(k8sClient.Create(ctx, dm)).To(Succeed())
		return dm
	}

	BeforeEach(func() {
		ctx = context.Background()
		nsCounter++
		namespace = "rotdrift-" + itoa(nsCounter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
	})

	// §1 — the API key checksum is recorded on the Deployment and only mirrored
	// into the Pod template (rolling the Pods) on a rotation. The first
	// observation of a legacy Deployment records without rolling.
	It("records the API key checksum and rolls the Pod template only on rotation", func() {
		rr := r()
		dm := mkDM("rot", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Create: records on the Deployment; no Pods exist yet, so the template
		// may carry it (first revision).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("key-v1"), false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		recorded1 := dep.Annotations[apiKeyChecksumAnnotation]
		Expect(recorded1).NotTo(BeEmpty())

		// Same key -> no template churn, recorded unchanged.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("key-v1"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).To(Equal(recorded1))

		// Rotated key -> template checksum changes (roll).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("key-v2"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).NotTo(Equal(recorded1))
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(dep.Annotations[apiKeyChecksumAnnotation]))
	})

	// §1 r1 — a stable Deployment created before this feature (no recorded
	// checksum, no template checksum) must NOT roll on the first observation:
	// the operator records the checksum but leaves the template untouched.
	It("adopts a legacy stable without rolling, then rolls on a later rotation", func() {
		rr := r()
		dm := mkDM("legacy", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Simulate a legacy Deployment: created without any checksum at all.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations).NotTo(HaveKey(apiKeyChecksumAnnotation))
		Expect(dep.Spec.Template.Annotations).NotTo(HaveKey(apiKeyChecksumAnnotation))
		tmplBefore := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]

		// First observation with a key: record only, template unchanged (no roll).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v1"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).NotTo(BeEmpty())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "no roll on first observation")

		// A steady-state reconcile with the same key still does not roll.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v1"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "still no roll in steady state")

		// A genuine rotation now rolls the template.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("key-v2"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).NotTo(BeEmpty())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(dep.Annotations[apiKeyChecksumAnnotation]))
	})

	// §2 — an injected sidecar container in the live Deployment is drift and is
	// reverted (deploymentMatchesDesired returns false, so ensureDeployment writes).
	It("reverts an injected sidecar container (drift)", func() {
		rr := r()
		dm := mkDM("drift", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "drift-r1"}, dep)).To(Succeed())
		ownContainers := len(dep.Spec.Template.Spec.Containers)

		// Inject a sidecar out of band.
		dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers,
			corev1.Container{Name: "sidecar", Image: "evil:latest"})
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())

		// Reconcile the Deployment: the extra container is reverted.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "drift-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Spec.Containers).To(HaveLen(ownContainers), "injected sidecar reverted")
		for _, c := range dep.Spec.Template.Spec.Containers {
			Expect(c.Name).NotTo(Equal("sidecar"))
		}
	})

	// — an injected Pod-template annotation (e.g. sidecar.istio.io/inject)
	// is drift and is reverted; a user rollout-restart annotation is tolerated.
	It("reverts an injected template annotation but tolerates kubectl restartedAt", func() {
		rr := r()
		dm := mkDM("annodrift", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("k1"), false)).To(Succeed())
		dep := &appsv1.Deployment{}
		depKey := types.NamespacedName{Namespace: namespace, Name: "annodrift-r1"}
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())

		// Inject a foreign annotation AND a user rollout-restart out of band.
		if dep.Spec.Template.Annotations == nil {
			dep.Spec.Template.Annotations = map[string]string{}
		}
		dep.Spec.Template.Annotations["sidecar.istio.io/inject"] = "true"
		dep.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = "2026-10-04T00:00:00Z"
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())

		// Reconcile: the injected annotation is reverted, restartedAt is preserved.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("k1"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations).NotTo(HaveKey("sidecar.istio.io/inject"), "injected annotation reverted")
		Expect(dep.Spec.Template.Annotations).To(HaveKeyWithValue("kubectl.kubernetes.io/restartedAt", "2026-10-04T00:00:00Z"),
			"user rollout-restart tolerated")
		Expect(dep.Spec.Template.Annotations).To(HaveKey(apiKeyChecksumAnnotation), "desired annotations kept")

		// A steady-state reconcile with only restartedAt present must be a no-op
		// (no loop): the template is not rewritten.
		rvBefore := dep.ResourceVersion
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("k1"), false)).To(Succeed())
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(dep.ResourceVersion).To(Equal(rvBefore), "restartedAt alone does not trigger an update loop")
	})

	// §3 — cache size shrink / storageClass change are CacheSpecImmutable;
	// a grow with a non-expandable class is CacheSpecImmutable.
	It("reports CacheSpecImmutable for a shrink and a storageClass change", func() {
		rr := r()
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "p"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}},
			},
		}

		shrink := mkDM("shrink", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{Size: resource.MustParse("5Gi")}
		})
		degraded, err := rr.guardCacheSharing(ctx, shrink, pvc)
		Expect(err).NotTo(HaveOccurred())
		Expect(degraded).To(BeTrue())
		deg := meta.FindStatusCondition(shrink.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonCacheSpecImmutable))

		sc := "other-class"
		scChange := mkDM("scchange", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{StorageClassName: &sc}
		})
		degraded, err = rr.guardCacheSharing(ctx, scChange, pvc)
		Expect(err).NotTo(HaveOccurred())
		Expect(degraded).To(BeTrue())
		deg = meta.FindStatusCondition(scChange.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg.Reason).To(Equal(reasonCacheSpecImmutable))
	})

	// §3 r1 — a grow is attempted as an expansion patch WITHOUT reading the
	// (cluster-scoped) StorageClass. The API server itself rejects an expansion
	// it does not allow (here: a PVC with no expandable StorageClass), and the
	// guard surfaces that rejection as CacheSpecImmutable rather than consulting
	// the StorageClass object.
	It("surfaces an API-rejected size grow as CacheSpecImmutable without reading the StorageClass", func() {
		rr := r()
		realPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "grow"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")}},
			},
		}
		Expect(k8sClient.Create(ctx, realPVC)).To(Succeed())

		grow := mkDM("grow", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Cache = &decisionmodelv1alpha1.CacheSpec{Size: resource.MustParse("10Gi")}
		})
		degraded, err := rr.guardCacheSharing(ctx, grow, realPVC)
		Expect(err).NotTo(HaveOccurred(), "an API policy rejection is CacheSpecImmutable, not a returned error")
		Expect(degraded).To(BeTrue())
		deg := meta.FindStatusCondition(grow.Status.Conditions, decisionmodelv1alpha1.ConditionDegraded)
		Expect(deg).NotTo(BeNil())
		Expect(deg.Reason).To(Equal(reasonCacheSpecImmutable))
		// PVC request unchanged (the rejected expansion was not applied).
		got := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "grow"}, got)).To(Succeed())
		q := got.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(q.Cmp(resource.MustParse("5Gi"))).To(Equal(0), "PVC request unchanged after a rejected grow")
	})
})

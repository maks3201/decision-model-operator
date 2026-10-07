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
	"crypto/sha256"
	"encoding/hex"
	"testing"

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

// checksum, drift, cache size.
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

	// the API key checksum is recorded on the Deployment and only mirrored
	// into the Pod template (rolling the Pods) on a rotation. The first
	// observation of a legacy Deployment records without rolling.
	It("records the API key checksum and rolls the Pod template only on rotation", func() {
		rr := r()
		dm := mkDM("rot", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Create: records on the Deployment; no Pods exist yet, so the template
		// may carry it (first revision).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "key-v1"), "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		recorded1 := dep.Annotations[apiKeyChecksumAnnotation]
		Expect(recorded1).NotTo(BeEmpty())

		// Same key -> no template churn, recorded unchanged.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "key-v1"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).To(Equal(recorded1))

		// Rotated key -> template checksum changes (roll).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "key-v2"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "rot-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).NotTo(Equal(recorded1))
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(dep.Annotations[apiKeyChecksumAnnotation]))
	})

	// a stable Deployment created before this feature (no recorded
	// checksum, no template checksum) must NOT roll on the first observation:
	// the operator records the checksum but leaves the template untouched.
	It("adopts a legacy stable without rolling, then rolls on a later rotation", func() {
		rr := r()
		dm := mkDM("legacy", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Simulate a legacy Deployment: created without any checksum at all.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, "", "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations).NotTo(HaveKey(apiKeyChecksumAnnotation))
		Expect(dep.Spec.Template.Annotations).NotTo(HaveKey(apiKeyChecksumAnnotation))
		tmplBefore := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]

		// First observation with a key: record only, template unchanged (no roll).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("sec-uid", "key-v1"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).NotTo(BeEmpty())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "no roll on first observation")

		// A steady-state reconcile with the same key still does not roll.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("sec-uid", "key-v1"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "still no roll in steady state")

		// A genuine rotation now rolls the template.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", true, apiKeyChecksum("sec-uid", "key-v2"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "legacy-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).NotTo(BeEmpty())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(dep.Annotations[apiKeyChecksumAnnotation]))
	})

	// Migration to the HMAC checksum must not roll an existing Deployment that
	// recorded the old sha256 form of the SAME key: the recorded annotation is
	// migrated to the HMAC value (metadata only); the Pod template is left
	// byte-identical. A later value rotation then rolls with the HMAC in the
	// template. A legacy recorded value that does NOT match the current key (the
	// key rotated while the operator was down) is treated as a rotation (roll).
	It("migrates the recorded checksum to HMAC without rolling, then rolls on rotation", func() {
		rr := r()
		dm := mkDM("mig", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		const key = "key-v1"
		legacy := legacyAPIKeyChecksum(key)
		hmacV := apiKeyChecksum("sec-uid", key)
		Expect(hmacV).NotTo(Equal(legacy))

		// Seed a pre-HMAC Deployment: both the recorded and the template annotation
		// hold the legacy sha256 form (as the old operator wrote them).
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, legacy, "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "mig-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).To(Equal(legacy))
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(legacy))
		tmplBefore := dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]
		specHashBefore := dep.Annotations[specHashAnnotation]

		// Upgrade: ensureDeployment now gets the HMAC as current and the legacy as
		// the legacy form. Because recorded == legacy, migrate the recorded
		// annotation to HMAC and keep the template unchanged -> no roll.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, hmacV, legacy, false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "mig-r1"}, dep)).To(Succeed())
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).To(Equal(hmacV), "recorded migrated to HMAC")
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "template unchanged -> no roll")
		Expect(dep.Annotations[specHashAnnotation]).To(Equal(specHashBefore), "spec hash unchanged -> no roll")

		// Idempotent: a second upgrade reconcile with the same key does nothing.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, hmacV, legacy, false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "mig-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(tmplBefore), "still no roll")

		// A real rotation now rolls the template to the new HMAC.
		hmac2 := apiKeyChecksum("sec-uid", "key-v2")
		legacy2 := legacyAPIKeyChecksum("key-v2")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, hmac2, legacy2, false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "mig-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(hmac2), "rotation rolls the template")
		Expect(dep.Annotations[apiKeyChecksumAnnotation]).To(Equal(hmac2))
	})

	// A legacy recorded value that does NOT match the current key's old checksum
	// means the key rotated while the operator was down: roll (not migrate).
	It("rolls when the legacy recorded value does not match the current key", func() {
		rr := r()
		dm := mkDM("migrot", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")

		// Seed with the legacy form of an OLD key.
		oldLegacy := legacyAPIKeyChecksum("old-key")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, oldLegacy, "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "migrot-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(oldLegacy))

		// Current key is different: HMAC(new) current, legacy(new) legacy. recorded
		// (oldLegacy) != legacy(new) and != HMAC(new) -> rotation -> roll.
		hmacNew := apiKeyChecksum("sec-uid", "new-key")
		legacyNew := legacyAPIKeyChecksum("new-key")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, hmacNew, legacyNew, false)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "migrot-r1"}, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations[apiKeyChecksumAnnotation]).To(Equal(hmacNew), "a down-time rotation rolls")
	})
	It("reverts an injected sidecar container (drift)", func() {
		rr := r()
		dm := mkDM("drift", nil)
		params := rr.paramsFor(dm, defaultDigest, fakeImage, "r1")
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, "", "", false)).To(Succeed())
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "drift-r1"}, dep)).To(Succeed())
		ownContainers := len(dep.Spec.Template.Spec.Containers)

		// Inject a sidecar out of band.
		dep.Spec.Template.Spec.Containers = append(dep.Spec.Template.Spec.Containers,
			corev1.Container{Name: "sidecar", Image: "evil:latest"})
		Expect(k8sClient.Update(ctx, dep)).To(Succeed())

		// Reconcile the Deployment: the extra container is reverted.
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, "", "", false)).To(Succeed())
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
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "k1"), "", false)).To(Succeed())
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
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "k1"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(dep.Spec.Template.Annotations).NotTo(HaveKey("sidecar.istio.io/inject"), "injected annotation reverted")
		Expect(dep.Spec.Template.Annotations).To(HaveKeyWithValue("kubectl.kubernetes.io/restartedAt", "2026-10-04T00:00:00Z"),
			"user rollout-restart tolerated")
		Expect(dep.Spec.Template.Annotations).To(HaveKey(apiKeyChecksumAnnotation), "desired annotations kept")

		// A steady-state reconcile with only restartedAt present must be a no-op
		// (no loop): the template is not rewritten.
		rvBefore := dep.ResourceVersion
		Expect(rr.ensureDeployment(ctx, dm, rr.Engines["ollaya"], params, "r1", false, apiKeyChecksum("sec-uid", "k1"), "", false)).To(Succeed())
		Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
		Expect(dep.ResourceVersion).To(Equal(rvBefore), "restartedAt alone does not trigger an update loop")
	})

	// cache size shrink / storageClass change are CacheSpecImmutable;
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

	// a grow is attempted as an expansion patch WITHOUT reading the
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

	// apiKeyTrigger derives the Pod-template rotation trigger from the auth
	// Secret's UID + value, so a label-only edit of the Secret (which does not
	// change the UID or the value) does not change the trigger and does not roll
	// the serving Pods.
	It("apiKeyTrigger is stable across a label-only edit of the auth Secret", func() {
		rr := r()
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "ak",
				Labels: map[string]string{decisionmodelv1alpha1.LabelAPIKey: "true"},
			},
			Data: map[string][]byte{"token": []byte("s3cret")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		dm := mkDM("trig", func(d *decisionmodelv1alpha1.DecisionModel) {
			d.Spec.Auth = &decisionmodelv1alpha1.AuthSpec{APIKeySecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "ak"}, Key: "token",
			}}
		})

		apiKey, err := rr.apiKey(ctx, dm)
		Expect(err).NotTo(HaveOccurred())
		before, legacyBefore := rr.apiKeyTrigger(ctx, dm, apiKey)
		Expect(before).NotTo(BeEmpty())
		Expect(legacyBefore).NotTo(BeEmpty())
		Expect(before).NotTo(Equal(legacyBefore), "HMAC trigger differs from the legacy sha256 form")

		// A label-only edit of the Secret: the trigger must not change.
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ak"}, secret)).To(Succeed())
		secret.Labels["extra"] = "x"
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		after, _ := rr.apiKeyTrigger(ctx, dm, apiKey)
		Expect(after).To(Equal(before), "label edit does not roll the Pods")

		// A value change DOES change the trigger (drives the in-place roll).
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ak"}, secret)).To(Succeed())
		secret.Data["token"] = []byte("rotated")
		Expect(k8sClient.Update(ctx, secret)).To(Succeed())
		rotated, _ := rr.apiKeyTrigger(ctx, dm, "rotated")
		Expect(rotated).NotTo(Equal(before), "a key rotation changes the trigger")
	})
})

// apiKeyChecksum must not be an offline-verifiable hash of the key value: a
// reader of the serving Deployment (who cannot read the auth Secret) must not be
// able to test guesses. It is HMAC-SHA256 keyed by the Secret UID, so it depends
// on both the UID and the value, is stable, and is never sha256(key) in any
// prefix form.
func TestApiKeyChecksumNotOfflineVerifiable(t *testing.T) {
	const uid = "11111111-2222-3333-4444-555555555555"
	const key = "s3cret"

	c := apiKeyChecksum(uid, key)
	if c == "" {
		t.Fatal("checksum must be non-empty for a non-empty key")
	}
	if len(c) != 16 {
		t.Errorf("checksum length = %d, want 16", len(c))
	}
	// Stable for the same inputs (survives restarts/leader changes).
	if apiKeyChecksum(uid, key) != c {
		t.Error("checksum is not stable for the same uid+key")
	}
	// Changes when the key value changes (drives the in-place roll).
	if apiKeyChecksum(uid, "other") == c {
		t.Error("checksum must change when the key value changes")
	}
	// Changes when the Secret UID changes (delete+recreate = rotation).
	if apiKeyChecksum("99999999-2222-3333-4444-555555555555", key) == c {
		t.Error("checksum must change when the Secret UID changes")
	}
	// Must NOT be sha256(key) in any prefix form: a Deployment reader cannot
	// verify a guess offline.
	plain := func(k string) string {
		sum := sha256.Sum256([]byte(k))
		return hex.EncodeToString(sum[:])
	}
	if c == plain(key)[:16] {
		t.Error("checksum must not be a prefix of sha256(key)")
	}
	// Empty key -> empty checksum (no auth, no annotation).
	if apiKeyChecksum(uid, "") != "" {
		t.Error("empty key must yield an empty checksum")
	}
}

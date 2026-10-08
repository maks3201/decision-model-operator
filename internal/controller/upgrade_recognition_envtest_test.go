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
	"fmt"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	decisionmodelv1alpha1 "github.com/maks3201/decision-model-operator/api/v1alpha1"
	"github.com/maks3201/decision-model-operator/internal/engine"
	"github.com/maks3201/decision-model-operator/internal/engine/ollaya"
)

// An operator upgrade must never roll a quiet DecisionModel: a stable recorded by
// an older release (tag-only serving image) must be recognised as the SAME
// revision by a build that renders the same tag digest-pinned
// (repo:tag@sha256:<index>). The chart-upgrade E2E caught this when 0.3.0's quiet
// stable rolled on upgrade to the digest-pinning build. These specs reproduce the
// recorded shapes of 0.3.0 and 0.4.0 and drive the current reconciler with the
// real ollaya engine (so the computed image is the real digest-pinned default),
// asserting no candidate is started and the stable Deployment template keeps the
// recorded tag-only image.
var _ = Describe("upgrade does not roll a quiet DecisionModel", func() {
	var (
		ctx       context.Context
		namespace string
		counter   int
		srv       *httptest.Server
		regHost   string
	)
	// The model manifest the registry serves; Resolve hashes the raw bytes. The
	// stable records this digest, so resolveDigest reuses it (no re-resolve) and
	// the candidate digest equals the stable's.
	manifestBody := []byte(`{"schemaVersion":2}`)
	modelDigest := func() string {
		sum := sha256.Sum256(manifestBody)
		return hex.EncodeToString(sum[:])
	}()
	// 0.10.0 is the runtime the pre-upgrade releases shipped; the tag-only serving
	// image a 0.3.0/0.4.0 stable recorded. This build renders the same tag with
	// its index digest appended.
	const legacyVersion = "0.10.0"
	tagImage := "ghcr.io/ollaya-dev/ollaya:" + legacyVersion

	int32Ptr := func(v int32) *int32 { return &v }
	getDM := func(name string) *decisionmodelv1alpha1.DecisionModel {
		dm := &decisionmodelv1alpha1.DecisionModel{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, dm)).To(Succeed())
		return dm
	}
	rec := func(r *DecisionModelReconciler, name string) {
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	}
	newReconciler := func() *DecisionModelReconciler {
		eng := ollaya.New(ollaya.WithRegistryURL(srv.URL))
		return &DecisionModelReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Engines:                 map[string]engine.Engine{"ollaya": eng},
			Prober:                  &fakeProber{loaded: engine.Loaded{Name: "laya:en", Digest: modelDigest, Device: "cpu"}},
			AllowedRegistries:       []string{regHost},
			AllowInsecureRegistries: true,
			Recorder:                events.NewFakeRecorder(128),
		}
	}
	// createLegacyStable creates a DM whose status already carries a stable
	// recorded as the given release did, plus the serving Deployment that stable
	// is running (tag-only image), so the reconcile has a live template to
	// compare against. shape mutates the recorded RevisionStatus per release.
	createLegacyStable := func(name, legacyHash string, shape func(*decisionmodelv1alpha1.RevisionStatus)) {
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1),
			},
		})).To(Succeed())
		stable := &decisionmodelv1alpha1.RevisionStatus{
			Hash: legacyHash, Engine: "ollaya", Model: "laya:en", Digest: modelDigest,
			Device: "cpu", Image: tagImage,
		}
		shape(stable)
		Expect(updateDMStatus(ctx, namespace, name, func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision = stable.DeepCopy()
			dm.Status.Phase = decisionmodelv1alpha1.PhaseReady
		})).To(Succeed())
		// The live serving Deployment the stable is running: tag-only image, the
		// revision label it was created under.
		dep := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: revisionName(getDM(name), legacyHash),
				Labels: revisionLabels(getDM(name), legacyHash),
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: int32Ptr(1),
				Selector: &metav1.LabelSelector{MatchLabels: revisionLabels(getDM(name), legacyHash)},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: revisionLabels(getDM(name), legacyHash)},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ollaya", Image: tagImage}},
					},
				},
			},
		}
		Expect(controllerutil.SetControllerReference(getDM(name), dep, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
	}
	servingImageOf := func(name, rev string) string {
		dep := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: revisionName(getDM(name), rev)}, dep)).To(Succeed())
		return dep.Spec.Template.Spec.Containers[0].Image
	}

	BeforeEach(func() {
		ctx = context.Background()
		counter++
		namespace = fmt.Sprintf("upgrade-recog-%d", counter)
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(manifestBody)
		}))
		u, perr := neturl.Parse(srv.URL)
		Expect(perr).NotTo(HaveOccurred())
		regHost = strings.ToLower(u.Host)
	})
	AfterEach(func() {
		if srv != nil {
			srv.Close()
		}
	})

	It("recognises a 0.3.0-recorded stable (tag-only image, no placement/version) without rolling", func() {
		r := newReconciler()
		// 0.3.0: 10-hex hash computed with the tag-only image, no RuntimeVersion,
		// no Placement recorded (the field did not exist).
		spec := getLegacySpec("laya:en", "cpu")
		legacyHash := legacyRevisionHash(spec, modelDigest, tagImage)
		createLegacyStable("v030", legacyHash, func(s *decisionmodelv1alpha1.RevisionStatus) {
			s.RuntimeVersion = ""
			s.Placement = "" // pre-placement: empty, adopted in place
		})

		rec(r, "v030")

		dm := getDM("v030")
		Expect(dm.Status.CandidateRevision).To(BeNil(),
			"an upgrade must not start a candidate for a quiet 0.3.0 stable")
		Expect(dm.Status.StableRevision.Hash).To(Equal(legacyHash), "the stable keeps its 10-hex hash name")
		Expect(dm.Status.StableRevision.Image).To(Equal(tagImage),
			"the stable keeps its recorded tag-only image verbatim (no roll)")
		Expect(servingImageOf("v030", legacyHash)).To(Equal(tagImage),
			"the serving Deployment template is unchanged (tag-only image)")
	})

	It("recognises a 0.4.0-recorded stable (tag-only image, runtimeVersion + placementNone) without rolling", func() {
		r := newReconciler()
		spec := getLegacySpec("laya:en", "cpu")
		legacyHash := legacyRevisionHash(spec, modelDigest, tagImage)
		createLegacyStable("v040", legacyHash, func(s *decisionmodelv1alpha1.RevisionStatus) {
			s.RuntimeVersion = legacyVersion // 0.4.0 recorded the version
			s.Placement = placementNone      // 0.4.0 recorded placement
		})

		rec(r, "v040")

		dm := getDM("v040")
		Expect(dm.Status.CandidateRevision).To(BeNil(),
			"an upgrade must not start a candidate for a quiet 0.4.0 stable")
		Expect(dm.Status.StableRevision.Image).To(Equal(tagImage),
			"the stable keeps its recorded tag-only image verbatim (no roll)")
		Expect(servingImageOf("v040", legacyHash)).To(Equal(tagImage))
	})

	It("recognises a stable carrying a spec.image override without rolling", func() {
		// A stable created with an explicit spec.image: the recorded image is the
		// user's override (tag or digest form). An upgrade keeps it verbatim. Here
		// the override is a tag-only custom image; the current build renders the
		// same override (spec.image bypasses the engine default), so recognition
		// holds and nothing rolls.
		const override = "registry.example.com/ollaya/custom:1.2.3"
		r := newReconciler()
		spec := getLegacySpec("laya:en", "cpu")
		spec.Image = override
		legacyHash := legacyRevisionHash(spec, modelDigest, override)
		Expect(k8sClient.Create(ctx, &decisionmodelv1alpha1.DecisionModel{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "ovr"},
			Spec: decisionmodelv1alpha1.DecisionModelSpec{
				Engine: "ollaya", Model: "laya:en", Device: "cpu", Replicas: int32Ptr(1), Image: override,
			},
		})).To(Succeed())
		Expect(updateDMStatus(ctx, namespace, "ovr", func(dm *decisionmodelv1alpha1.DecisionModel) {
			dm.Status.StableRevision = &decisionmodelv1alpha1.RevisionStatus{
				Hash: legacyHash, Engine: "ollaya", Model: "laya:en", Digest: modelDigest,
				Device: "cpu", Image: override,
			}
			dm.Status.Phase = decisionmodelv1alpha1.PhaseReady
		})).To(Succeed())

		rec(r, "ovr")

		dm := getDM("ovr")
		Expect(dm.Status.CandidateRevision).To(BeNil(),
			"an upgrade must not start a candidate for a quiet stable with a spec.image override")
		Expect(dm.Status.StableRevision.Image).To(Equal(override), "the override image is kept verbatim")
	})
})

// getLegacySpec builds the DecisionModelSpec a legacy stable was created from.
func getLegacySpec(model, device string) decisionmodelv1alpha1.DecisionModelSpec {
	return decisionmodelv1alpha1.DecisionModelSpec{Engine: "ollaya", Model: model, Device: device}
}
